#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use std::io::IsTerminal;
use std::net::SocketAddr;

use anyhow::Context;
use basin_s3::Basin;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::Verbosity;
use recall_provider::{
    fvm_shared::address,
    json_rpc::{JsonRpcProvider, Url},
};
use recall_sdk::network::Network as SdkNetwork;
use recall_signer::{
    key::{parse_secret_key, SecretKey},
    AccountKind, SubnetID, Wallet,
};
use homedir::my_home;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    #[command(flatten)]
    verbose: Verbosity,

    /// Host name to listen on.
    #[arg(long, env, default_value = "127.0.0.1")]
    host: String,

    /// Port number to listen on.
    #[arg(long, env, default_value = "8014")]
    port: u16,

    /// Network presets for subnet and RPC URLs.
    #[arg(short, long, env, value_enum, default_value_t = Network::Testnet)]
    network: Network,

    /// Wallet private key (ECDSA, secp256k1) for signing transactions.
    #[arg(short, long, env, value_parser = parse_secret_key)]
    private_key: Option<SecretKey>,

    /// Access key used for authentication.
    #[arg(long, env, requires("secret_key"))]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long, env, requires("access_key"))]
    secret_key: Option<String>,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long, env, value_parser = validate_domain)]
    domain_name: Option<String>,

    /// Subnet ID for custom network
    #[arg(long, env, required_if_eq("network", "custom"))]
    subnet_id: Option<SubnetID>,

    /// RPC URL for custom network
    #[arg(long, env, required_if_eq("network", "custom"))]
    rpc_url: Option<Url>,

    /// Object API URL for custom network
    #[arg(long, env, required_if_eq("network", "custom"))]
    object_api_url: Option<Url>,

    /// Prometheus metrics socket address, e.g. 127.0.0.1:9090
    #[arg(long, env)]
    metrics_listen_address: Option<SocketAddr>,
}

fn validate_domain(input: &str) -> Result<String, &'static str> {
    if input.contains('/') {
        Err("invalid domain")
    } else {
        Ok(input.to_owned())
    }
}

fn setup_tracing(cli: &Cli) {
    use tracing_subscriber::EnvFilter;

    let log_level = match cli.verbose.log_level() {
        Some(level) => level.to_string(),
        None => "info".to_string(),
    };

    let enable_color = std::io::stdout().is_terminal();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    setup_tracing(&cli);
    run(cli)
}

#[tokio::main]
async fn run(cli: Cli) -> anyhow::Result<()> {
    let network_def = NetworkDefinition::new(&cli)?;
    address::set_current_network(network_def.address_network);

    // Setup network provider
    let provider =
        JsonRpcProvider::new_http(network_def.rpc_url, None, Some(network_def.object_api_url))?;

    let root = my_home()?.unwrap().join(".s3-basin");
    std::fs::create_dir_all(&root)?;

    let basin = match cli.private_key {
        Some(sk) => {
            // Setup local wallet using private key from arg
            let mut wallet =
                Wallet::new_secp256k1(sk, AccountKind::Ethereum, network_def.subnet_id)?;
            wallet.init_sequence(&provider).await?;
            Basin::new(root, provider, Some(wallet))?
        }
        None => Basin::new(root, provider, None)?,
    };

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(basin);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (cli.access_key, cli.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = cli.domain_name {
            b.set_base_domain(domain_name);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    if let Some(metrics_addr) = cli.metrics_listen_address {
        let builder = prometheus_exporter::Builder::new(metrics_addr);
        let _ = builder.start().context("failed to start metrics server")?;
        info!(addr = %metrics_addr, "running metrics endpoint");
    }

    // Run server
    let listener = TcpListener::bind((cli.host.as_str(), cli.port)).await?;
    let local_addr = listener.local_addr()?;

    let hyper_service = service.into_shared();

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        let (socket, _) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let conn = http_server.serve_connection(TokioIo::new(socket), hyper_service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shutdown!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Network {
    /// Network presets for mainnet.
    Mainnet,
    /// Network presets for Calibration (default pre-mainnet).
    Testnet,
    /// Network presets for a local three-node network.
    Localnet,
    /// Network presets for local development.
    Devnet,
    /// Custom network definition
    Custom,
}

impl Network {
    pub fn get(&self) -> Option<SdkNetwork> {
        match self {
            Network::Mainnet => Some(SdkNetwork::Mainnet),
            Network::Testnet => Some(SdkNetwork::Testnet),
            Network::Localnet => Some(SdkNetwork::Localnet),
            Network::Devnet => Some(SdkNetwork::Devnet),
            Network::Custom => None,
        }
    }
}

struct NetworkDefinition {
    subnet_id: SubnetID,
    rpc_url: Url,
    object_api_url: Url,
    address_network: address::Network,
}

impl NetworkDefinition {
    fn new(cli: &Cli) -> Result<Self, anyhow::Error> {
        match cli.network.get() {
            Some(network) => {
                let cfg = network.get_config();
                return Ok(Self {
                    address_network: if network == SdkNetwork::Mainnet {
                        address::Network::Mainnet
                    } else {
                        address::Network::Testnet
                    },
                    rpc_url: cfg.rpc_url,
                    object_api_url: cfg.object_api_url,
                    subnet_id: cfg.subnet_id,
                });
            }
            None => Ok(Self {
                address_network: address::Network::Testnet,
                subnet_id: cli.subnet_id.clone().unwrap(),
                rpc_url: cli.rpc_url.clone().unwrap(),
                object_api_url: cli.object_api_url.clone().unwrap(),
            }),
        }
    }
}
