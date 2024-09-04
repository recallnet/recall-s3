#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]

use std::io::IsTerminal;

use basin_s3::Basin;
use clap::{CommandFactory, Parser, ValueEnum};
use clap_verbosity_flag::Verbosity;
use fendermint_crypto::SecretKey;
use hoku_provider::json_rpc::JsonRpcProvider;
use hoku_sdk::network::Network as SdkNetwork;
use hoku_signer::{key::parse_secret_key, AccountKind, Wallet};
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
    #[arg(long, env)]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long, env)]
    secret_key: Option<String>,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long, env)]
    domain_name: Option<String>,
}

fn setup_tracing(cli: &Cli) {
    use tracing_subscriber::EnvFilter;

    let log_level = match cli.verbose.log_level() {
        Some(level) => level.to_string(),
        None => "info".to_string(),
    };

    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::new(log_level))
        .with_ansi(enable_color)
        .init();
}

fn check_cli_args(cli: &Cli) {
    use clap::error::ErrorKind;

    let mut cmd = Cli::command();

    // TODO: how to specify the requirements with clap derive API?
    if let (Some(_), None) | (None, Some(_)) = (&cli.access_key, &cli.secret_key) {
        let msg = "access key and secret key must be specified together";
        cmd.error(ErrorKind::MissingRequiredArgument, msg).exit();
    }

    if let Some(ref s) = cli.domain_name {
        if s.contains('/') {
            let msg = format!("expected domain name, found URL-like string: {s:?}");
            cmd.error(ErrorKind::InvalidValue, msg).exit();
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    check_cli_args(&cli);
    setup_tracing(&cli);
    run(cli)
}

#[tokio::main]
async fn run(cli: Cli) -> anyhow::Result<()> {
    cli.network.get().init();

    let network = cli.network.get();
    // Setup network provider
    let provider =
        JsonRpcProvider::new_http(network.rpc_url()?, None, Some(network.object_api_url()?))?;

    let root = my_home()?.unwrap().join(".s3-basin");
    std::fs::create_dir_all(&root)?;

    let basin = match cli.private_key {
        Some(sk) => {
            // Setup local wallet using private key from arg
            let mut wallet =
                Wallet::new_secp256k1(sk, AccountKind::Ethereum, network.subnet_id()?)?;
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
}

impl Network {
    pub fn get(&self) -> SdkNetwork {
        match self {
            Network::Mainnet => SdkNetwork::Mainnet,
            Network::Testnet => SdkNetwork::Testnet,
            Network::Localnet => SdkNetwork::Localnet,
            Network::Devnet => SdkNetwork::Devnet,
        }
    }
}
