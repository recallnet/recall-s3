#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]

use adm_provider::json_rpc::JsonRpcProvider;
use adm_sdk::network::Network;

use adm_signer::key::parse_secret_key;
use adm_signer::AccountKind;
use adm_signer::Wallet;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_basin::Basin;
use s3s_basin::BasinWallet;
use tempfile::tempdir;
use tokio::sync::OnceCell;

use std::env;

use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tracing::{debug, error};

const DOMAIN_NAME: &str = "localhost:8014";

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "it_aws=debug,s3s_fs=debug,s3s=debug");
    }

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .init()
}

async fn get_wallet() -> Wallet {
    let sk = parse_secret_key(env::var("PRIVATE_KEY").unwrap().as_str()).unwrap();

    let network = Network::Testnet;
    network.init();
    let provider = JsonRpcProvider::new_http(
        network.rpc_url().unwrap(),
        None,
        Some(network.object_api_url().unwrap()),
    )
    .unwrap();

    let mut wallet =
        Wallet::new_secp256k1(sk, AccountKind::Ethereum, network.subnet_id().unwrap()).unwrap();
    wallet.init_sequence(&provider).await.unwrap();
    wallet
}

async fn config() -> &'static SdkConfig {
    static WALLET: OnceCell<Wallet> = OnceCell::const_new();
    let _ = WALLET.get_or_init(get_wallet).await;

    static CONFIG: Lazy<SdkConfig> = Lazy::new(|| {
        setup_tracing();

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        let network = Network::Testnet;
        network.init();
        // Setup network provider
        let provider = JsonRpcProvider::new_http(
            network.rpc_url().unwrap(),
            None,
            Some(network.object_api_url().unwrap()),
        )
        .unwrap();

        let basin = Basin::new(
            tempdir().unwrap().into_path(),
            provider,
            Some(BasinWallet::new(WALLET.get().unwrap().clone())),
        )
        .unwrap();

        // Setup S3 service
        let service = {
            let mut b = S3ServiceBuilder::new(basin);
            b.set_auth(SimpleAuth::from_single(
                cred.access_key_id(),
                cred.secret_access_key(),
            ));
            b.set_base_domain(DOMAIN_NAME);
            b.build()
        };

        // Convert to aws http client
        let client = s3s_aws::Client::from(service.into_shared());

        // Setup aws sdk config
        SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
            .region(Region::new("us-west-2"))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build()
    });
    &CONFIG
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    LOCK.lock().await
}

async fn create_bucket(c: &Client) -> Result<()> {
    c.create_bucket().bucket("bucket").send().await?;

    debug!("created bucket");
    Ok(())
}

async fn delete_object(c: &Client, bucket: &str, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

async fn delete_bucket(c: &Client, bucket: &str) -> Result<()> {
    c.delete_bucket().bucket(bucket).send().await?;
    Ok(())
}

macro_rules! log_and_unwrap {
    ($result:expr) => {
        match $result {
            Ok(ans) => {
                debug!(?ans);
                ans
            }
            Err(err) => {
                error!(?err);
                return Err(err.into());
            }
        }
    };
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_buckets() -> Result<()> {
    let c = Client::new(config().await);
    let response1 = log_and_unwrap!(c.list_buckets().send().await);
    drop(response1);

    //create_bucket(&c).await?;

    let response2 = log_and_unwrap!(c.list_buckets().send().await);
    let bucket_count = response2.buckets().len();

    assert_eq!(bucket_count, 6);

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_objects_v2() -> Result<()> {
    let c = Client::new(config().await);
    let bucket = "t2kk6h4zq3ecnjcufbfzif7htuodbti4kq3p5wtxa".to_string();
    let bucket_str = bucket.as_str();
    // create_bucket(&c).await?;

    let test_prefix = "this/is/a/test/path/";
    let key1 = "this/is/a/test/path/file1.txt";
    let key2 = "this/is/a/test/path/file2.txt";
    {
        let content = "hello world\nनमस्ते दुनिया\n";

        c.put_object()
            .bucket(bucket_str)
            .key(key1)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await?;
        c.put_object()
            .bucket(bucket_str)
            .key(key2)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await?;
    }

    let result = c
        .list_objects_v2()
        .bucket(bucket_str)
        .prefix(test_prefix)
        .send()
        .await;

    let response = log_and_unwrap!(result);

    let contents: Vec<_> = response
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .collect();
    assert!(!contents.is_empty());
    assert!(contents.contains(&key1));
    assert!(contents.contains(&key2));

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_single_object() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(config().await);
    let bucket = "t2kk6h4zq3ecnjcufbfzif7htuodbti4kq3p5wtxa".to_string();
    let bucket = bucket.as_str();
    let key = "sample-test.txt";
    let content = "hello world\n你好世界\n";
    // let crc32c =
    //     base64_simd::STANDARD.encode_to_string(crc32c::crc32c(content.as_bytes()).to_be_bytes());

    //create_bucket(&c).await?;

    {
        let body = ByteStream::from_static(content.as_bytes());
        c.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            //.checksum_crc32_c(crc32c.as_str())
            .send()
            .await?;
    }

    {
        let ans = c
            .get_object()
            .bucket(bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        //let checksum_crc32c = ans.checksum_crc32_c.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        //assert_eq!(checksum_crc32c, crc32c);
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, bucket, key).await?;
        //delete_bucket(&c, bucket).await?;
    }

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    let _guard = serial().await;

    let c = Client::new(config().await);

    let bucket = "t2kk6h4zq3ecnjcufbfzif7htuodbti4kq3p5wtxa".to_string();
    let bucket = bucket.as_str();
    //create_bucket(&c).await?;

    let key = "sample-test.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";

    let upload_id = {
        let ans = c
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        ans.upload_id.unwrap()
    };
    let upload_id = upload_id.as_str();

    let upload_parts = {
        let body = ByteStream::from_static(content.as_bytes());
        let part_number = 1;

        let ans = c
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .body(body)
            .part_number(part_number)
            .send()
            .await?;

        let part = CompletedPart::builder()
            .e_tag(ans.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build();

        vec![part]
    };

    {
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _ = c
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(upload)
            .upload_id(upload_id)
            .send()
            .await?;
    }

    {
        let ans = c.get_object().bucket(bucket).key(key).send().await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, bucket, key).await?;
        //delete_bucket(&c, bucket).await?;
    }

    Ok(())
}
