#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::must_use_candidate, //
)]
use anyhow::Result;
use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::error::BucketAlreadyExists;
use aws_sdk_s3::types::BucketLocationConstraint;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::types::CreateBucketConfiguration;
use aws_sdk_s3::Client;
use basin_s3::Basin;
use ethers::utils::hex::ToHexExt;
use hoku_provider::json_rpc::JsonRpcProvider;
use hoku_sdk::network::Network;
use hoku_signer::key::parse_secret_key;
use hoku_signer::AccountKind;
use hoku_signer::Signer;
use hoku_signer::Wallet;
use ipc_api::evm::payload_to_evm_address;
use once_cell::sync::Lazy;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use std::env;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Duration};
use tracing::{debug, error};

const DOMAIN_NAME: &str = "localhost:8014";

struct SdkConfigWithAddress {
    sdk: SdkConfig,
    address: String,
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .init()
}

async fn get_wallet() -> Wallet {
    let sk = parse_secret_key(env::var("PRIVATE_KEY").unwrap().as_str()).unwrap();

    let network = Network::Localnet;
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

async fn config() -> &'static SdkConfigWithAddress {
    static WALLET: OnceCell<Wallet> = OnceCell::const_new();
    let _ = WALLET.get_or_init(get_wallet).await;

    static CONFIG: Lazy<SdkConfigWithAddress> = Lazy::new(|| {
        setup_tracing();

        // Fake credentials
        let cred = Credentials::for_tests();

        // Setup S3 provider
        let network = Network::Localnet;
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
            Some(WALLET.get().unwrap().clone()),
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
        let config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .http_client(client)
            .region(Region::new("us-west-2"))
            .endpoint_url(format!("http://{DOMAIN_NAME}"))
            .build();

        let eth_address =
            payload_to_evm_address(WALLET.get().unwrap().clone().address().payload()).unwrap();

        SdkConfigWithAddress {
            sdk: config,
            address: eth_address.encode_hex_with_prefix(),
        }
    });
    &CONFIG
}

async fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    LOCK.lock().await
}

async fn create_bucket(c: &Client, bucket: &str) -> Result<()> {
    let location = BucketLocationConstraint::UsWest2;
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(location)
        .build();

    c.create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket)
        .send()
        .await?;

    Ok(())
}

async fn delete_object(c: &Client, bucket: &String, key: &str) -> Result<()> {
    c.delete_object().bucket(bucket).key(key).send().await?;
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
async fn test_bucket_exists() -> Result<()> {
    let config = config().await;
    let c = Client::new(&config.sdk);

    let bucket_str = "test-bucket-exists";

    create_bucket(&c, bucket_str).await?;

    assert!(create_bucket(&c, bucket_str)
        .await
        .err()
        .unwrap()
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<BucketAlreadyExists>()
        .is_some());

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_buckets() -> Result<()> {
    let config = config().await;
    let c = Client::new(&config.sdk);
    let response1 = log_and_unwrap!(c.list_buckets().send().await);
    drop(response1);

    let bucket1_str = "test-list-buckets-1";
    let bucket2_str = "test-list-buckets-2";

    create_bucket(&c, bucket1_str).await?;
    create_bucket(&c, bucket2_str).await?;

    let response2 = log_and_unwrap!(c.list_buckets().send().await);
    let bucket_names: Vec<_> = response2
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name())
        .collect();
    assert!(bucket_names.contains(&bucket1_str));
    assert!(bucket_names.contains(&bucket2_str));

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_list_objects_v2() -> Result<()> {
    let config = config().await;
    let c = Client::new(&config.sdk);

    let bucket = "test-list-objects";
    let bucket_with_owner = format!("{}.{}", &config.address, bucket);

    //create_bucket(&c, bucket).await?;

    let test_prefix = "this/is/a/test/path/";
    let key1 = "this/is/a/test/path/file1.txt";
    let key2 = "this/is/a/test/path/file2.txt";
    {
        let content = "hello world\nनमस्ते दुनिया\n";
        c.put_object()
            .bucket(&bucket_with_owner)
            .key(key1)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await?;
        c.put_object()
            .bucket(&bucket_with_owner)
            .key(key2)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await?;
    }

    let result = c
        .list_objects_v2()
        .bucket(&bucket_with_owner)
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
    let config = config().await;
    let c = Client::new(&config.sdk);

    let bucket = "test-single-object";
    let bucket_with_owner = format!("{}.{}", &config.address, bucket);

    let key = "sample.txt";
    let content = "hello world\n你好世界\n"; // md5 = 4a944a9af55168f2e2063907c421b061

    create_bucket(&c, bucket).await?;
    {
        let body = ByteStream::from_static(content.as_bytes());
        c.put_object()
            .bucket(&bucket_with_owner)
            .key(key)
            .body(body)
            .send()
            .await?;
    }
    // wait for object resolution
    sleep(Duration::from_millis(5000)).await;

    {
        let ans = c
            .get_object()
            .bucket(&bucket_with_owner)
            .key(key)
            .send()
            .await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        let e_tag = ans.e_tag.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(e_tag, "\"4a944a9af55168f2e2063907c421b061\"".to_string());
        assert_eq!(body.as_ref(), content.as_bytes());
    }
    {
        delete_object(&c, &bucket_with_owner, key).await?;
        //delete_bucket(&c, bucket).await?;
    }

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_multipart() -> Result<()> {
    let _guard = serial().await;

    let config = config().await;
    let c = Client::new(&config.sdk);

    let bucket = "test-multipart";
    let bucket_with_owner = format!("{}.{}", &config.address, bucket);

    create_bucket(&c, bucket).await?;

    let key = "sample-test.txt";
    let content = "abcdefghijklmnopqrstuvwxyz/0123456789/!@#$%^&*();\n";

    let upload_id = {
        let ans = c
            .create_multipart_upload()
            .bucket(&bucket_with_owner)
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
            .bucket(&bucket_with_owner)
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
            .bucket(&bucket_with_owner)
            .key(key)
            .multipart_upload(upload)
            .upload_id(upload_id)
            .send()
            .await?;
    }

    // wait for object resolution
    sleep(Duration::from_millis(5000)).await;

    {
        let ans = c
            .get_object()
            .bucket(&bucket_with_owner)
            .key(key)
            .send()
            .await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        let e_tag = ans.e_tag.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(e_tag, "\"af77e80818f1ff6fa731c8877e8b52ec-1\"".to_string());
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, &bucket_with_owner, key).await?;
        //delete_bucket(&c, bucket).await?;
    }

    Ok(())
}

#[tokio::test]
#[tracing::instrument]
async fn test_copy() -> Result<()> {
    let _guard = serial().await;

    let config = config().await;
    let c = Client::new(&config.sdk);

    let bucket = "test-copy";
    let bucket_with_owner = format!("{}.{}", &config.address, bucket);

    let key = "sample.txt";
    let content = "hello world\n你好世界\n"; // md5 = 4a944a9af55168f2e2063907c421b061

    create_bucket(&c, bucket).await?;

    {
        let body = ByteStream::from_static(content.as_bytes());
        c.put_object()
            .bucket(&bucket_with_owner)
            .key(key)
            .body(body)
            .send()
            .await?;
    }
    // wait for object resolution
    sleep(Duration::from_millis(5000)).await;

    {
        c.copy_object()
            .bucket(&bucket_with_owner)
            .key("sample-copy.txt")
            .copy_source(format!("{}/{}", &bucket_with_owner, &key))
            .send()
            .await?;
    }

    // wait for object resolution
    sleep(Duration::from_millis(5000)).await;

    {
        let ans = c
            .get_object()
            .bucket(&bucket_with_owner)
            .key("sample-copy.txt")
            .send()
            .await?;

        let content_length: usize = ans.content_length().unwrap().try_into().unwrap();
        let e_tag = ans.e_tag.unwrap();
        let body = ans.body.collect().await?.into_bytes();

        assert_eq!(content_length, content.len());
        assert_eq!(e_tag, "\"4a944a9af55168f2e2063907c421b061\"".to_string());
        assert_eq!(body.as_ref(), content.as_bytes());
    }

    {
        delete_object(&c, &bucket_with_owner, key).await?;
        delete_object(&c, &bucket_with_owner, "sample-copy.txt").await?;
        //delete_bucket(&c, bucket).await?;
    }

    Ok(())
}
