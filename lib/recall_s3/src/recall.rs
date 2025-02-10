use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bucket::{split_eth_address, BucketNameWithOwner};
use bytestring::ByteString;
use ethers::utils::hex::ToHexExt;
use recall_provider::{
    fvm_shared::address::Address, json_rpc::JsonRpcProvider, query::FvmQueryHeight, Client,
};
use recall_sdk::machine::bucket::{Bucket, ObjectState, QueryOptions};
use recall_sdk::machine::Machine;
use recall_signer::{Signer, Void};
use s3s::dto::{BucketName, ObjectKey, PartNumber};
use s3s::{s3_error, S3Error, S3ErrorCode};
use uuid::Uuid;

pub struct Recall<C: Client + Send + Sync, S: Signer> {
    pub root: PathBuf,
    pub provider: Arc<JsonRpcProvider<C>>,
    pub wallet: Option<S>,
    pub is_read_only: bool,
}

impl<C, S> Recall<C, S>
where
    C: Client + Send + Sync,
    S: Signer,
{
    pub fn new(
        root: PathBuf,
        provider: JsonRpcProvider<C>,
        wallet: Option<S>,
    ) -> anyhow::Result<Self> {
        let is_read_only = wallet.is_none();
        Ok(Self {
            root,
            wallet,
            is_read_only,
            provider: Arc::new(provider),
        })
    }

    pub fn get_upload_path(&self, upload_id: &Uuid) -> PathBuf {
        self.root.join(format!("upload-{upload_id}.json"))
    }

    pub fn get_upload_part_path(&self, upload_id: &Uuid, part_number: PartNumber) -> PathBuf {
        self.root
            .join(format!(".upload-{upload_id}.part-{part_number}.json"))
    }

    pub async fn get_object(
        &self,
        machine: &Bucket,
        key: &ObjectKey,
    ) -> Result<ObjectState, S3Error> {
        let object_list = machine
            .query(
                self.provider.deref(),
                QueryOptions {
                    prefix: key.to_string(),
                    start_key: Some(key.as_bytes().into()),
                    limit: 1,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        if let Some((_, object_state)) = object_list.objects.into_iter().next() {
            return Ok(object_state);
        }

        Err(s3_error!(NoSuchKey))
    }
    pub async fn get_bucket_address_by_alias(
        &self,
        bucket: &BucketNameWithOwner,
    ) -> Result<Option<Address>, S3Error> {
        let signer = &Void::new(bucket.owner());
        let list = Bucket::list(self.provider.deref(), signer, FvmQueryHeight::Committed)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let alias = bucket.name();
        for item in list {
            if let Some(v) = item.metadata.get(crate::s3::ALIAS_METADATA_KEY) {
                if v.eq(&alias) {
                    return Ok(Some(item.address));
                }
            }
        }

        Ok(None)
    }

    /// Given a bucket name figure out the full bucket path with the bucket owner address prefix according to the following rules:
    /// - If a wallet is provided at start-up, always try to use it as the owner of the bucket, unless the user provides the owner of the bucket in the bucket name.
    /// - If a wallet is not provided, only read calls are allowed, and the owner's address prefix must be part of the bucket name.
    pub fn get_bucket_path(&self, bucket: &BucketName) -> Result<BucketNameWithOwner, S3Error> {
        let eth_address = self
            .wallet
            .as_ref()
            .map(|wallet| wallet.eth_address().expect("wallet must has eth address"));
        match split_eth_address(bucket) {
            Some((addr, bucket_name)) => {
                BucketNameWithOwner::from(format!("{}.{}", addr, bucket_name))
            }
            None => {
                if let Some(eth_address) = eth_address {
                    return BucketNameWithOwner::from(format!(
                        "{}.{}",
                        eth_address.encode_hex_with_prefix(),
                        bucket
                    ));
                }

                Err(S3Error::new(S3ErrorCode::Custom(ByteString::from(
                    "owner address prefix is missing".to_string(),
                ))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Recall;
    use bytestring::ByteString;
    use recall_provider::fvm_shared::chainid::ChainID;
    use recall_provider::json_rpc::{JsonRpcProvider, Url};
    use recall_provider::util::ethers_address_to_fil_address;
    use recall_signer::key::parse_secret_key;
    use recall_signer::{AccountKind, SubnetID, Wallet};
    use s3s::S3ErrorCode;
    use std::str::FromStr;
    use tempfile::tempdir;

    #[test]
    fn test_get_bucket_path_with_wallet() {
        let tmp_dir = tempdir().unwrap();

        let provider = JsonRpcProvider::new_http(
            Url::from_str("http://127.0.0.1").unwrap(),
            ChainID::from(1),
            None,
            None,
        )
        .expect("json rpc provider should not fail");

        let sk =
            parse_secret_key("1c323d494d1d069fe4c891350a1ec691c4216c17418a0cb3c7533b143bd2b812")
                .expect("parse private key should not fail");
        let wallet = Wallet::new_secp256k1(
            sk,
            AccountKind::Ethereum,
            SubnetID::from_str("test").unwrap(),
        )
        .unwrap();

        let recall = Recall::new(tmp_dir.into_path(), provider, Some(wallet)).unwrap();

        let addr =
            ethers::types::Address::from_str("0xc05fe6b63ffa4b3c518e6ff1e597358ee839db01").unwrap();
        let owner = ethers_address_to_fil_address(&addr).unwrap();

        // without prefix, use wallet's address
        let result = recall.get_bucket_path(&"foo".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().owner(), owner);

        // wallet's address matches the prefix address
        let result =
            recall.get_bucket_path(&"0xc05fe6b63ffa4b3c518e6ff1e597358ee839db01.foo".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_bucket_path_no_wallet() {
        let tmp_dir = tempdir().unwrap();

        let provider = JsonRpcProvider::new_http(
            Url::from_str("http://127.0.0.1").unwrap(),
            ChainID::from(1),
            None,
            None,
        )
        .expect("json rpc provider should not fail");

        let wallet: Option<Wallet> = None;
        let recall = Recall::new(tmp_dir.into_path(), provider, wallet).unwrap();

        // without prefix, throws an error
        let result = recall.get_bucket_path(&"foo".to_string());
        assert_eq!(
            *result.unwrap_err().code(),
            S3ErrorCode::Custom(ByteString::from(
                "owner address prefix is missing".to_string(),
            ))
        );

        // with prefix

        let addr =
            ethers::types::Address::from_str("0xc05fe6b63ffa4b3c518e6ff1e597358ee839db01").unwrap();
        let owner = ethers_address_to_fil_address(&addr).unwrap();

        let result =
            recall.get_bucket_path(&"0xc05fe6b63ffa4b3c518e6ff1e597358ee839db01.foo".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().owner(), owner);
    }
}
