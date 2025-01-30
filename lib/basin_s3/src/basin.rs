use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bucket::BucketNameWithOwner;
use bytestring::ByteString;
use recall_provider::{
    fvm_shared::address::Address, json_rpc::JsonRpcProvider, query::FvmQueryHeight, Client,
};
use recall_sdk::machine::bucket::{Bucket, ObjectState, QueryOptions};
use recall_sdk::machine::Machine;
use recall_signer::{Signer, Void};
use s3s::dto::{ObjectKey, PartNumber};
use s3s::{s3_error, S3Error, S3ErrorCode};
use uuid::Uuid;

pub struct Basin<C: Client + Send + Sync, S: Signer> {
    pub root: PathBuf,
    pub provider: Arc<JsonRpcProvider<C>>,
    pub wallet: Option<S>,
    pub is_read_only: bool,
}

impl<C, S> Basin<C, S>
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
}
