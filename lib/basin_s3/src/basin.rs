use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use crate::bucket::BucketNameWithOwner;
use bytestring::ByteString;
use fendermint_actor_bucket::Object;
use fendermint_vm_message::query::FvmQueryHeight;
use fvm_shared::address::Address;
use hoku_provider::json_rpc::JsonRpcProvider;
use hoku_sdk::machine::bucket::{Bucket, QueryOptions};
use hoku_sdk::machine::Machine;
use hoku_signer::{Signer, Void};
use s3s::dto::{ObjectKey, PartNumber};
use s3s::{s3_error, S3Error, S3ErrorCode};
use tendermint_rpc::Client;
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

    pub async fn get_object(&self, machine: &Bucket, key: &ObjectKey) -> Result<Object, S3Error> {
        let object_list = machine
            .query(
                self.provider.deref(),
                QueryOptions {
                    prefix: key.to_string(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object = if let Some(object) = object_list.objects.into_iter().next() {
            if let Some(object) = object.1 {
                object
            } else {
                return Err(s3_error!(NoSuchKey));
            }
        } else {
            return Err(s3_error!(NoSuchKey));
        };

        Ok(object)
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
