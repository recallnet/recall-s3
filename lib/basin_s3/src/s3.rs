use std::collections::HashMap;
use std::ops::{Deref, Not};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bucket::BucketNameWithOwner;
use crate::utils::hex;
use crate::utils::{copy_bytes, HashReader};
use crate::{bucket, Basin};

use async_tempfile::TempFile;
use bytestring::ByteString;
use ethers::utils::hex::ToHexExt;
use fendermint_actor_machine::WriteAccess;
use fendermint_vm_message::query::FvmQueryHeight;
use futures::StreamExt;
use futures::TryStreamExt;
use hoku_provider::message::GasParams;
use hoku_sdk::machine::bucket::AddOptions;
use hoku_sdk::machine::bucket::Bucket;
use hoku_sdk::machine::bucket::DeleteOptions;
use hoku_sdk::machine::bucket::GetOptions;
use hoku_sdk::machine::bucket::QueryOptions;
use hoku_sdk::machine::Machine;
use hoku_signer::Signer;
use ipc_api::evm::payload_to_evm_address;
use lazy_static::lazy_static;
use md5::Digest;
use md5::Md5;
use prometheus::{register_int_counter_vec, IntCounterVec};
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use tendermint_rpc::Client;
use tokio::fs;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tracing::debug;
use tracing::log::error;
use uuid::Uuid;

static LAST_MODIFIED_METADATA_KEY: &str = "last_modified";
static CREATION_DATE_METADATA_KEY: &str = "creation_date";
static ETAG_METADATA_KEY: &str = "etag";
pub static ALIAS_METADATA_KEY: &str = "alias";

static MAX_LIST_OBJECTS_KEYS: u64 = 1000;

lazy_static! {
    static ref COUNTER_S3_ACTIONS: IntCounterVec = register_int_counter_vec!(
        "basin_s3_call",
        "Number of S3 calls.",
        &["action", "status"]
    )
    .unwrap();
}

struct S3ActionCounter {
    action: &'static str,
    success: bool,
}

impl S3ActionCounter {
    fn new(action: &'static str) -> Self {
        Self {
            action,
            success: false,
        }
    }
}

impl Drop for S3ActionCounter {
    fn drop(&mut self) {
        let status = if self.success { "success" } else { "error" };
        COUNTER_S3_ACTIONS
            .with_label_values(&[self.action, status])
            .inc();
    }
}

#[async_trait::async_trait]
impl<C, S> S3 for Basin<C, S>
where
    C: Client + Send + Sync + 'static,
    S: Signer + 'static,
{
    // #[tracing::instrument]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let mut action_counter = S3ActionCounter::new("abort_multipart_upload");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "AbortMultipartUpload is not implemented in read-only mode"
            ));
        }

        let AbortMultipartUploadInput { upload_id, .. } = req.input;

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;
        let prefix = format!(".upload_id-{upload_id}");
        let mut iter = try_!(fs::read_dir(&self.root).await);
        while let Some(entry) = try_!(iter.next_entry().await) {
            let file_type = try_!(entry.file_type().await);
            if file_type.is_file().not() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };

            if name.starts_with(&prefix) {
                try_!(fs::remove_file(entry.path()).await);
            }
        }
        action_counter.success = true;
        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let mut action_counter = S3ActionCounter::new("complete_multipart_upload");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CompleteMultipartUpload is not implemented in read-only mode"
            ));
        }

        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        let bucket = BucketNameWithOwner::from(bucket)?;

        let Some(multipart_upload) = multipart_upload else {
            return Err(s3_error!(InvalidPart));
        };

        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let mut file = try_!(TempFile::new().await);

        let mut cnt: i32 = 0;
        let mut e_tag_hash = <Md5 as Digest>::new();
        for part in multipart_upload.parts.into_iter().flatten() {
            let part_number = part
                .part_number
                .ok_or_else(|| s3_error!(InvalidRequest, "missing part number"))?;
            cnt += 1;
            if part_number != cnt {
                return Err(s3_error!(InvalidRequest, "invalid part order"));
            }

            let part_path = self.get_upload_part_path(&upload_id, part_number);
            let reader = try_!(fs::File::open(&part_path).await);
            let mut hash_reader = HashReader::new(reader);
            let _ = try_!(tokio::io::copy(&mut hash_reader, &mut file).await);
            e_tag_hash.update(hash_reader.finalize());
            try_!(fs::remove_file(&part_path).await);
        }

        try_!(file.flush().await);
        try_!(file.rewind().await);

        let md5_sum = hex(e_tag_hash.finalize());
        let e_tag = format!("\"{md5_sum}-{cnt}\"");

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };
        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let last_modified = try_!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();
        let _ = machine
            .add_from_path(
                self.provider.deref(),
                &mut wallet,
                &key,
                file.file_path(),
                AddOptions {
                    metadata: HashMap::from([
                        (
                            LAST_MODIFIED_METADATA_KEY.to_string(),
                            last_modified.to_string(),
                        ),
                        (ETAG_METADATA_KEY.to_string(), e_tag.to_string()),
                    ]),
                    ..AddOptions::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let output = CompleteMultipartUploadOutput {
            e_tag: Some(e_tag),
            bucket: Some(bucket.name()),
            key: Some(key),
            ..Default::default()
        };
        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let mut action_counter = S3ActionCounter::new("copy_object");
        let input = req.input;
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                ..
            } => (
                BucketNameWithOwner::from(bucket.to_string())?,
                key.to_string(),
            ),
        };

        let (dst_bucket, dst_key) = (BucketNameWithOwner::from(input.bucket)?, input.key);

        // Download object to a file
        let Some(src_address) = self.get_bucket_address_by_alias(&src_bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let machine = Bucket::attach(src_address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let src_object = self.get_object(&machine, &src_key).await?;

        let mut file = try_!(TempFile::new().await);
        let (writer, mut reader) = tokio::io::duplex(4096);

        let provider = self.provider.clone();
        tokio::spawn(async move {
            let _ = machine
                .get(
                    provider.deref(),
                    src_key.as_str(),
                    writer,
                    GetOptions {
                        range: None,
                        height: FvmQueryHeight::Committed,
                        show_progress: false,
                    },
                )
                .await
                .map_err(|err| error!("failed to download object: {}", err));
        });

        try_!(tokio::io::copy(&mut reader, &mut file).await);

        // Upload file
        try_!(file.flush().await);
        try_!(file.rewind().await);

        let mut wallet = match &self.wallet {
            Some(w) => w.to_owned(),
            None => unreachable!(),
        };

        let Some(dst_address) = self.get_bucket_address_by_alias(&dst_bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let machine = Bucket::attach(dst_address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let last_modified = try_!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();

        let e_tag = src_object
            .metadata
            .get(ETAG_METADATA_KEY)
            .ok_or(S3Error::new(S3ErrorCode::Custom(ByteString::from(
                "no etag".to_string(),
            ))))?;

        let _ = machine
            .add_reader(
                self.provider.deref(),
                &mut wallet,
                &dst_key,
                file,
                AddOptions {
                    metadata: HashMap::from([
                        (
                            LAST_MODIFIED_METADATA_KEY.to_string(),
                            last_modified.to_string(),
                        ),
                        (ETAG_METADATA_KEY.to_string(), e_tag.to_string()),
                    ]),
                    ..AddOptions::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let copy_object_result = CopyObjectResult {
            last_modified: Timestamp::parse(
                TimestampFormat::EpochSeconds,
                last_modified.to_string().as_str(),
            )
            .ok(),
            ..Default::default()
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            ..Default::default()
        };

        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let mut action_counter = S3ActionCounter::new("create_bucket");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CreateBucket is not implemented in read-only mode"
            ));
        }

        let bucket = req.input.bucket;
        if !bucket::check_bucket_name(bucket.as_str()) {
            return Err(s3_error!(InvalidBucketName));
        }

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let eth_address = payload_to_evm_address(wallet.address().payload())
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let bucket = BucketNameWithOwner::from(format!(
            "{}.{}",
            eth_address.encode_hex_with_prefix(),
            bucket
        ))?;

        if self.get_bucket_address_by_alias(&bucket).await?.is_some() {
            return Err(s3_error!(BucketAlreadyExists));
        }

        let creation_date = try_!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();

        let (machine, _) = Bucket::new(
            self.provider.deref(),
            &mut wallet,
            None,
            WriteAccess::OnlyOwner,
            HashMap::from([
                (
                    CREATION_DATE_METADATA_KEY.to_string(),
                    creation_date.to_string(),
                ),
                (ALIAS_METADATA_KEY.to_string(), bucket.name()),
            ]),
            GasParams::default(),
        )
        .await
        .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let address = machine.address().to_string();

        action_counter.success = true;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(address),
        }))
    }

    // #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let mut action_counter = S3ActionCounter::new("create_multipart_upload");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CreateMultipartUpload is not implemented in read-only mode"
            ));
        }

        let input = req.input;
        let upload_id = Uuid::new_v4();

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id.to_string()),
            ..Default::default()
        };

        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let mut action_counter = S3ActionCounter::new("delete_object");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObject is not implemented in read-only mode"
            ));
        }

        let bucket = BucketNameWithOwner::from(req.input.bucket)?;
        let key = req.input.key;

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };
        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let tx = machine
            .delete(
                self.provider.deref(),
                &mut wallet,
                key.as_str(),
                DeleteOptions::default(),
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        debug!(hash = ?tx.hash, status = ?tx.status);

        action_counter.success = true;
        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let mut action_counter = S3ActionCounter::new("delete_objects");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObjects is not implemented in read-only mode"
            ));
        }

        let bucket = BucketNameWithOwner::from(req.input.bucket)?;
        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };
        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        for object in req.input.delete.objects {
            let tx = machine
                .delete(
                    self.provider.deref(),
                    &mut wallet,
                    object.key.as_str(),
                    DeleteOptions::default(),
                )
                .await
                .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

            debug!(hash = ?tx.hash, status = ?tx.status);
        }

        action_counter.success = true;
        let output = DeleteObjectsOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let mut action_counter = S3ActionCounter::new("get_object");
        let input = req.input;
        let bucket = BucketNameWithOwner::from(input.bucket)?;

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object = self.get_object(&machine, &input.key).await?;
        let file_len = object.size;

        let (content_length, content_range) = match input.range {
            None => (file_len, None),
            Some(range) => {
                let file_range = range.check(file_len)?;
                let content_length = file_range.end - file_range.start;
                let content_range =
                    fmt_content_range(file_range.start, file_range.end - 1, file_len);
                (content_length, Some(content_range))
            }
        };

        let content_length_i64 = try_!(i64::try_from(content_length));

        let range = match input.range {
            Some(Range::Int { first, last }) => Some(format!(
                "{}-{}",
                first,
                last.map_or(String::new(), |v| v.to_string())
            )),
            Some(Range::Suffix { length }) => Some(format!("-{length}")),
            _ => None,
        };

        let (writer, reader) = tokio::io::duplex(4096);
        let reader_stream = ReaderStream::new(reader);

        let provider = self.provider.clone();
        tokio::spawn(async move {
            let _ = machine
                .get(
                    provider.deref(),
                    input.key.as_str(),
                    writer,
                    GetOptions {
                        range,
                        height: FvmQueryHeight::Committed,
                        show_progress: false,
                    },
                )
                .await
                .map_err(|err| error!("failed to download object: {}", err));
        });

        let last_modified = object
            .metadata
            .get(LAST_MODIFIED_METADATA_KEY)
            .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

        let e_tag = object
            .metadata
            .get(ETAG_METADATA_KEY)
            .map(|v| v.to_string());

        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(reader_stream)),
            content_length: Some(content_length_i64),
            e_tag,
            content_range,
            last_modified,
            ..Default::default()
        };
        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let mut action_counter = S3ActionCounter::new("head_bucket");
        let input = req.input;
        let bucket = BucketNameWithOwner::from(input.bucket)?;

        let Some(_) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        action_counter.success = true;
        Ok(S3Response::new(HeadBucketOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let mut action_counter = S3ActionCounter::new("head_object");
        let input = req.input;
        let bucket = BucketNameWithOwner::from(input.bucket)?;

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object_list = machine
            .query(
                self.provider.deref(),
                QueryOptions {
                    prefix: input.key.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object_state = if let Some((_, object_state)) = object_list.objects.into_iter().next() {
            object_state
        } else {
            return Err(s3_error!(NoSuchKey));
        };

        let content_length_i64 = try_!(i64::try_from(object_state.size));

        // TODO: detect content type
        let content_type = mime::APPLICATION_OCTET_STREAM;
        let last_modified = object_state
            .metadata
            .get(LAST_MODIFIED_METADATA_KEY)
            .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

        let output = HeadObjectOutput {
            content_length: Some(content_length_i64),
            content_type: Some(content_type),
            last_modified,
            metadata: None,
            ..Default::default()
        };
        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let mut action_counter = S3ActionCounter::new("list_buckets");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "ListBuckets is not implemented in read-only mode"
            ));
        }

        let wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let list = Bucket::list(self.provider.deref(), &wallet, FvmQueryHeight::Committed)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut buckets: Vec<s3s::dto::Bucket> = Vec::new();

        for data in list {
            let creation_date = data
                .metadata
                .get(CREATION_DATE_METADATA_KEY)
                .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

            let name = data
                .metadata
                .get(ALIAS_METADATA_KEY)
                .cloned()
                .or(Some(data.address.to_string()));

            let bucket = s3s::dto::Bucket {
                name,
                creation_date,
            };
            buckets.push(bucket);
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let mut action_counter = S3ActionCounter::new("list_objects");
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        action_counter.success = true;
        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            common_prefixes: v2.common_prefixes,
            max_keys: v2.max_keys,
            is_truncated: v2.is_truncated,
            marker: v2.continuation_token,
            next_marker: v2.next_continuation_token,
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let mut action_counter = S3ActionCounter::new("list_objects_v2");
        let input: ListObjectsV2Input = req.input;
        let bucket = BucketNameWithOwner::from(input.bucket)?;

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let prefix = match &input.prefix {
            Some(prefix) => prefix.to_string(),
            None => String::new(),
        };

        let delimiter = match &input.delimiter {
            Some(delimiter) => delimiter.to_string(),
            None => String::new(),
        };

        let limit = input
            .max_keys
            .map_or(MAX_LIST_OBJECTS_KEYS, |v| {
                v.try_into().unwrap_or(MAX_LIST_OBJECTS_KEYS)
            })
            .min(MAX_LIST_OBJECTS_KEYS);
        let start_key = input
            .continuation_token
            .as_ref()
            .map(|v| v.as_bytes().to_vec());

        let response = machine
            .query(
                self.provider.deref(),
                QueryOptions {
                    prefix,
                    delimiter,
                    start_key,
                    limit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut objects: Vec<Object> = Vec::new();
        for (key, object_state) in response.objects {
            let key_str = String::from_utf8_lossy(&key);

            let last_modified = object_state
                .metadata
                .get(LAST_MODIFIED_METADATA_KEY)
                .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

            objects.push(Object {
                key: Some(key_str.to_string()),
                last_modified,
                size: Some(try_!(i64::try_from(object_state.size))),
                ..Default::default()
            });
        }

        let mut common_prefixes: CommonPrefixList = Vec::new();
        for common_prefix in response.common_prefixes {
            let s = try_!(String::from_utf8(common_prefix));
            common_prefixes.push(CommonPrefix { prefix: Some(s) });
        }

        let key_count = try_!(i32::try_from(objects.len()));
        let next_continuation_token = response
            .next_key
            .map(|key| String::from_utf8_lossy(&key).into());

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(
                i32::try_from(MAX_LIST_OBJECTS_KEYS)
                    .expect("MAX_LIST_OBJECTS_KEYS must fit into i32"),
            ),
            contents: Some(objects),
            delimiter: input.delimiter,
            common_prefixes: Some(common_prefixes),
            encoding_type: input.encoding_type,
            name: Some(bucket.name()),
            prefix: input.prefix,
            is_truncated: next_continuation_token.is_some().into(),
            continuation_token: input.continuation_token,
            next_continuation_token,
            ..Default::default()
        };

        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let mut action_counter = S3ActionCounter::new("put_object");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "PutObject is not implemented in read-only mode"
            ));
        }

        let input = req.input;

        let PutObjectInput {
            body, bucket, key, ..
        } = input;

        let bucket = BucketNameWithOwner::from(bucket)?;

        let Some(address) = self.get_bucket_address_by_alias(&bucket).await? else {
            return Err(s3_error!(NoSuchBucket));
        };

        let Some(mut body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let machine = Bucket::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut file = try_!(TempFile::new().await);

        let mut md5_hash = <Md5 as Digest>::new();
        while let Some(Ok(v)) = body.next().await {
            md5_hash.update(v.as_ref());
            try_!(file.write_all(&v).await);
        }
        try_!(file.flush().await);
        try_!(file.rewind().await);

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };

        let md5_sum = hex(md5_hash.finalize());
        let e_tag = format!("\"{md5_sum}\"");

        let last_modified = try_!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();
        let mut metadata = HashMap::from([
            (
                LAST_MODIFIED_METADATA_KEY.to_string(),
                last_modified.to_string(),
            ),
            (ETAG_METADATA_KEY.to_string(), e_tag.to_string()),
        ]);

        if input.metadata.is_some() {
            for (key, value) in input.metadata.unwrap() {
                metadata.insert(key, value);
            }
        };

        let _tx = machine
            .add_from_path(
                self.provider.deref(),
                &mut wallet,
                &key,
                file.file_path(),
                AddOptions {
                    metadata,
                    ..AddOptions::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let output = PutObjectOutput {
            e_tag: Some(e_tag),
            ..Default::default()
        };

        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let mut action_counter = S3ActionCounter::new("upload_part");
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "UploadPart is not implemented in read-only mode"
            ));
        }

        let UploadPartInput {
            body,
            upload_id,
            part_number,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let upload_id = Uuid::parse_str(&upload_id).map_err(|_| s3_error!(InvalidRequest))?;

        let file_path = self.get_upload_part_path(&upload_id, part_number);
        let mut md5_hash = <Md5 as Digest>::new();
        let stream = body.inspect_ok(|bytes| md5_hash.update(bytes.as_ref()));
        let mut file = try_!(fs::File::create(&file_path).await);
        let size = copy_bytes(stream, &mut file).await?;
        try_!(file.flush().await);

        let md5_sum = hex(md5_hash.finalize());
        debug!(path = ?file_path, ?size, %md5_sum, "write file");

        let output = UploadPartOutput {
            e_tag: Some(format!("\"{md5_sum}\"")),
            ..Default::default()
        };
        action_counter.success = true;
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_bucket_location(
        &self,
        _req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let mut action_counter = S3ActionCounter::new("get_bucket_location");
        let output = GetBucketLocationOutput::default();
        action_counter.success = true;
        Ok(S3Response::new(output))
    }
}

/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}
