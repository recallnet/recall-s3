use std::collections::HashMap;
use std::ops::{Deref, Not};
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::utils::copy_bytes;
use crate::utils::hex;
use crate::Basin;

use adm_provider::message::GasParams;
use adm_sdk::machine;
use adm_sdk::machine::objectstore::AddOptions;
use adm_sdk::machine::objectstore::DeleteOptions;
use adm_sdk::machine::objectstore::GetOptions;
use adm_sdk::machine::objectstore::ObjectStore;
use adm_sdk::machine::objectstore::QueryOptions;
use adm_sdk::machine::Machine;
use adm_signer::Signer;
use async_tempfile::TempFile;
use bytestring::ByteString;
use fendermint_actor_machine::WriteAccess;
use fendermint_vm_message::query::FvmQueryHeight;
use futures::StreamExt;
use futures::TryStreamExt;
use fvm_shared::address::Address;
use md5::Digest;
use md5::Md5;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use tendermint_rpc::Client;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncSeekExt, ReadBuf};
use tokio_util::io::ReaderStream;
use tracing::debug;
use tracing::log::error;
use uuid::Uuid;

static LAST_MODIFIED_METADATA_KEY: &str = "last_modified";
static CREATION_DATE_METADATA_KEY: &str = "creation_date";
static ETAG_METADATA_KEY: &str = "etag";

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
        Ok(S3Response::new(AbortMultipartUploadOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
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
            let mut hasher_reader = HasherReader::new(reader);
            let _ = try_!(tokio::io::copy(&mut hasher_reader, &mut file).await);
            e_tag_hash.update(hasher_reader.finalize());
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

        let address = try_!(Address::from_str(bucket.as_str()));
        let machine = ObjectStore::attach(address)
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
            bucket: Some(bucket),
            key: Some(key),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (src_bucket, src_key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                ..
            } => (bucket.to_string(), key.to_string()),
        };

        let (dst_bucket, dst_key) = (input.bucket, input.key);

        // Download object to a file
        let address = try_!(Address::from_str(&src_bucket));
        let machine = ObjectStore::attach(address)
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

        let address = try_!(Address::from_str(&dst_bucket));
        let machine = ObjectStore::attach(address)
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

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "CreateBucket is not implemented in read-only mode"
            ));
        }

        let creation_date = try_!(SystemTime::now().duration_since(UNIX_EPOCH)).as_secs();

        let mut wallet = match &self.wallet {
            Some(w) => w.clone(),
            None => unreachable!(),
        };
        let (machine, _) = ObjectStore::new(
            self.provider.deref(),
            &mut wallet,
            WriteAccess::OnlyOwner,
            HashMap::from([(
                CREATION_DATE_METADATA_KEY.to_string(),
                creation_date.to_string(),
            )]),
            GasParams::default(),
        )
        .await
        .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let address = machine.address().to_string();

        Ok(S3Response::new(CreateBucketOutput {
            location: Some(address),
        }))
    }

    // #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
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

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObject is not implemented in read-only mode"
            ));
        }

        let bucket = req.input.bucket;
        let key = req.input.key;

        let address = try_!(Address::from_str(&bucket));
        let machine = ObjectStore::attach(address)
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

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        if self.is_read_only {
            return Err(s3_error!(
                NotImplemented,
                "DeleteObjects is not implemented in read-only mode"
            ));
        }

        let address = try_!(Address::from_str(&req.input.bucket));
        let machine = ObjectStore::attach(address)
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

        let output = DeleteObjectsOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let object = self.get_object(&machine, &input.key).await?;
        let file_len = object.size as u64;

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
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let Ok(address) = Address::from_str(&input.bucket) else {
            return Ok(S3Response::new(HeadBucketOutput {
                ..Default::default()
            }));
        };
        let _ = machine::info(self.provider.deref(), address, FvmQueryHeight::Committed)
            .await
            .map_err(|_| s3_error!(NoSuchBucket))?;

        Ok(S3Response::new(HeadBucketOutput {
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address)
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

        let object = if let Some(object) = object_list.objects.into_iter().next() {
            object.1
        } else {
            return Err(s3_error!(NoSuchKey));
        };

        let content_length_i64 = try_!(i64::try_from(object.size as u64));

        // TODO: detect content type
        let content_type = mime::APPLICATION_OCTET_STREAM;
        let last_modified = object
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
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_buckets(
        &self,
        _: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
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
        let list = ObjectStore::list(self.provider.deref(), &wallet, FvmQueryHeight::Committed)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut buckets: Vec<Bucket> = Vec::new();

        for data in list {
            let creation_date = data
                .metadata
                .get(CREATION_DATE_METADATA_KEY)
                .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

            let bucket_name = data.address.to_string();

            let bucket = Bucket {
                creation_date,
                name: Some(bucket_name),
            };
            buckets.push(bucket);
        }

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            common_prefixes: v2.common_prefixes,
            max_keys: v2.max_keys,
            ..Default::default()
        }))
    }

    // #[tracing::instrument]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input: ListObjectsV2Input = req.input;

        let address = try_!(Address::from_str(&input.bucket));
        let machine = ObjectStore::attach(address)
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

        let response = machine
            .query(
                self.provider.deref(),
                QueryOptions {
                    prefix,
                    delimiter,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let mut objects: Vec<Object> = Vec::new();
        for (key, object) in response.objects {
            let key_str = try_!(String::from_utf8(key));

            let last_modified = object
                .metadata
                .get(LAST_MODIFIED_METADATA_KEY)
                .map(|v| Timestamp::parse(TimestampFormat::EpochSeconds, v.as_str()).unwrap());

            objects.push(Object {
                key: Some(key_str),
                last_modified,
                size: Some(try_!(i64::try_from(object.size))),
                ..Default::default()
            });
        }

        let mut common_prefixes: CommonPrefixList = Vec::new();
        for common_prefix in response.common_prefixes {
            let s = try_!(String::from_utf8(common_prefix));
            common_prefixes.push(CommonPrefix { prefix: Some(s) });
        }

        let key_count = try_!(i32::try_from(objects.len()));

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            delimiter: input.delimiter,
            common_prefixes: Some(common_prefixes),
            encoding_type: input.encoding_type,
            name: Some(input.bucket),
            prefix: input.prefix,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
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

        let Some(mut body) = body else {
            return Err(s3_error!(IncompleteBody));
        };

        let address = try_!(Address::from_str(bucket.as_str()));
        let machine = ObjectStore::attach(address)
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

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
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
        Ok(S3Response::new(output))
    }

    //#[tracing::instrument]
    async fn get_bucket_location(
        &self,
        _req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }
}

/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
fn fmt_content_range(start: u64, end_inclusive: u64, size: u64) -> String {
    format!("bytes {start}-{end_inclusive}/{size}")
}

struct HasherReader<R> {
    inner: R,
    hasher: Md5,
}

impl<R> HasherReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: <Md5 as Digest>::new(),
        }
    }

    fn finalize(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for HasherReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Perform the read operation
        let poll_result = Pin::new(&mut self.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(())) = &poll_result {
            // Update the hash with the data that was read
            self.hasher.update(buf.filled());
        }

        poll_result
    }
}
