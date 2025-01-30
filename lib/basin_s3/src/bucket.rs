use bytestring::ByteString;
use recall_provider::{fvm_shared::address::Address, util::ethers_address_to_fil_address};
use s3s::dto::BucketName;
use s3s::{s3_error, S3Error, S3ErrorCode};
use std::str::FromStr;

pub struct BucketNameWithOwner {
    name: String,
    owner: Address,
}

impl BucketNameWithOwner {
    pub fn from(bucket_name: BucketName) -> Result<Self, S3Error> {
        let parts = bucket_name.split(".").collect::<Vec<&str>>();

        if parts.len() <= 1 {
            return Err(s3_error!(InvalidBucketName));
        }

        let addr = ethers::types::Address::from_str(parts[0])
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;
        let owner = ethers_address_to_fil_address(&addr)
            .map_err(|e| S3Error::new(S3ErrorCode::Custom(ByteString::from(e.to_string()))))?;

        let name = parts[1..].join(".");
        if !check_bucket_name(name.as_str()) {
            return Err(s3_error!(InvalidBucketName));
        }
        Ok(Self { name, owner })
    }

    pub fn owner(&self) -> Address {
        self.owner
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

pub fn check_bucket_name(name: &str) -> bool {
    if !(3_usize..=20).contains(&name.len()) {
        return false;
    }

    if !name
        .as_bytes()
        .iter()
        .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        return false;
    }

    let Some(true) = name
        .as_bytes()
        .first()
        .map(|&b| b.is_ascii_lowercase() || b.is_ascii_digit())
    else {
        return false;
    };

    let Some(true) = name
        .as_bytes()
        .last()
        .map(|&b| b.is_ascii_lowercase() || b.is_ascii_digit())
    else {
        return false;
    };

    if name.contains("..") {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use crate::bucket::BucketNameWithOwner;
    #[test]
    fn test_bucket_name_with_owner() {
        let bucket = BucketNameWithOwner::from(
            "0xe1209fb9aa2d08c8541297ec06ee6bbb63b10edc.foo.bar".to_string(),
        )
        .unwrap();
        assert_eq!("foo.bar", bucket.name());

        let res =
            BucketNameWithOwner::from("0xe1209fb9aa2d08c8541297ec06ee6bbb63b10edc".to_string());
        assert!(res.is_err());

        let res =
            BucketNameWithOwner::from("0xe1209fb9aa2d08c8541297ec06ee6bbb63b10edc.".to_string());
        assert!(res.is_err());
    }
}
