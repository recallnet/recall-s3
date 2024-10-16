use anyhow::{anyhow, Error};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct EncryptionKey {
    key: Vec<u8>,
    encrypted_key: Vec<u8>,
}

impl EncryptionKey {
    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
    }

    pub fn encrypted_key(&self) -> Vec<u8> {
        self.encrypted_key.clone()
    }

    pub fn encrypted_key_as_str(&self) -> String {
        STANDARD.encode(&self.encrypted_key)
    }
}

/// An async trait which represents the KMS API
#[async_trait::async_trait]
pub trait Kms {
    // async fn create_key();

    // TODO(bcalza): add context
    async fn fetch_encryption_key(&self, master_key: &String) -> Result<EncryptionKey, Error>;

    async fn decrypt_encryption_key(
        &self,
        master_key: &String,
        encrypted_key: &Vec<u8>,
    ) -> Result<EncryptionKey, Error>;
}

#[derive(Clone)]
pub struct Kes {
    endpoint: String,
    client: Client,
}

impl Kes {
    pub fn new(endpoint: String, key: Vec<u8>, cert: Vec<u8>) -> Result<Self, anyhow::Error> {
        let certificate = reqwest::Certificate::from_pem(&cert)?;

        let pem = [&key, "\n".as_bytes(), &cert].concat();
        let identity = reqwest::Identity::from_pem(&pem)?;

        // Build the client with TLS configuration
        let client = Client::builder()
            .use_rustls_tls()
            .add_root_certificate(certificate)
            .identity(identity)
            .https_only(true)
            .build()?;

        Ok(Kes { client, endpoint })
    }
}

#[async_trait::async_trait]
impl Kms for Kes {
    async fn fetch_encryption_key(
        &self,
        master_key: &String,
    ) -> Result<EncryptionKey, anyhow::Error> {
        #[derive(Debug, Deserialize)]
        struct Response {
            plaintext: String,
            ciphertext: String,
        }

        let response = self
            .client
            .post(format!("{}/v1/key/generate/{}", &self.endpoint, master_key))
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("failed"));
        }

        let data: Response = response.json().await?;
        let key = STANDARD.decode(data.plaintext).unwrap();
        let encrypted_key = STANDARD.decode(data.ciphertext).unwrap();
        return Ok(EncryptionKey { key, encrypted_key });
    }

    async fn decrypt_encryption_key(
        &self,
        master_key: &String,
        encrypted_key: &Vec<u8>,
    ) -> Result<EncryptionKey, Error> {
        #[derive(Debug, Serialize)]
        struct Request {
            ciphertext: String,
        }

        #[derive(Debug, Deserialize)]
        struct Response {
            plaintext: String,
        }

        let body = Request {
            ciphertext: STANDARD.encode(encrypted_key),
        };

        let response = self
            .client
            .put(format!("{}/v1/key/decrypt/{}", &self.endpoint, master_key))
            .json(&body)
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await?;

        let data: Response = response.json().await?;
        let key = STANDARD.decode(data.plaintext).unwrap();

        return Ok(EncryptionKey {
            key,
            encrypted_key: encrypted_key.clone(),
        });
    }
}
