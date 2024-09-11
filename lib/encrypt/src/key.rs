use crate::EncryptionKey;
use dare::{CipherSuite, DAREDecryptor, DAREEncryptor, HEADER_SIZE};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::path::Path;

#[derive(Debug)]
pub struct ObjectKey {
    pub key: [u8; 32],
}

#[derive(Debug)]
pub struct SealedObjectKey {
    key: Vec<u8>,
    iv: [u8; 32],
    algorithm: String,
}

impl SealedObjectKey {
    pub fn new(key: String, iv_str: String, algorithm: String) -> SealedObjectKey {
        let key = hex_simd::decode_to_vec(key).unwrap();
        let iv = hex_simd::decode_to_vec(iv_str.as_bytes())
            .unwrap()
            .as_slice()[0..32]
            .try_into()
            .unwrap();

        SealedObjectKey { key, iv, algorithm }
    }
    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
    }

    pub fn algorithm(&self) -> String {
        self.algorithm.clone()
    }

    pub fn iv_as_hex(&self) -> String {
        hex_simd::encode_to_string(self.iv.as_ref(), hex_simd::AsciiCase::Lower)
    }

    pub fn key_as_hex(&self) -> String {
        hex_simd::encode_to_string(self.key.as_slice(), hex_simd::AsciiCase::Lower)
    }

    pub fn unseal(&self, kek: &EncryptionKey, bucket: &String, object: &String) -> ObjectKey {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&kek.key()).expect("HMAC can take key of any size");

        // Write data to the MAC
        mac.update(self.iv.as_slice()); // iv
        mac.update("DAREv1-HMAC-SHA256".as_bytes());

        let path = Path::new(&bucket).join(object);
        mac.update(path.to_str().unwrap().as_bytes());

        // Compute the final HMAC and store it in sealing_key
        let bytes = mac.finalize().into_bytes();
        let sealing_key = bytes.as_slice()[0..32].try_into().unwrap();

        let mut decryptor = DAREDecryptor::new(sealing_key);
        let ciphertext = self.key.as_slice();
        let key = decryptor
            .decrypt(&ciphertext[..HEADER_SIZE], &ciphertext[HEADER_SIZE..])
            .unwrap();

        ObjectKey {
            key: key.as_slice()[0..32].try_into().unwrap(),
        }
    }
}

impl ObjectKey {
    pub fn seal(
        &self,
        kek: &EncryptionKey,
        iv: &[u8; 32],
        bucket: &String,
        object: &String,
    ) -> SealedObjectKey {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&kek.key()).expect("HMAC can take key of any size");

        // Write data to the MAC
        mac.update(iv); // iv
        mac.update("DAREv1-HMAC-SHA256".as_bytes());

        let path = Path::new(&bucket).join(object);
        mac.update(path.to_str().unwrap().as_bytes());

        // Compute the final HMAC and store it in sealing_key
        let bytes = mac.finalize().into_bytes();
        let sealing_key = bytes.as_slice()[0..32].try_into().unwrap();

        let mut encryptor = DAREEncryptor::new(sealing_key, CipherSuite::CHACHA20POLY1305).unwrap();
        let cipher_text = encryptor.encrypt(&self.key).unwrap();

        SealedObjectKey {
            iv: *iv,
            key: cipher_text,
            algorithm: "DAREv1-HMAC-SHA256".to_string(),
        }
    }
}
