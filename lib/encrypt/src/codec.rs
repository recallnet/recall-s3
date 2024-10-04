use bytes::{Bytes, BytesMut};
use dare::{DAREDecryptor, DAREError, HEADER_SIZE, MAX_PAYLOAD_SIZE, TAG_SIZE};
use tokio_util::codec::Decoder;

pub struct DareCodec {
    decryptor: DAREDecryptor,
}

impl DareCodec {
    pub fn new(decryptor: DAREDecryptor) -> Self {
        Self { decryptor }
    }
}

impl Decoder for DareCodec {
    type Item = Bytes;
    type Error = DAREError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() >= HEADER_SIZE + MAX_PAYLOAD_SIZE + TAG_SIZE {
            let chunk = src.split_to(HEADER_SIZE + MAX_PAYLOAD_SIZE + TAG_SIZE);
            let (header, message) = (&chunk[..HEADER_SIZE], &chunk[HEADER_SIZE..]);
            let plaintext = self.decryptor.decrypt(header, message)?;
            return Ok(Some(Bytes::from(plaintext)));
        }

        Ok(None)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !src.is_empty() {
            let remaining_chunk = src.split_to(src.len());
            let (header, message) = (
                &remaining_chunk[..HEADER_SIZE],
                &remaining_chunk[HEADER_SIZE..],
            );
            let plaintext = self.decryptor.decrypt(header, message)?;
            return Ok(Some(Bytes::from(plaintext)));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::DareCodec;
    use crate::{Kes, Kms, SealedObjectKey};
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bytes::BufMut;
    use dare::DAREDecryptor;
    use futures::stream::StreamExt;
    use std::io::Cursor;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn test_async_decryption() {
        let encrypted = std::fs::read("/Users/brunocalza/projects/s3-ipc/hello2.bin").unwrap();

        let oek = "10011f000000000093cca72546292b879e5d610ded0dbe57af595eed3571449e295c60d587d83d3fefbdd631d321f2d421a623c370531b0ecb32dc8c948119a0";
        let iv = "4abf5d0d66046ad4a191e188f53ef34fa8977fdb996cae37734a0c5c5bc7c17a";
        let algorithm = "DAREv1-HMAC-SHA256";

        let sealed_object_key =
            SealedObjectKey::new(oek.to_string(), iv.to_string(), algorithm.to_string());

        let cert =
            std::fs::read("/Users/brunocalza/projects/s3-ipc/lib/encrypt/root.cert").unwrap();
        let key = std::fs::read("/Users/brunocalza/projects/s3-ipc/lib/encrypt/root.key").unwrap();

        let kms = Kes::new("https://play.min.io:7373".to_string(), key, cert).unwrap();

        let kek = STANDARD.decode("9s847AOqu6FIZAONO/U/KHHqReBHnqd4se7E7nJowTzuf0fO/HP7UM0KZyWi/KkztHYGSRy3EUpPMqJZx5nNdnmJJG0WioBbmcnB7Q==").unwrap();

        let encryption_key = kms
            .decrypt_encryption_key(&"bcalza-key".to_string(), &kek)
            .await
            .unwrap();
        let object_key = sealed_object_key.unseal(
            &encryption_key,
            &"foo".to_string(),
            &"hello2.txt".to_string(),
        );
        // Decryption
        let decryptor = DAREDecryptor::new(object_key.key);

        let mut framed = Framed::new(Cursor::new(encrypted), DareCodec::new(decryptor));
        let mut decrypted = Vec::new();

        while let Some(chunk) = framed.next().await {
            match chunk {
                Ok(decrypted_data) => {
                    decrypted.put_slice(&decrypted_data[..]);
                }
                Err(e) => {
                    eprintln!("Error decoding: {}", e);
                    break;
                }
            }
        }

        assert_eq!(b"a".repeat(10), decrypted[..10]);
    }
}
