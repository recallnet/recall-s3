pub mod codec;
mod key;
mod kms;

pub use codec::*;
pub use key::*;
pub use kms::*;

use std::error::Error;

use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::Sha256;

pub fn generate_object_key(
    kek: &EncryptionKey,
    random: Option<&mut dyn RngCore>,
) -> Result<ObjectKey, Box<dyn Error>> {
    let random = match random {
        Some(r) => r,
        None => &mut OsRng,
    };

    // Generate nonce
    let mut nonce = [0u8; 32];
    random.fill_bytes(&mut nonce);

    // Define the context
    const CONTEXT: &str = "object-encryption-key generation";

    // HMAC setup
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&kek.key()[..])?;
    mac.update(CONTEXT.as_bytes());
    mac.update(&nonce);

    // Finalize and return the key
    let result = mac.finalize();
    let key = result.into_bytes();

    let mut object_key = [0u8; 32];
    object_key.copy_from_slice(&key[..32]);

    Ok(ObjectKey { key: object_key })
}

#[cfg(test)]
mod tests {

    use crate::kms::Kes;
    use crate::kms::Kms;

    use std::fs;

    // #[test]
    // fn test_generate_object_key() {
    //     let kek = "zEOVQr9gsarxgezzaeAPP1k6PAamdSzjkyZ+C0Ysh8s=";
    //     let kek_vec = decode(kek).unwrap();
    //     let decoded_kek = kek_vec.as_slice()[0..32].try_into().unwrap();
    //
    //     let random = &mut OsRng;
    //     let object_key = generate_object_key(decoded_kek, Some(random)).unwrap();
    //
    //     dbg!(&object_key);
    //
    //     let mut iv: [u8; 32] = [0x00; 32]; // Use a proper random key in production
    //     OsRng.fill_bytes(&mut iv);
    //
    //     let sealed_object_key = object_key.seal(
    //         decoded_kek,
    //         &iv,
    //         &"my_bucket".to_string(),
    //         &"my_object".to_string(),
    //     );
    //
    //     dbg!(sealed_object_key);
    // }

    #[tokio::test]
    async fn test_fetch_encryption_key() {
        let cert = fs::read("./root.cert").unwrap();
        let key = fs::read("./root.key").unwrap();

        let kms = Kes::new("https://play.min.io:7373".to_string(), key, cert).unwrap();
        match kms.fetch_encryption_key(&"bcalza-key".to_string()).await {
            Err(err) => {
                dbg!(err);
            }
            Ok(ek) => {
                kms.decrypt_encryption_key(&"bcalza-key".to_string(), &ek.encrypted_key())
                    .await
                    .unwrap();
            }
        }
    }
}
