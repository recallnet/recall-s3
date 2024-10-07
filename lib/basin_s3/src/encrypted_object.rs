use crate::Error;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use dare::{HEADER_SIZE, MAX_PAYLOAD_SIZE, TAG_SIZE};
use encrypt::SealedObjectKey;
use fendermint_actor_objectstore::Object;

pub struct EncryptedObject {
    oek: String,
    iv: String,
    algorithm: String,
    master_key: String,
    kek: String,
    content_length: u64,
}

impl EncryptedObject {
    pub fn new(object: &Object) -> Result<EncryptedObject, Error> {
        let metadata = &object.metadata;
        let iv = metadata.get("sse_iv").unwrap().clone();
        let oek = metadata.get("sse_oek").unwrap().clone();
        let algorithm = metadata.get("sse_algorithm").unwrap().clone();

        let master_key = metadata.get("sse_master_key").unwrap().clone();
        let kek = metadata.get("sse_kek").unwrap().clone();

        let content_length = (&object.size).clone();

        Ok(EncryptedObject {
            oek,
            iv,
            algorithm,
            master_key,
            kek,
            content_length,
        })
    }

    pub fn master_key(&self) -> &String {
        return &self.master_key;
    }

    pub fn kek_to_vec(&self) -> Vec<u8> {
        return STANDARD.decode(&self.kek).unwrap()
    }

    pub fn sealed_object_key(&self) -> SealedObjectKey {
        SealedObjectKey::new(self.oek.clone(), self.iv.clone(), self.algorithm.clone())
    }

    pub fn decrypted_content_length(&self) -> u64 {
        let package_size = HEADER_SIZE + MAX_PAYLOAD_SIZE + TAG_SIZE;
        let content_length = self.content_length as usize;

        let n_package = (content_length + package_size - 1) / package_size;

        (content_length - (n_package * (HEADER_SIZE + TAG_SIZE))) as u64
    }
}
