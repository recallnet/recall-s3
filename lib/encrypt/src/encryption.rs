use std::pin::Pin;
use std::task::{Context, Poll};

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use pin_project::pin_project;
use rand::rngs::OsRng;
use rand::RngCore;
use tokio::io::AsyncWrite;

const CHUNK_SIZE: usize = 4096;

#[pin_project]
pub struct EncryptedWriter<W> {
    #[pin]
    inner: W,
    cipher: Aes256Gcm,
    nonce: [u8; 12],
    buffer: Vec<u8>,   // Buffer to store unencrypted data
    block_size: usize, // Usually 4096 bytes for encryption
}

impl<W: AsyncWrite> EncryptedWriter<W> {
    pub fn new(inner: W, key: &[u8]) -> Self {
        let cipher = Aes256Gcm::new_from_slice(key).expect("invalid key size");

        // Generate a random nonce
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);

        let nonce = nonce_bytes;

        Self {
            inner,
            cipher,
            nonce,
            buffer: Vec::with_capacity(CHUNK_SIZE),
            block_size: CHUNK_SIZE,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for EncryptedWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();

        this.buffer.extend_from_slice(buf);
        while this.buffer.len() >= *this.block_size {
            let to_encrypt = this.buffer.drain(..*this.block_size).collect::<Vec<u8>>();
            let nonce = Nonce::from_slice(this.nonce.as_slice());
            let encrypted_data = this
                .cipher
                .encrypt(nonce, &*to_encrypt)
                .expect("encryption failure");

            let write_result = Pin::new(&mut this.inner).poll_write(cx, &encrypted_data)?;
            if write_result.is_pending() {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        if !this.buffer.is_empty() {
            let mut last_block = std::mem::take(this.buffer);
            let padding_len = *this.block_size - last_block.len();
            last_block.extend(vec![0u8; padding_len]);
            let nonce = Nonce::from_slice(this.nonce.as_slice());
            let encrypted_data = this
                .cipher
                .encrypt(nonce, &*last_block)
                .expect("encryption failure");

            let write_result = Pin::new(&mut this.inner).poll_write(cx, &encrypted_data)?;
            if write_result.is_pending() {
                return Poll::Pending;
            }
        }

        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        let _ = this.inner.as_mut().poll_flush(cx)?;

        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

pub fn encrypt_writer<R: AsyncWrite + Unpin>(reader: R, key: &[u8]) -> EncryptedWriter<R> {
    EncryptedWriter::new(reader, key)
}

/// Encrypts data using AES-256-GCM
///
/// # Arguments
///
/// * `key` - A 256-bit key as a byte array.
/// * `data` - The data to encrypt.
///
/// Returns a tuple containing the nonce and the ciphertext.
pub fn encrypt(key: &[u8], data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let cipher = Aes256Gcm::new_from_slice(key).unwrap();

    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher.encrypt(nonce, data).expect("encryption failure!");

    (nonce_bytes.to_vec(), ciphertext)
}

/// Decrypts data using AES-256-GCM
///
/// # Arguments
///
/// * `key` - A 256-bit key as a byte array.
/// * `nonce` - The nonce used for encryption.
/// * `ciphertext` - The encrypted data.
///
/// Returns the decrypted plaintext.
pub fn decrypt(key: &[u8; 32], nonce: &[u8], ciphertext: &[u8]) -> Vec<u8> {
    let cipher = <Aes256Gcm as KeyInit>::new_from_slice(key).unwrap();

    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .expect("decryption failure!");

    plaintext
}
