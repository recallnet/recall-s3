use std::collections::VecDeque;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::*;

use bytes::{Buf, BufMut, Bytes};
use dare::{DAREDecryptor, DAREHeader, HEADER_SIZE, MAX_PAYLOAD_SIZE, TAG_SIZE};
use futures::{ready, Stream, StreamExt};
use md5::{Digest, Md5};
use pin_project::pin_project;
use s3s::dto::StreamingBlob;
use s3s::StdError;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub async fn copy_bytes<S, W>(mut stream: S, writer: &mut W) -> Result<u64>
where
    S: Stream<Item = Result<Bytes, StdError>> + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut nwritten: u64 = 0;
    while let Some(result) = stream.next().await {
        let bytes = match result {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e)),
        };
        writer.write_all(&bytes).await?;
        nwritten += bytes.len() as u64;
    }
    writer.flush().await?;
    Ok(nwritten)
}

pub fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}

pub struct HashReader<R> {
    inner: R,
    hasher: Md5,
}

impl<R> HashReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: <Md5 as Digest>::new(),
        }
    }

    pub fn finalize(self) -> Vec<u8> {
        self.hasher.finalize().to_vec()
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for HashReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // in case the buf is already filled, save its length
        let pos = buf.filled().len();

        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes = buf.filled();
                self.hasher.update(&bytes[pos..bytes.len()]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

pub struct StreamingBlobReader {
    inner: StreamingBlob,
    buffer: VecDeque<u8>,
}

impl StreamingBlobReader {
    pub fn new(inner: StreamingBlob) -> Self {
        Self {
            inner,
            buffer: VecDeque::new(),
        }
    }
}

impl AsyncRead for StreamingBlobReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        while this.buffer.is_empty() {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    let _ = this.buffer.write_all(bytes.chunk());
                }
                Poll::Ready(Some(Err(_))) => {
                    unreachable!()
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        while buf.remaining() > 0 && !this.buffer.is_empty() {
            buf.put_u8(this.buffer.pop_front().unwrap());
        }

        Poll::Ready(Ok(()))
    }
}

#[pin_project(project = DecryptingReaderStateProj)]
#[derive(Debug)]
enum DecryptingReaderState {
    ReadingHeader,
    ReadingMessage(#[pin] Vec<u8>),
    Decrypt(#[pin] Vec<u8>, #[pin] Vec<u8>),
    //WritingPlaintext(#[pin] Vec<u8>),
}

#[pin_project]
pub struct DecryptingReader<R> {
    #[pin]
    inner: R,
    //#[pin]
    buffer: Vec<u8>,
    #[pin]
    decrypter: DAREDecryptor,
    #[pin]
    state: DecryptingReaderState,
    position: usize,
    passed: bool,
}

impl<R: AsyncRead> DecryptingReader<R> {
    pub fn new(inner: R, decrypter: DAREDecryptor) -> Self {
        Self {
            inner,
            decrypter,
            buffer: vec![0; HEADER_SIZE + MAX_PAYLOAD_SIZE + TAG_SIZE],
            position: 0,
            state: DecryptingReaderState::ReadingHeader,
            passed: false,
        }
    }
}
impl<R: AsyncRead> AsyncRead for DecryptingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            use DecryptingReaderState::*;

            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                DecryptingReaderStateProj::ReadingHeader => {
                    this.buffer.resize(HEADER_SIZE, 0);
                    let read_buf = &mut ReadBuf::new(this.buffer);

                    match ready!(this.inner.as_mut().poll_read(cx, read_buf)) {
                        Ok(()) => {
                            if this.buffer.len() >= HEADER_SIZE {
                                let header = &this.buffer[*this.position..HEADER_SIZE];
                                *this.position += HEADER_SIZE;
                                this.state.set(ReadingMessage(header.to_vec()));
                            }
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    };
                }
                DecryptingReaderStateProj::ReadingMessage(header) => {
                    //TODO: handle error
                    let dare_header = DAREHeader::from_bytes(&header).unwrap();
                    let message_size = dare_header.payload_size() as usize + TAG_SIZE;

                    this.buffer.resize(message_size, 0);

                    let read_buf = &mut ReadBuf::new(this.buffer);
                    match ready!(this.inner.as_mut().poll_read(cx, read_buf)) {
                        Ok(()) => {
                            if this.buffer.len() >= message_size {
                                let message = &this.buffer[..];
                                //*this.position = *this.position + message_size;
                                let h = header.to_vec();
                                this.state.set(Decrypt(h, message.to_vec()));
                            }
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    };
                }
                DecryptingReaderStateProj::Decrypt(header, message) => {
                    let decrypter = &mut this.decrypter;
                    //TODO: handle error
                    let decrypted = decrypter.decrypt(&header, &message).unwrap();
                    println!("{}", String::from_utf8(decrypted.clone()).unwrap().len());

                    buf.put_slice(&decrypted[..]);

                    this.state.set(ReadingHeader);

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{hex, DecryptingReader, HashReader};
    use dare::CipherSuite::AES256GCM;
    use dare::{DAREDecryptor, DAREEncryptor};
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_hasher() {
        let data = Cursor::new("The quick brown fox jumps over the lazy dog");
        let reader = tokio::io::BufReader::new(data);
        let mut hash_reader = HashReader::new(reader);
        let _ = tokio::io::copy(&mut hash_reader, &mut tokio::io::empty())
            .await
            .unwrap();
        let hash = hex(hash_reader.finalize());

        assert_eq!("9e107d9d372bb6826bd81d3542a419d6", hash);
    }

    #[tokio::test]
    async fn test_decryption() {
        let key = [0u8; 32];
        let plaintext = b"A".repeat(100);

        // Encryption
        let mut encryptor = DAREEncryptor::new(key, AES256GCM).unwrap();
        let mut encrypted = Vec::new();
        let mut plaintext_cursor = Cursor::new(&plaintext);
        encryptor
            .encrypt_stream(&mut plaintext_cursor, &mut encrypted)
            .await
            .unwrap();

        let decryptor = DAREDecryptor::new(key);

        let mut reader = DecryptingReader::new(Cursor::new(encrypted), decryptor);
        let mut decrypted = vec![0; plaintext.len()];

        reader.read_exact(&mut decrypted).await.unwrap();
        assert_eq!(plaintext.to_vec(), decrypted);
    }
}
