use crate::error::*;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use md5::{Digest, Md5};
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

#[cfg(test)]
mod tests {
    use crate::utils::{hex, HashReader};
    use std::io::Cursor;

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
}
