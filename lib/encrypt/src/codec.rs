use bytes::{Bytes, BytesMut};
use dare::{DAREDecryptor, DAREError, DAREHeader, HEADER_SIZE, TAG_SIZE};
use tokio_util::codec::Decoder;

pub struct Filter {
    pub offset: u64,
    pub length: u64,
    pub consumed: u64,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    Header,
    Payload((DAREHeader, usize)),
}

/// A codec that decrypts data encrypted using the DARE format.
///
/// You can specify an (offset, length) pair that you want to decrypt from the original content.
pub struct DareCodec {
    decryptor: DAREDecryptor,
    state: DecodeState,
    should_filter: bool,

    // proprieties used in case of filtering
    offset: u64,    // bytes less than offset should be ignored
    consumed: u64,  // how many bytes we have consumed from original content
    remaining: u64, // how many bytes left to return
}

impl DareCodec {
    pub fn new(decryptor: DAREDecryptor) -> Self {
        Self {
            decryptor,
            state: DecodeState::Header,
            should_filter: false,

            offset: 0,
            consumed: 0,
            remaining: 0,
        }
    }

    pub fn with_filter(decryptor: DAREDecryptor, filter: Filter) -> Self {
        Self {
            decryptor,
            state: DecodeState::Header,
            should_filter: true,

            offset: filter.offset,
            consumed: filter.consumed,
            remaining: filter.length,
        }
    }
    fn decode_header(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<(DAREHeader, usize)>, DAREError> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        let header = src.split_to(HEADER_SIZE);
        let header = DAREHeader::from_bytes(header.as_ref())?;

        let payload_size = header.payload_size();
        let payload_size_with_tag = payload_size as usize + TAG_SIZE;

        // Ensure that the buffer has enough space to read the incoming
        // payload
        src.reserve(payload_size_with_tag.saturating_sub(src.len()));

        Ok(Some((header, payload_size_with_tag)))
    }

    fn decode_payload(&self, n: usize, src: &mut BytesMut) -> Option<BytesMut> {
        if src.len() < n {
            return None;
        }

        Some(src.split_to(n))
    }

    fn filter_bytes(&mut self, plaintext: &[u8]) -> Option<Bytes> {
        if !self.should_filter {
            return Some(Bytes::copy_from_slice(plaintext));
        }

        let plaintext_size = plaintext.len() as u64;

        // We haven't reached offset yet, we must ignore the decrypted content
        if self.consumed + plaintext_size <= self.offset {
            self.consumed += plaintext_size;
            return Some(Bytes::new());
        }

        // We reached offset, so we take the bytes from offset up to the end of package
        //
        // +---------------------------------+
        // |  DISCARD  |      GRAB THIS      |
        // +---------------------------------+
        // |           |
        // consumed    offset
        //
        let plaintext_within_range = &plaintext[(self.offset - self.consumed) as usize..];
        let plaintext_within_range_size = plaintext_within_range.len() as u64;

        // if grabbed fewer bytes than the remaining bytes to take, we return it all
        //
        // +---------------------------------+-----------------------
        // |  DISCARD  |      GRAB THIS      |       NEXT PACKAGE
        // +---------------------------------+-----------------------
        // |           |                                   |
        // consumed    offset                              remaining
        //
        if plaintext_within_range_size <= self.remaining {
            self.consumed += plaintext_size;
            self.offset = self.consumed;
            self.remaining -= plaintext_within_range_size;
            return Some(Bytes::copy_from_slice(plaintext_within_range));
        }

        // if not, we must take up to the remaining
        //
        // +---------------------------------+
        // |  DISCARD  | GRAB THIS | DISCARD |
        // +---------------------------------+
        // |           |           |
        // consumed    offset      remaining
        //
        let plaintext_within_range = &plaintext_within_range[..self.remaining as usize];
        let plaintext_within_range_size = plaintext_within_range.len() as u64;
        self.consumed += plaintext_size;
        self.offset = self.consumed;
        self.remaining -= plaintext_within_range_size;

        Some(Bytes::copy_from_slice(plaintext_within_range))
    }
}

impl Decoder for DareCodec {
    type Item = Bytes;
    type Error = DAREError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (header, n) = match self.state {
            DecodeState::Header => match self.decode_header(src)? {
                Some((header, n)) => {
                    self.state = DecodeState::Payload((header, n));
                    (header, n)
                }
                None => return Ok(None),
            },
            DecodeState::Payload((header, n)) => (header, n),
        };

        match self.decode_payload(n, src) {
            Some(data) => {
                self.state = DecodeState::Header;

                let plaintext = self.decryptor.decrypt(&header.to_bytes(), data.as_ref())?;
                let data = self.filter_bytes(plaintext.as_slice());

                // Make sure the buffer has enough space to read the next header
                src.reserve(HEADER_SIZE.saturating_sub(src.len()));

                Ok(data)
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{DareCodec, Filter};
    use dare::{CipherSuite, DAREDecryptor, DAREEncryptor};
    use std::io::Cursor;
    use std::str;
    use tokio_stream::StreamExt;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn test_dare_codec() {
        let key = [0u8; 32]; // In practice, use a secure random key

        // Let's generate a content of 200,000 bytes, that repeats itself every 5 bytes.
        // This content requires 4 packages (20,000 / 65536 ) for encryption.
        //
        // 0               65535              131071              196607    199999
        // +-------------------+---------------------------------------+---------+
        // |       64KiB       |       64KiB       |       64KiB       | 3392 B  |
        // +-------------------+---------------------------------------+---------+
        // abcde.............deabcdea............eabcdea.............abcdea......e
        let plaintext = b"abcde".repeat(40000).to_vec();

        // Encryption
        let mut encryptor =
            DAREEncryptor::new(key, CipherSuite::AES256GCM).expect("should not fail");
        let mut encrypted = Vec::new();
        let mut plaintext_cursor = Cursor::new(&plaintext);
        encryptor
            .encrypt_stream(&mut plaintext_cursor, &mut encrypted)
            .await
            .unwrap();

        struct TestSpec {
            offset: u64,
            length: u64,
            expected_frames: Vec<&'static str>,
        }

        let tests = vec![
            TestSpec {
                offset: 0,
                length: 5,
                expected_frames: vec!["abcde", "", "", ""],
            },
            TestSpec {
                offset: 0,
                length: 6,
                expected_frames: vec!["abcdea", "", "", ""],
            },
            TestSpec {
                offset: 65533,
                length: 3,
                expected_frames: vec!["dea", "", "", ""],
            },
            TestSpec {
                offset: 65533,
                length: 8,
                expected_frames: vec!["dea", "bcdea", "", ""],
            },
            TestSpec {
                offset: 69999,
                length: 1,
                expected_frames: vec!["", "e", "", ""],
            },
            TestSpec {
                offset: 131069,
                length: 7,
                expected_frames: vec!["", "eab", "cdea", ""],
            },
            TestSpec {
                offset: 196605,
                length: 6,
                expected_frames: vec!["", "", "abc", "dea"],
            },
            TestSpec {
                offset: 199999,
                length: 1,
                expected_frames: vec!["", "", "", "e"],
            },
        ];

        for test in tests {
            let decryptor = DAREDecryptor::new(key);
            let encrypted_cursor = Cursor::new(&mut encrypted);
            let mut framed = Framed::new(
                encrypted_cursor,
                DareCodec::with_filter(
                    decryptor,
                    Filter {
                        offset: test.offset,
                        length: test.length,
                        consumed: 0,
                    },
                ),
            );

            let mut frames = Vec::new();
            while let Some(frame) = framed.next().await {
                match frame {
                    Ok(data) => frames.push(str::from_utf8(data.as_ref()).unwrap().to_string()),
                    Err(_) => {
                        break;
                    }
                }
            }

            assert_eq!(test.expected_frames, frames)
        }
    }
}
