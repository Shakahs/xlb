//! Share contiguous network buffers with iroh-blobs' exact-read API.

use std::{future::Future, io};

use bytes::{Bytes, BytesMut};

pub(crate) trait ByteFragments: Send {
    fn next_bytes(&mut self, limit: usize) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;
}

/// Keep the original buffer when it contains the whole verification group.
pub(crate) async fn receive(reader: &mut impl ByteFragments, len: usize) -> io::Result<Bytes> {
    if len == 0 {
        return Ok(Bytes::new());
    }
    let Some(head) = reader.next_bytes(len).await? else { return Ok(Bytes::new()) };
    if head.len() == len {
        return Ok(head);
    }
    let mut joined = BytesMut::with_capacity(len);
    joined.extend_from_slice(&head);
    while joined.len() < len {
        let Some(fragment) = reader.next_bytes(len - joined.len()).await? else { break };
        joined.extend_from_slice(&fragment);
    }
    Ok(joined.freeze())
}

pub(crate) async fn receive_exact(reader: &mut impl ByteFragments, len: usize) -> io::Result<Bytes> {
    let bytes = receive(reader, len).await?;
    if bytes.len() == len {
        Ok(bytes)
    } else {
        Err(io::ErrorKind::UnexpectedEof.into())
    }
}

pub(crate) struct QuicBytes(pub(crate) iroh::endpoint::RecvStream);

impl ByteFragments for QuicBytes {
    async fn next_bytes(&mut self, limit: usize) -> io::Result<Option<Bytes>> {
        Ok(self.0.read_chunk(limit).await?)
    }
}

impl iroh_blobs::util::RecvStream for QuicBytes {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        receive(self, len).await
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        receive_exact(self, len).await
    }

    async fn recv_exact(&mut self, target: &mut [u8]) -> io::Result<()> {
        use iroh::endpoint::ReadExactError;

        self.0.read_exact(target).await.map_err(|error| match error {
            ReadExactError::FinishedEarly(_) => io::Error::new(io::ErrorKind::UnexpectedEof, error),
            ReadExactError::ReadError(error) => error.into(),
        })
    }

    fn stop(&mut self, code: iroh::endpoint::VarInt) -> io::Result<()> {
        Ok(self.0.stop(code)?)
    }

    fn id(&self) -> u64 {
        self.0.id().index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Input(VecDeque<Bytes>);

    impl ByteFragments for Input {
        async fn next_bytes(&mut self, limit: usize) -> io::Result<Option<Bytes>> {
            let Some(mut bytes) = self.0.pop_front() else { return Ok(None) };
            if bytes.len() > limit {
                let prefix = bytes.split_to(limit);
                self.0.push_front(bytes);
                Ok(Some(prefix))
            } else {
                Ok(Some(bytes))
            }
        }
    }

    // [utest -> dsn~xlb-chunk-discovery~1]
    #[tokio::test]
    async fn contiguous_payload_keeps_its_buffer_and_fragmented_payload_keeps_its_bytes() {
        let body = Bytes::from(vec![10, 20, 30, 40, 50]);
        let mut input = Input(VecDeque::from([body.clone(), Bytes::from_static(&[60, 70])]));
        let shared = receive_exact(&mut input, 3).await.unwrap();
        assert_eq!(shared.as_ptr(), body.as_ptr());
        assert_eq!(receive_exact(&mut input, 4).await.unwrap(), &[40, 50, 60, 70][..]);
    }
}
