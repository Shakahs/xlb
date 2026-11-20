//! Bridges the daemon's Tokio runtime to tokio-uring's local runtime.

use std::{fs::File, io};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tokio::sync::{mpsc, oneshot};

const READ_BYTES: usize = 64 * 1024;

pub(crate) struct ChunkSlice {
    pub(crate) file: File,
    pub(crate) start: u64,
    pub(crate) end: u64,
}

struct ChunkRead {
    slice: ChunkSlice,
    output: mpsc::Sender<io::Result<Bytes>>,
}

#[derive(Clone)]
pub(crate) struct DiskChunks(mpsc::Sender<ChunkRead>);

fn messages<T>(receiver: mpsc::Receiver<T>) -> impl Stream<Item = T> {
    stream::unfold(receiver, |mut receiver| async { receiver.recv().await.map(|item| (item, receiver)) })
}

impl DiskChunks {
    pub(crate) async fn start(depth: usize) -> io::Result<Self> {
        let (requests, pending) = mpsc::channel::<ChunkRead>(depth);
        let (startup, ready) = oneshot::channel();
        std::thread::Builder::new().name("xlb-cache-reader".into()).spawn(move || {
            match tokio_uring::Runtime::new(&tokio_uring::builder()) {
                Ok(runtime) => {
                    if startup.send(Ok(())).is_ok() {
                        runtime.block_on(messages(pending).for_each(|request| async {
                            tokio_uring::spawn(request.read());
                        }));
                    }
                }
                Err(err) => { let _ = startup.send(Err(err)); }
            }
        })?;
        ready.await.map_err(io::Error::other)??;
        Ok(Self(requests))
    }

    pub(crate) async fn read(&self, slice: ChunkSlice) -> io::Result<impl Stream<Item = io::Result<Bytes>> + use<>> {
        let (output, body) = mpsc::channel(2);
        self.0.send(ChunkRead { slice, output }).await
            .map_err(|_| io::Error::other("the io_uring reader stopped"))?;
        Ok(messages(body))
    }
}

impl ChunkRead {
    async fn read(self) {
        let ChunkSlice { file, start, end } = self.slice;
        let file = tokio_uring::fs::File::from_std(file);
        let reader = &file;
        let stream = stream::try_unfold(start, move |offset| async move {
            if offset >= end {
                return Ok(None);
            }
            let len = (end - offset).min(READ_BYTES as u64) as usize;
            let (result, buffer) = reader.read_at(BytesMut::with_capacity(len), offset).await;
            let count = result?;
            if count == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "cached image chunk ended early"));
            }
            Ok(Some((buffer.freeze(), offset + count as u64)))
        });
        let result = stream.try_for_each(|bytes| async {
            self.output.send(Ok(bytes)).await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
        }).await;
        if let Err(err) = result {
            let _ = self.output.send(Err(err)).await;
        }
        let _ = file.close().await;
    }
}
