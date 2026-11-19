//! Client for the xlb daemon's Unix control socket.
//!
//! The socket carries length-prefixed JSON: a big-endian `u32` byte count,
//! then one JSON document ([`protocol`]). This crate depends only on tokio
//! and serde, so a program can talk to the daemon without linking iroh.
//!
//! ```no_run
//! # async fn example() -> Result<(), xlb_client::ClientError> {
//! let mut client = xlb_client::Client::connect("/run/xlb/xlb.sock").await?;
//! let node = client.inspect().await?;
//! let fetched = client
//!     .fetch("models", "<blake3-hex>", |progress| {
//!         println!("{} bytes via {:?}", progress.bytes_so_far, progress.tier)
//!     })
//!     .await?;
//! println!("{} holds the blob at {}", node.node_id.unwrap_or_default(), fetched.path);
//! # Ok(())
//! # }
//! ```

pub mod protocol;

use std::path::Path;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
};

pub use protocol::{
    ClassInfo, ClassStats, ClassTotals, Command, FetchMetric, FetchProgress, FetchRecord,
    FetchResult, NodeEvent, NodeInfo, Peer, Response, SeedSummary, Tier, TreeInfo,
};

/// Largest frame either side accepts.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum ClientError {
    /// The socket could not be reached or broke.
    Io(std::io::Error),
    /// A frame was oversized, was not valid JSON, or was not the response
    /// the command calls for.
    Protocol(String),
    /// The daemon answered the command with an error.
    Daemon(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "xlb socket: {e}"),
            ClientError::Protocol(message) => write!(f, "xlb protocol: {message}"),
            ClientError::Daemon(message) => write!(f, "xlb daemon: {message}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Io(e) => Some(e),
            ClientError::Protocol(_) | ClientError::Daemon(_) => None,
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Io(e)
    }
}

/// Write `value` as one frame.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let json = serde_json::to_vec(value).map_err(|e| ClientError::Protocol(e.to_string()))?;
    let len = u32::try_from(json.len())
        .ok()
        .filter(|_| json.len() <= MAX_FRAME_BYTES)
        .ok_or_else(|| ClientError::Protocol(format!("frame too large: {} bytes", json.len())))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one frame.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<T, ClientError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(ClientError::Protocol(format!(
            "frame too large: {len} bytes (max {MAX_FRAME_BYTES})"
        )));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(|e| ClientError::Protocol(e.to_string()))
}

/// One connection to the daemon. Commands on a connection run one at a time.
pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ClientError> {
        Ok(Self {
            stream: UnixStream::connect(socket_path).await?,
        })
    }

    pub async fn send(&mut self, command: &Command) -> Result<(), ClientError> {
        write_frame(&mut self.stream, command).await
    }

    pub async fn recv(&mut self) -> Result<Response, ClientError> {
        read_frame(&mut self.stream).await
    }

    /// Send a command that has exactly one response and return it.
    pub async fn roundtrip(&mut self, command: &Command) -> Result<Response, ClientError> {
        self.send(command).await?;
        self.recv().await
    }

    /// This node's ID, bound UDP addresses and cache directory.
    pub async fn inspect(&mut self) -> Result<NodeInfo, ClientError> {
        match self.roundtrip(&Command::Inspect).await? {
            Response::NodeInfo(info) => Ok(info),
            other => Err(unexpected(other)),
        }
    }

    pub async fn class_stats(&mut self, class: &str) -> Result<ClassStats, ClientError> {
        let command = Command::ClassStats {
            class: class.to_string(),
        };
        match self.roundtrip(&command).await? {
            Response::ClassStats(stats) => Ok(stats),
            other => Err(unexpected(other)),
        }
    }

    /// Replace the peer set of every class with a peer tier. Returns how
    /// many classes took it.
    pub async fn set_peers(&mut self, peers: Vec<Peer>) -> Result<u64, ClientError> {
        self.replace_peer_sets(peers, None).await
    }

    /// Replace, on every class with a peer tier, the nodes the daemon reads
    /// from (`peers`) and the node IDs it serves (`admit`). Returns how many
    /// classes took them.
    pub async fn set_peer_sets(
        &mut self,
        peers: Vec<Peer>,
        admit: Vec<String>,
    ) -> Result<u64, ClientError> {
        self.replace_peer_sets(peers, Some(admit)).await
    }

    async fn replace_peer_sets(
        &mut self,
        peers: Vec<Peer>,
        admit: Option<Vec<String>>,
    ) -> Result<u64, ClientError> {
        let command = Command::SetPeers {
            peers,
            admit,
            class: None,
        };
        match self.roundtrip(&command).await? {
            Response::PeersSet { classes } => Ok(classes),
            other => Err(unexpected(other)),
        }
    }

    /// Fetch `hash` (BLAKE3 hex) into the daemon's cache directory and
    /// return where the verified plain file is. `on_progress` sees the
    /// cumulative bytes while a peer or origin transfer runs; a cache hit
    /// reports none.
    pub async fn fetch(
        &mut self,
        class: &str,
        hash: &str,
        mut on_progress: impl FnMut(FetchProgress),
    ) -> Result<FetchResult, ClientError> {
        self.send(&Command::Fetch {
            class: class.to_string(),
            hash: hash.to_string(),
        })
        .await?;
        loop {
            match self.recv().await? {
                Response::FetchProgress(progress) => on_progress(progress),
                Response::FetchResult(result) => return Ok(result),
                other => return Err(unexpected(other)),
            }
        }
    }

    /// Stream node events to `on_event` until it returns `false` or the
    /// daemon hangs up.
    pub async fn subscribe_events(
        &mut self,
        mut on_event: impl FnMut(NodeEvent) -> bool,
    ) -> Result<(), ClientError> {
        self.send(&Command::SubscribeEvents).await?;
        loop {
            match self.recv().await {
                Ok(Response::Event(event)) => {
                    if !on_event(event) {
                        return Ok(());
                    }
                }
                Ok(other) => return Err(unexpected(other)),
                Err(ClientError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(())
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn unexpected(response: Response) -> ClientError {
    match response {
        Response::Error { message } => ClientError::Daemon(message),
        other => ClientError::Protocol(format!("unexpected response: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_fetch_reports_progress_and_returns_the_result() {
        let (near, mut far) = UnixStream::pair().unwrap();
        let mut client = Client { stream: near };
        let server = tokio::spawn(async move {
            let command: Command = read_frame(&mut far).await.unwrap();
            assert_eq!(
                command,
                Command::Fetch {
                    class: "models".into(),
                    hash: "ab".into()
                }
            );
            for bytes_so_far in [4, 8] {
                let progress = Response::FetchProgress(FetchProgress {
                    bytes_so_far,
                    total: Some(8),
                    tier: Tier::Origin,
                });
                write_frame(&mut far, &progress).await.unwrap();
            }
            let result = Response::FetchResult(FetchResult {
                class: "models".into(),
                hash: "ab".into(),
                path: "/cache/ab".into(),
                bytes: 8,
                tier: Tier::Origin,
                elapsed_ms: 1,
            });
            write_frame(&mut far, &result).await.unwrap();
        });

        let mut seen = Vec::new();
        let result = client
            .fetch("models", "ab", |p| seen.push(p.bytes_so_far))
            .await
            .unwrap();
        assert_eq!(seen, [4, 8]);
        assert_eq!(result.path, "/cache/ab");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_daemon_error_is_returned_as_such() {
        let (near, mut far) = UnixStream::pair().unwrap();
        let mut client = Client { stream: near };
        tokio::spawn(async move {
            let _: Command = read_frame(&mut far).await.unwrap();
            let error = Response::Error {
                message: "unknown class: x".into(),
            };
            write_frame(&mut far, &error).await.unwrap();
        });
        let result = client.fetch("x", "ab", |_| {}).await;
        assert!(matches!(result, Err(ClientError::Daemon(m)) if m == "unknown class: x"));
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_before_it_is_read() {
        let (mut near, mut far) = UnixStream::pair().unwrap();
        far.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        let result: Result<Response, _> = read_frame(&mut near).await;
        assert!(matches!(result, Err(ClientError::Protocol(_))));
    }
}
