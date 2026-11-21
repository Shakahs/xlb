//! An in-process S3-style HTTP origin for tests. It accepts only correctly
//! SigV4-signed requests, serves byte ranges of one object and the object's
//! outboard, and records every request it accepted.
#![allow(dead_code)]

use std::{
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
};
use xlb::{
    sigv4::{self, SigningInput},
    BlakeHash, Credentials, OriginConfig, OriginSource,
};

pub const BUCKET: &str = "artifacts";
pub const REGION: &str = "auto";

pub fn credentials() -> Credentials {
    Credentials {
        access_key_id: "test-key-id".into(),
        secret_access_key: "test-secret".into(),
    }
}

/// Deterministic bytes that are not a repeating block.
pub fn test_data(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// The outboard the importer publishes for `data`: the pre-order hash pairs
/// of the 16 KiB chunk group tree, without a size prefix.
pub fn outboard_of(data: &[u8]) -> Vec<u8> {
    let block_size = bao_tree::BlockSize::from_chunk_log(4);
    bao_tree::io::outboard::PreOrderMemOutboard::create(data, block_size).data
}

/// One request the server accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub method: String,
    /// Whether the request named the outboard rather than the object.
    pub outboard: bool,
    /// Half-open byte range of a ranged GET.
    pub range: Option<(u64, u64)>,
}

/// How the server answers ranged GETs of the object.
#[derive(Debug, Clone, Copy, Default)]
pub struct RangeBehavior {
    /// Wait this long before answering.
    pub delay: Duration,
    /// Never answer a range that starts at or after this offset, except the
    /// one that ends at the end of the object.
    pub hang_from: Option<u64>,
    /// Once, drop a ranged response after writing a prefix of its body.
    pub disconnect_once: Option<(u64, usize)>,
    /// Corrupt one byte of the body after computing the outboard.
    pub corrupt_at: Option<usize>,
    /// Hold the final range when ranges are paused, for indexed reads without a size probe.
    pub pause_tail: bool,
}

struct Served {
    host: String,
    object_path: String,
    outboard_path: String,
    body: Vec<u8>,
    outboard: Vec<u8>,
    behavior: RangeBehavior,
    disconnected: AtomicBool,
    ranges_paused: watch::Sender<bool>,
    seen: Mutex<Vec<Seen>>,
}

pub struct OriginServer {
    endpoint: String,
    served: Arc<Served>,
}

impl OriginServer {
    /// Serve `body` as the object at `/artifacts/models/blake3/<key_hash>`
    /// and its outboard beside it.
    pub async fn start(key_hash: BlakeHash, body: Vec<u8>) -> Self {
        Self::start_with(key_hash, body, RangeBehavior::default()).await
    }

    pub async fn start_with(key_hash: BlakeHash, mut body: Vec<u8>, behavior: RangeBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        let object_path = format!("/{BUCKET}/models/blake3/{}", key_hash.to_hex());
        let outboard = outboard_of(&body);
        if let Some(offset) = behavior.corrupt_at {
            body[offset] ^= 1;
        }
        let served = Arc::new(Served {
            host: host.clone(),
            outboard_path: format!("{object_path}.obao"),
            object_path,
            outboard,
            body,
            behavior,
            disconnected: AtomicBool::new(false),
            ranges_paused: watch::Sender::new(false),
            seen: Mutex::default(),
        });
        let accepting = served.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve_connection(stream, accepting.clone()));
            }
        });
        Self {
            endpoint: format!("http://{host}"),
            served,
        }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.served.seen.lock().unwrap().clone()
    }

    /// Pause data ranges. By default the final range can still prove the object size.
    pub fn pause_object_ranges(&self, paused: bool) {
        self.served.ranges_paused.send_replace(paused);
    }

    /// The byte ranges of the object requested so far, sorted.
    pub fn object_ranges(&self) -> Vec<(u64, u64)> {
        let mut ranges: Vec<(u64, u64)> = self
            .seen()
            .iter()
            .filter(|seen| !seen.outboard)
            .filter_map(|seen| seen.range)
            .collect();
        ranges.sort();
        ranges
    }

    pub fn config(&self, range_bytes: u64, concurrency: usize) -> OriginConfig {
        OriginConfig {
            source: OriginSource::S3 {
                endpoint: self.endpoint.clone(),
                bucket: BUCKET.into(),
                region: REGION.into(),
                key_template: "models/blake3/{blake3}".into(),
                credentials: credentials(),
            },
            range_bytes,
            concurrency,
            range_retries: 1,
        }
    }
}

async fn serve_connection(mut stream: TcpStream, served: Arc<Served>) {
    let mut buffer = Vec::new();
    loop {
        // Requests carry no body, so a request ends at the blank line.
        let head_end = loop {
            if let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buffer.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
        buffer.drain(..head_end);

        let mut request_line = head.lines().next().unwrap_or_default().split(' ');
        let method = request_line.next().unwrap_or_default().to_string();
        let target = request_line.next().unwrap_or_default().to_string();
        let header = |name: &str| {
            head.lines().skip(1).find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.trim()
                    .eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_string())
            })
        };

        let range_header = header("range");
        let range = range_header.as_deref().and_then(|value| {
            let (start, last) = value.strip_prefix("bytes=")?.split_once('-')?;
            Some((start.parse::<u64>().ok()?, last.parse::<u64>().ok()? + 1))
        });

        // Recompute the signature from what arrived on the wire.
        let signed_range = range_header.as_deref().map(|value| ("range", value));
        let expected = sigv4::sign_with_date(
            &credentials(),
            &SigningInput {
                method: &method,
                host: &header("host").unwrap_or_default(),
                path: &target,
                headers: signed_range.as_slice(),
                payload_sha256: &header("x-amz-content-sha256").unwrap_or_default(),
                region: REGION,
                unix_secs: 0,
            },
            &header("x-amz-date").unwrap_or_default(),
        );
        let authorized = header("authorization").as_deref() == Some(&expected.authorization)
            && header("host").as_deref() == Some(&served.host);

        let (body, outboard) = (&served.body, &served.outboard);
        let object = target == served.object_path;
        let empty: &[u8] = &[];
        let (status, headers, payload): (&str, String, &[u8]) = if !authorized {
            ("403 Forbidden", String::new(), empty)
        } else if !object && target != served.outboard_path {
            ("404 Not Found", String::new(), empty)
        } else {
            served.seen.lock().unwrap().push(Seen {
                method: method.clone(),
                outboard: !object,
                range,
            });
            match (method.as_str(), range) {
                ("HEAD", None) if object => (
                    "200 OK",
                    format!("Content-Length: {}\r\n", body.len()),
                    empty,
                ),
                ("GET", None) if !object => (
                    "200 OK",
                    format!("Content-Length: {}\r\n", outboard.len()),
                    outboard.as_slice(),
                ),
                ("GET", Some((start, end)))
                    if object && end as usize <= body.len() && start < end =>
                {
                    let last = end as usize == body.len();
                    if served.behavior.hang_from.is_some_and(|from| start >= from) && !last {
                        std::future::pending::<()>().await;
                    }
                    if !last || served.behavior.pause_tail {
                        let mut ranges_paused = served.ranges_paused.subscribe();
                        let _ = ranges_paused.wait_for(|paused| !paused).await;
                    }
                    tokio::time::sleep(served.behavior.delay).await;
                    (
                        "206 Partial Content",
                        format!(
                            "Content-Length: {}\r\nContent-Range: bytes {start}-{}/{}\r\n",
                            end - start,
                            end - 1,
                            body.len()
                        ),
                        &body[start as usize..end as usize],
                    )
                }
                _ => ("400 Bad Request", String::new(), empty),
            }
        };
        let content_length = if headers.is_empty() {
            "Content-Length: 0\r\n"
        } else {
            ""
        };
        let response = format!("HTTP/1.1 {status}\r\n{headers}{content_length}\r\n");
        if stream.write_all(response.as_bytes()).await.is_err() {
            return;
        }
        let interrupted = served.behavior.disconnect_once.filter(|&(broken, _)| {
            status == "206 Partial Content" && range.is_some_and(|(start, _)| start == broken)
                && !served.disconnected.swap(true, Ordering::Relaxed)
        });
        if let Some((_, prefix_len)) = interrupted {
            let _ = stream.write_all(&payload[..prefix_len.min(payload.len())]).await;
            return;
        }
        if stream.write_all(payload).await.is_err() {
            return;
        }
    }
}
