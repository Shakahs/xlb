//! Test S3 endpoint that verifies signed requests for a whole CAS chunk.

mod sigv4;

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{BlakeHash, Credentials, OriginConfig};
use bytes::Bytes;

const BUCKET: &str = "artifacts";
const REGION: &str = "auto";

fn credentials() -> Credentials {
    Credentials {
        access_key_id: "test-key-id".into(),
        secret_access_key: "test-secret".into(),
    }
}

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

#[derive(Clone, Copy)]
pub enum FirstGet {
    Throttle,
    Disconnect(usize),
}

struct Served {
    host: String,
    bodies: HashMap<String, Bytes>,
    first_get: Option<FirstGet>,
    paused: watch::Sender<bool>,
    gets: AtomicUsize,
    get_offsets: std::sync::Mutex<Vec<usize>>,
    connections: AtomicUsize,
}

pub struct OriginServer {
    served: Arc<Served>,
    _server: AbortOnDropHandle<()>,
}

impl OriginServer {
    pub async fn start(hash: BlakeHash, body: Vec<u8>) -> Self {
        Self::start_with(hash, body, None).await
    }

    pub async fn start_with(hash: BlakeHash, body: Vec<u8>, first_get: Option<FirstGet>) -> Self {
        Self::start_many_with(vec![(hash, Bytes::from(body))], first_get).await
    }

    pub async fn start_many(bodies: Vec<(BlakeHash, Bytes)>) -> Self {
        Self::start_many_with(bodies, None).await
    }

    async fn start_many_with(bodies: Vec<(BlakeHash, Bytes)>, first_get: Option<FirstGet>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let served = Arc::new(Served {
            host: listener.local_addr().unwrap().to_string(),
            bodies: bodies.into_iter().map(|(hash, body)| (format!("/{BUCKET}/models/blake3/{}", hash.to_hex()), body)).collect(),
            first_get,
            paused: watch::Sender::new(false),
            gets: AtomicUsize::new(0),
            get_offsets: std::sync::Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
        });
        let accepting = served.clone();
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { return };
                        accepting.connections.fetch_add(1, Ordering::Relaxed);
                        connections.spawn(serve_connection(stream, accepting.clone()));
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self { served, _server: AbortOnDropHandle::new(server) }
    }

    pub fn gets(&self) -> usize {
        self.served.gets.load(Ordering::Relaxed)
    }

    /// The first byte each GET asked for, in arrival order.
    pub fn get_offsets(&self) -> Vec<usize> {
        self.served.get_offsets.lock().unwrap().clone()
    }

    pub fn connections(&self) -> usize {
        self.served.connections.load(Ordering::Relaxed)
    }

    pub fn pause(&self, paused: bool) {
        self.served.paused.send_replace(paused);
    }

    pub fn config(&self, concurrency: usize) -> OriginConfig {
        OriginConfig {
            endpoint: format!("http://{}", self.served.host),
            bucket: BUCKET.into(),
            region: REGION.into(),
            key_template: "models/blake3/{blake3}".into(),
            credentials: credentials(),
            concurrency: NonZeroUsize::new(concurrency).unwrap(),
            retries: 1,
        }
    }
}

/// The `[start, end)` span of a single `bytes=start-[last]` range.
fn byte_range(value: &str, length: usize) -> Option<(usize, usize)> {
    let (start, last) = value.strip_prefix("bytes=")?.split_once('-')?;
    let start: usize = start.parse().ok()?;
    let end = if last.is_empty() { length } else { last.parse::<usize>().ok()?.saturating_add(1).min(length) };
    (start < end).then_some((start, end))
}

async fn serve_connection(mut stream: TcpStream, served: Arc<Served>) {
    while serve_request(&mut stream, &served).await {}
}

async fn serve_request(stream: &mut TcpStream, served: &Served) -> bool {
    let mut buffer = Vec::new();
    while !buffer.ends_with(b"\r\n\r\n") {
        let mut bytes = [0; 4096];
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return false,
            Ok(length) => buffer.extend_from_slice(&bytes[..length]),
        }
    }
    let head = String::from_utf8_lossy(&buffer);
    let mut request = head.lines().next().unwrap_or_default().split(' ');
    let method = request.next().unwrap_or_default();
    let path = request.next().unwrap_or_default();
    let version = request.next().unwrap_or_default();
    let header = |name: &str| {
        head.lines().skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        }).unwrap_or_default()
    };
    let auth = header("authorization");
    let signed = auth.split("SignedHeaders=").nth(1).and_then(|value| value.split_once(',')).map(|(names, _)| names);
    let extra: Vec<_> = signed.unwrap_or_default().split(';')
        .filter(|name| !matches!(*name, "" | "host" | "x-amz-date" | "x-amz-content-sha256"))
        .map(|name| (name, header(name)))
        .collect();
    let expected = sigv4::sign_with_date(
        &credentials(),
        &sigv4::SigningInput {
            method,
            host: header("host"),
            path,
            headers: &extra,
            payload_sha256: header("x-amz-content-sha256"),
            region: REGION,
            unix_secs: 0,
        },
        header("x-amz-date"),
    );
    let mut first_get = None;
    let mut content_range = String::new();
    let (status, length, payload) = if version != "HTTP/1.1" {
        ("505 HTTP Version Not Supported", 0, &[][..])
    } else if auth != expected.authorization || header("host") != served.host {
        ("403 Forbidden", 0, &[][..])
    } else if !served.bodies.contains_key(path) {
        ("404 Not Found", 0, &[][..])
    } else if method == "HEAD" {
        ("200 OK", served.bodies[path].len(), &[][..])
    } else if method == "GET" {
        let body = served.bodies[path].as_ref();
        let range = header("range");
        let span = if range.is_empty() { Some((0, body.len())) } else { byte_range(range, body.len()) };
        if served.gets.fetch_add(1, Ordering::Relaxed) == 0 {
            first_get = served.first_get;
        }
        served.get_offsets.lock().unwrap().push(span.map_or(0, |(start, _)| start));
        let mut paused = served.paused.subscribe();
        let _ = paused.wait_for(|paused| !paused).await;
        match span {
            _ if matches!(first_get, Some(FirstGet::Throttle)) => ("429 Too Many Requests", 0, &[][..]),
            Some((start, end)) if !range.is_empty() => {
                content_range = format!("Content-Range: bytes {start}-{}/{}\r\n", end - 1, body.len());
                ("206 Partial Content", end - start, &body[start..end])
            }
            Some(_) => ("200 OK", body.len(), body),
            None => ("416 Range Not Satisfiable", 0, &[][..]),
        }
    } else {
        ("400 Bad Request", 0, &[][..])
    };
    let keep_alive = !matches!(first_get, Some(FirstGet::Disconnect(_)))
        && !header("connection").eq_ignore_ascii_case("close");
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let response = format!("HTTP/1.1 {status}\r\nContent-Length: {length}\r\n{content_range}Connection: {connection}\r\n\r\n");
    let payload_length = match first_get {
        Some(FirstGet::Disconnect(prefix)) => prefix.min(payload.len()),
        _ => payload.len(),
    };
    stream.write_all(response.as_bytes()).await.is_ok()
        && stream.write_all(&payload[..payload_length]).await.is_ok()
        && keep_alive
}
