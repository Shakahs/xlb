//! The swarm tests' S3 origin: a real Garage server, the release pinned in
//! MODULE.bazel, behind a fault proxy. The proxy forwards every request to
//! Garage, which verifies the SigV4 signatures and serves the objects. It
//! counts GETs and the first byte each asks for, holds GETs while paused, and
//! forces the first GET's throttle or mid-body disconnect.

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    process::{Child, Command},
    sync::watch,
    task::JoinSet,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{BlakeHash, Credentials, OriginConfig};
use bytes::Bytes;

const BUCKET: &str = "artifacts";
const REGION: &str = "auto";
const ACCESS_KEY_ID: &str = "test-key-id";
const SECRET_ACCESS_KEY: &str = "test-secret-access-key";
const RPC_SECRET: &str = "786c6220737761726d2074657374206761726167652072706320736563726574";
const READY_DEADLINE: Duration = Duration::from_secs(60);

fn credentials() -> Credentials {
    Credentials {
        access_key_id: ACCESS_KEY_ID.into(),
        secret_access_key: SECRET_ACCESS_KEY.into(),
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
    garage: PathBuf,
    first_get: Option<FirstGet>,
    paused: watch::Sender<bool>,
    gets: AtomicUsize,
    get_offsets: std::sync::Mutex<Vec<usize>>,
    connections: AtomicUsize,
}

pub struct OriginServer {
    served: Arc<Served>,
    host: String,
    _proxy: AbortOnDropHandle<()>,
    _garage: Child,
    _directory: tempfile::TempDir,
}

/// Starts a single-node Garage whose S3 API listens on a Unix socket in
/// `directory`, with the test bucket and key.
fn start_garage(directory: &Path) -> (Child, PathBuf) {
    let socket = directory.join("s3.sock");
    let configuration = directory.join("garage.toml");
    std::fs::write(&configuration, format!(
        "metadata_dir = \"{meta}\"\ndata_dir = \"{data}\"\ndb_engine = \"sqlite\"\nreplication_factor = 1\n\
         rpc_bind_addr = \"127.0.0.1:0\"\nrpc_secret = \"{RPC_SECRET}\"\n\n\
         [s3_api]\ns3_region = \"{REGION}\"\napi_bind_addr = \"{socket}\"\n",
        meta = directory.join("meta").display(), data = directory.join("data").display(), socket = socket.display(),
    )).unwrap();
    let root = std::env::var_os("TEST_SRCDIR").expect("Bazel runfiles");
    let binary = PathBuf::from(root).join(std::env::var("GARAGE").expect("GARAGE names the Garage runfile"));
    let log = std::fs::File::create(directory.join("garage.log")).unwrap();
    let garage = Command::new(binary).arg("-c").arg(&configuration)
        .args(["server", "--single-node", "--default-access-key", "--default-bucket"])
        .env("GARAGE_DEFAULT_ACCESS_KEY", ACCESS_KEY_ID)
        .env("GARAGE_DEFAULT_SECRET_KEY", SECRET_ACCESS_KEY)
        .env("GARAGE_DEFAULT_BUCKET", BUCKET)
        .stdin(Stdio::null()).stdout(log.try_clone().unwrap()).stderr(log)
        .kill_on_drop(true).spawn().expect("start Garage");
    (garage, socket)
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

    #[expect(clippy::disallowed_methods, reason = "a fault proxy in front of the real Garage S3 server: it holds, throttles and disconnects GETs and forwards every request to Garage")]
    async fn start_many_with(bodies: Vec<(BlakeHash, Bytes)>, first_get: Option<FirstGet>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let (garage, socket) = start_garage(directory.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = listener.local_addr().unwrap().to_string();
        let served = Arc::new(Served {
            garage: socket,
            first_get,
            paused: watch::Sender::new(false),
            gets: AtomicUsize::new(0),
            get_offsets: std::sync::Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
        });
        let accepting = served.clone();
        let proxy = tokio::spawn(async move {
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
        let mut origin = Self { served, host, _proxy: AbortOnDropHandle::new(proxy), _garage: garage, _directory: directory };
        origin.store(bodies).await;
        origin
    }

    /// Stores each body under its CAS key once Garage serves the bucket, then
    /// clears the counters the uploads touched.
    async fn store(&mut self, bodies: Vec<(BlakeHash, Bytes)>) {
        opendal::install_default();
        let operator = opendal::Operator::new(opendal::services::S3::default()
            .endpoint(&format!("http://{}", self.host)).bucket(BUCKET).region(REGION)
            .access_key_id(ACCESS_KEY_ID).secret_access_key(SECRET_ACCESS_KEY).disable_config_load()).unwrap();
        let deadline = tokio::time::Instant::now() + READY_DEADLINE;
        for (hash, body) in bodies {
            let key = format!("models/blake3/{}", hash.to_hex());
            loop {
                match operator.write(&key, body.clone()).await {
                    Ok(_) => break,
                    Err(error) => {
                        let status = self._garage.try_wait().expect("check Garage's exit status");
                        if status.is_some() || tokio::time::Instant::now() >= deadline {
                            let log = std::fs::read_to_string(self._directory.path().join("garage.log")).unwrap_or_default();
                            panic!("Garage did not store {key} (exit: {status:?}): {error:?}\n{log}");
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        self.served.gets.store(0, Ordering::Relaxed);
        self.served.get_offsets.lock().unwrap().clear();
        self.served.connections.store(0, Ordering::Relaxed);
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
            endpoint: format!("http://{}", self.host),
            bucket: BUCKET.into(),
            region: REGION.into(),
            key_template: "models/blake3/{blake3}".into(),
            credentials: credentials(),
            concurrency: NonZeroUsize::new(concurrency).unwrap(),
            retries: 1,
        }
    }
}

/// The start of a single `bytes=start-[last]` range.
fn range_start(value: &str) -> Option<usize> {
    value.strip_prefix("bytes=")?.split_once('-')?.0.parse().ok()
}

/// Reads one HTTP/1.1 message head, keeping any bytes read past it.
async fn read_head<S: AsyncReadExt + Unpin>(stream: &mut S, buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let rest = buffer.split_off(end + 4);
            return Some(std::mem::replace(buffer, rest));
        }
        let mut bytes = [0; 8192];
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return None,
            Ok(length) => buffer.extend_from_slice(&bytes[..length]),
        }
    }
}

/// Reads `length` bytes, starting with any already buffered.
async fn read_body<S: AsyncReadExt + Unpin>(stream: &mut S, buffer: &mut Vec<u8>, length: usize) -> Option<Vec<u8>> {
    while buffer.len() < length {
        let mut bytes = [0; 65536];
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => buffer.extend_from_slice(&bytes[..read]),
        }
    }
    let rest = buffer.split_off(length);
    Some(std::mem::replace(buffer, rest))
}

fn header<'a>(head: &'a str, name: &str) -> &'a str {
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    }).unwrap_or_default()
}

async fn serve_connection(mut client: TcpStream, served: Arc<Served>) {
    let Ok(mut garage) = UnixStream::connect(&served.garage).await else { return };
    let (mut from_client, mut from_garage) = (Vec::new(), Vec::new());
    while forward_request(&mut client, &mut garage, &mut from_client, &mut from_garage, &served).await {}
}

/// Forwards one request to Garage and its response to the client, applying
/// the configured faults to GETs. Returns whether the connection stays open.
async fn forward_request(
    client: &mut TcpStream, garage: &mut UnixStream, from_client: &mut Vec<u8>, from_garage: &mut Vec<u8>, served: &Served,
) -> bool {
    let Some(request) = read_head(client, from_client).await else { return false };
    let head = String::from_utf8_lossy(&request).into_owned();
    let method = head.split(' ').next().unwrap_or_default().to_owned();
    let length = header(&head, "content-length").parse().unwrap_or(0);
    let Some(body) = read_body(client, from_client, length).await else { return false };
    let mut first_get = None;
    if method == "GET" {
        if served.gets.fetch_add(1, Ordering::Relaxed) == 0 {
            first_get = served.first_get;
        }
        served.get_offsets.lock().unwrap().push(range_start(header(&head, "range")).unwrap_or(0));
        let mut paused = served.paused.subscribe();
        let _ = paused.wait_for(|paused| !paused).await;
        if matches!(first_get, Some(FirstGet::Throttle)) {
            return client.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n").await.is_ok();
        }
    }
    if garage.write_all(&request).await.is_err() || garage.write_all(&body).await.is_err() {
        return false;
    }
    let Some(response) = read_head(garage, from_garage).await else { return false };
    let response_head = String::from_utf8_lossy(&response).into_owned();
    let status = response_head.split(' ').nth(1).unwrap_or_default();
    let length = if method == "HEAD" || matches!(status, "204" | "304") {
        0
    } else {
        header(&response_head, "content-length").parse().unwrap_or(0)
    };
    let Some(payload) = read_body(garage, from_garage, length).await else { return false };
    let (payload, keep_alive) = match first_get {
        Some(FirstGet::Disconnect(prefix)) => (&payload[..prefix.min(payload.len())], false),
        _ => (&payload[..], !header(&head, "connection").eq_ignore_ascii_case("close")),
    };
    client.write_all(&response).await.is_ok() && client.write_all(payload).await.is_ok() && keep_alive
}
