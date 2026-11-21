//! The daemon's JSON configuration.
//!
//! ```json
//! {
//!   "node": { "socket": "/run/xlb/xlb.sock", "socket_mode": "0666" },
//!   "classes": [{
//!     "name": "models",
//!     "cache_dir": "/var/lib/xlb/models",
//!     "cache_budget_bytes": 200000000000,
//!     "udp_bind": "0.0.0.0:4433",
//!     "origin": {
//!       "endpoint": "https://fly.storage.tigris.dev",
//!       "bucket": "artifacts",
//!       "region": "auto",
//!       "key_template": "models/blake3/{blake3}",
//!       "access_key_id_env": "AWS_ACCESS_KEY_ID",
//!       "secret_access_key_env": "AWS_SECRET_ACCESS_KEY"
//!     }
//!   }]
//! }
//! ```
//!
//! Unknown keys are ignored, so a config may carry `_comment` prose.

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use xlb::{
    origin::{
        DEFAULT_KEY_TEMPLATE, DEFAULT_ORIGIN_CONCURRENCY, DEFAULT_RANGE_BYTES,
        DEFAULT_RANGE_RETRIES,
    },
    peer::{
        DEFAULT_PEER_CONCURRENCY, DEFAULT_PEER_CONNECT_TIMEOUT, DEFAULT_PEER_PORT,
        DEFAULT_PEER_SEGMENT_BYTES, DEFAULT_PEER_STALL_TIMEOUT,
    },
    Credentials, OriginConfig, OriginSource, PeerConfig, RelaySetting,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node: NodeSettings,
    pub classes: Vec<ClassConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettings {
    /// Path of the Unix control socket.
    #[serde(default)]
    pub socket: Option<String>,
    /// Permission bits of the socket file, octal. The default lets any local
    /// user connect, because the client is a different container on the same
    /// node. A mode without group and other bits also restricts the socket
    /// to the daemon's own user ID.
    #[serde(default = "default_socket_mode")]
    pub socket_mode: String,
    /// Deployment cohort recorded on every fetch metric.
    #[serde(default = "default_peer_class")]
    pub peer_class: String,
    /// NDJSON file that receives one row per fetch. Absent = no file.
    #[serde(default)]
    pub metrics_path: Option<String>,
    /// Serve the local OCI pull endpoint from this class's S3 origin.
    #[serde(default)]
    pub image_mirror_class: Option<String>,
    /// Serve the containerd snapshot service over a node-local Unix socket.
    #[serde(default)]
    pub snapshotter: Option<xlb_mount::SnapshotSettings>,
    /// Maximum active OCI chunk downloads across pulls on this node.
    #[serde(default = "default_chunk_concurrency")]
    pub chunk_concurrency: NonZeroUsize,
    /// Maximum in-flight outgoing data and metadata requests across classes.
    #[serde(default = "default_total_request_concurrency")]
    pub total_request_concurrency: NonZeroUsize,
    #[serde(default = "default_s3_request_concurrency")]
    pub s3_request_concurrency: NonZeroUsize,
    #[serde(default = "default_peer_request_concurrency")]
    pub peer_request_concurrency: NonZeroUsize,
}

fn default_chunk_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(32).unwrap()
}

fn default_total_request_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(32).unwrap()
}

fn default_s3_request_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(32).unwrap()
}

fn default_peer_request_concurrency() -> NonZeroUsize {
    NonZeroUsize::new(16).unwrap()
}

fn default_socket_mode() -> String {
    "0666".into()
}
fn default_peer_class() -> String {
    "unknown".into()
}

impl NodeSettings {
    pub fn socket_mode(&self) -> anyhow::Result<u32> {
        let mode = u32::from_str_radix(self.socket_mode.trim_start_matches("0o"), 8)
            .with_context(|| format!("socket_mode {:?} is not octal", self.socket_mode))?;
        if mode > 0o777 {
            bail!("socket_mode {:?} has bits beyond 0777", self.socket_mode);
        }
        Ok(mode)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassConfig {
    pub name: String,
    /// Expose chunk-indexed models from this class at a read-only mount.
    #[serde(default)]
    pub model_mount: Option<PathBuf>,
    /// Directory of the plain files, one per complete blob.
    pub cache_dir: String,
    /// LRU budget for unpinned blobs.
    #[serde(default = "default_cache_budget")]
    pub cache_budget_bytes: u64,
    /// Whether the class serves and reads the peer set.
    #[serde(default = "yes")]
    pub peers: bool,
    /// UDP address of the peer endpoint.
    #[serde(default = "default_udp_bind")]
    pub udp_bind: String,
    #[serde(default = "default_peer_segment")]
    pub peer_segment_bytes: u64,
    #[serde(default = "default_peer_concurrency")]
    pub peer_concurrency: usize,
    #[serde(default = "default_peer_connect_timeout_ms")]
    pub peer_connect_timeout_ms: u64,
    /// How long a segment no peer holds may see no progress on any peer
    /// before this node reads it from the origin in place of its assigned
    /// reader.
    #[serde(default = "default_peer_stall_timeout_ms")]
    pub peer_stall_timeout_ms: u64,
    /// The relay servers of the peer endpoint: `"default"` for iroh's
    /// default relay map, `"disabled"` for none, or a list of relay URLs.
    /// Relays serve address discovery and hole punching only; blob requests
    /// are sent and served on direct paths.
    #[serde(default)]
    pub relay_mode: RelayModeSetting,
    #[serde(default)]
    pub origin: Option<OriginSettings>,
}

/// `relay_mode` as it is written in the configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RelayModeSetting {
    /// `"default"` or `"disabled"`.
    Named(String),
    /// Relay URLs.
    Urls(Vec<String>),
}

impl Default for RelayModeSetting {
    fn default() -> Self {
        RelayModeSetting::Named("default".into())
    }
}

impl RelayModeSetting {
    fn resolve(&self) -> anyhow::Result<RelaySetting> {
        match self {
            RelayModeSetting::Named(name) if name == "default" => Ok(RelaySetting::Default),
            RelayModeSetting::Named(name) if name == "disabled" => Ok(RelaySetting::Disabled),
            RelayModeSetting::Named(name) => {
                bail!("relay_mode {name:?} is not \"default\", \"disabled\" or a list of URLs")
            }
            RelayModeSetting::Urls(urls) => Ok(RelaySetting::Custom(urls.clone())),
        }
    }
}

fn default_cache_budget() -> u64 {
    5 * 1024 * 1024 * 1024
}
fn yes() -> bool {
    true
}
fn default_udp_bind() -> String {
    format!("0.0.0.0:{DEFAULT_PEER_PORT}")
}
fn default_peer_segment() -> u64 {
    DEFAULT_PEER_SEGMENT_BYTES
}
fn default_peer_concurrency() -> usize {
    DEFAULT_PEER_CONCURRENCY
}
fn default_peer_connect_timeout_ms() -> u64 {
    DEFAULT_PEER_CONNECT_TIMEOUT.as_millis() as u64
}
fn default_peer_stall_timeout_ms() -> u64 {
    DEFAULT_PEER_STALL_TIMEOUT.as_millis() as u64
}

impl ClassConfig {
    pub fn cache_dir(&self) -> PathBuf {
        PathBuf::from(expand_tilde(&self.cache_dir))
    }

    pub fn peer_config(&self) -> anyhow::Result<Option<PeerConfig>> {
        if !self.peers {
            return Ok(None);
        }
        let bind_addr: SocketAddr = self
            .udp_bind
            .parse()
            .with_context(|| format!("class {}: udp_bind {:?}", self.name, self.udp_bind))?;
        let relay = self
            .relay_mode
            .resolve()
            .with_context(|| format!("class {}", self.name))?;
        Ok(Some(PeerConfig {
            bind_addr,
            relay,
            segment_bytes: self.peer_segment_bytes,
            concurrency: self.peer_concurrency,
            connect_timeout: Duration::from_millis(self.peer_connect_timeout_ms),
            stall_timeout: Duration::from_millis(self.peer_stall_timeout_ms),
        }))
    }

    pub fn origin_config(&self) -> anyhow::Result<Option<OriginConfig>> {
        self.origin
            .as_ref()
            .map(|origin| {
                origin
                    .resolve(|name| std::env::var(name).ok())
                    .with_context(|| format!("class {}: origin", self.name))
            })
            .transpose()
    }
}

/// The origin of one class: either an S3-compatible bucket (`endpoint` and
/// `bucket`) read with signed requests, or an unauthenticated `url_template`.
#[derive(Clone, Serialize, Deserialize)]
pub struct OriginSettings {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    /// SigV4 region; Tigris uses `auto`.
    #[serde(default = "default_region")]
    pub region: String,
    /// Object key with a `{blake3}` token.
    #[serde(default = "default_key_template")]
    pub key_template: String,
    #[serde(default = "default_range_bytes")]
    pub range_bytes: u64,
    #[serde(default = "default_origin_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_range_retries")]
    pub range_retries: u32,
    #[serde(default, skip_serializing)]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing)]
    pub secret_access_key: Option<String>,
    /// Environment variable holding the access key ID, when it is not inline.
    #[serde(default)]
    pub access_key_id_env: Option<String>,
    /// Environment variable holding the secret, when it is not inline.
    #[serde(default)]
    pub secret_access_key_env: Option<String>,
    /// Full URL with a `{blake3}` token, read without credentials.
    #[serde(default)]
    pub url_template: Option<String>,
}

impl std::fmt::Debug for OriginSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginSettings")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("key_template", &self.key_template)
            .field("url_template", &self.url_template)
            .finish_non_exhaustive()
    }
}

fn default_region() -> String {
    "auto".into()
}
fn default_key_template() -> String {
    DEFAULT_KEY_TEMPLATE.into()
}
fn default_range_bytes() -> u64 {
    DEFAULT_RANGE_BYTES
}
fn default_origin_concurrency() -> usize {
    DEFAULT_ORIGIN_CONCURRENCY
}
fn default_range_retries() -> u32 {
    DEFAULT_RANGE_RETRIES
}

impl OriginSettings {
    /// Build the origin configuration, reading named credentials through
    /// `env`.
    pub fn resolve(&self, env: impl Fn(&str) -> Option<String>) -> anyhow::Result<OriginConfig> {
        let secret = |inline: &Option<String>,
                      variable: &Option<String>,
                      what: &str|
         -> anyhow::Result<String> {
            if let Some(value) = inline.as_ref().filter(|v| !v.is_empty()) {
                return Ok(value.clone());
            }
            let Some(variable) = variable else {
                bail!("no {what}: set {what} or {what}_env");
            };
            env(variable)
                .filter(|v| !v.is_empty())
                .with_context(|| format!("{what}_env names {variable}, which is not set"))
        };
        let source = match (&self.url_template, &self.endpoint, &self.bucket) {
            (Some(template), None, None) => OriginSource::Url {
                template: template.clone(),
            },
            (None, Some(endpoint), Some(bucket)) => OriginSource::S3 {
                endpoint: endpoint.clone(),
                bucket: bucket.clone(),
                region: self.region.clone(),
                key_template: self.key_template.clone(),
                credentials: Credentials {
                    access_key_id: secret(
                        &self.access_key_id,
                        &self.access_key_id_env,
                        "access_key_id",
                    )?,
                    secret_access_key: secret(
                        &self.secret_access_key,
                        &self.secret_access_key_env,
                        "secret_access_key",
                    )?,
                },
            },
            _ => bail!("give either url_template, or endpoint and bucket"),
        };
        Ok(OriginConfig {
            source,
            range_bytes: self.range_bytes,
            concurrency: self.concurrency,
            range_retries: self.range_retries,
        })
    }
}

impl NodeConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
        serde_json::from_str(&content).with_context(|| path.to_string())
    }
}

/// Expand a leading `~/` using `$HOME`. Returns the input unchanged otherwise.
pub fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}/{rest}", home.to_string_lossy());
        }
    }
    path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> NodeConfig {
        serde_json::from_str(json).expect("config parses")
    }

    const MINIMAL: &str = r#"{
      "_comment": "prose",
      "node": { "socket": "/tmp/s.sock" },
      "classes": [{ "name": "models", "cache_dir": "/var/lib/xlb/models" }]
    }"#;

    #[test]
    fn a_minimal_class_gets_the_documented_defaults() {
        let cfg = parse(MINIMAL);
        assert_eq!(cfg.node.socket.as_deref(), Some("/tmp/s.sock"));
        assert_eq!(cfg.node.socket_mode().unwrap(), 0o666);
        assert_eq!(cfg.node.peer_class, "unknown");
        assert!(cfg.node.metrics_path.is_none());
        assert_eq!(cfg.node.chunk_concurrency.get(), 32);

        let class = &cfg.classes[0];
        let peer = class.peer_config().unwrap().expect("peer tier on by default");
        assert_eq!(peer.bind_addr, "0.0.0.0:4433".parse().unwrap());
        assert_eq!(peer.relay, RelaySetting::Default);
        assert_eq!(peer.stall_timeout, Duration::from_secs(10));
        assert!(class.origin_config().unwrap().is_none());
    }

    #[test]
    fn relay_mode_is_a_name_or_a_list_of_urls() {
        let mut cfg = parse(MINIMAL);
        let relay = |cfg: &NodeConfig| cfg.classes[0].peer_config().map(|peer| peer.unwrap().relay);
        cfg.classes[0].relay_mode = serde_json::from_str(r#""disabled""#).unwrap();
        assert_eq!(relay(&cfg).unwrap(), RelaySetting::Disabled);
        cfg.classes[0].relay_mode = serde_json::from_str(r#"["https://relay.example"]"#).unwrap();
        assert_eq!(
            relay(&cfg).unwrap(),
            RelaySetting::Custom(vec!["https://relay.example".into()])
        );
        cfg.classes[0].relay_mode = serde_json::from_str(r#""sometimes""#).unwrap();
        assert!(relay(&cfg).is_err());
    }

    #[test]
    fn socket_mode_is_octal_and_bounded() {
        let mut cfg = parse(MINIMAL);
        cfg.node.socket_mode = "0600".into();
        assert_eq!(cfg.node.socket_mode().unwrap(), 0o600);
        for bad in ["rw", "888", "7777"] {
            cfg.node.socket_mode = bad.into();
            assert!(cfg.node.socket_mode().is_err(), "{bad} accepted");
        }
    }

    fn origin(json: &str) -> OriginSettings {
        serde_json::from_str(json).expect("origin parses")
    }

    #[test]
    fn an_s3_origin_reads_credentials_from_the_named_variables() {
        let settings = origin(
            r#"{
              "endpoint": "https://fly.storage.tigris.dev",
              "bucket": "artifacts",
              "access_key_id_env": "KEY_ID",
              "secret_access_key_env": "KEY_SECRET"
            }"#,
        );
        let env = |name: &str| match name {
            "KEY_ID" => Some("id".to_string()),
            "KEY_SECRET" => Some("secret".to_string()),
            _ => None,
        };
        let config = settings.resolve(env).unwrap();
        assert_eq!(config.range_bytes, 8 * 1024 * 1024);
        assert_eq!(config.concurrency, 32);
        match config.source {
            OriginSource::S3 {
                region,
                key_template,
                credentials,
                ..
            } => {
                assert_eq!(region, "auto");
                assert_eq!(key_template, "models/blake3/{blake3}");
                assert_eq!(credentials.access_key_id, "id");
                assert_eq!(credentials.secret_access_key, "secret");
            }
            other => panic!("expected an S3 origin, got {other:?}"),
        }
        assert!(settings.resolve(|_| None).is_err(), "unset variable");
    }

    #[test]
    fn inline_credentials_win_and_are_not_written_back_out() {
        let settings = origin(
            r#"{
              "endpoint": "http://127.0.0.1:9000",
              "bucket": "b",
              "access_key_id": "inline-id",
              "secret_access_key": "inline-secret"
            }"#,
        );
        assert!(settings.resolve(|_| None).is_ok());
        let json = serde_json::to_string(&settings).unwrap();
        assert!(!json.contains("inline-secret"), "{json}");
        assert!(!format!("{settings:?}").contains("inline-secret"));
    }

    #[test]
    fn a_url_template_origin_needs_no_credentials() {
        let settings = origin(r#"{ "url_template": "https://cdn.example/{blake3}" }"#);
        assert!(matches!(
            settings.resolve(|_| None).unwrap().source,
            OriginSource::Url { .. }
        ));
        assert!(origin("{}").resolve(|_| None).is_err());
    }
}
