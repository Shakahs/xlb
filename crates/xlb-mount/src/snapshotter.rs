use std::{
    collections::HashMap,
    fs,
    io::ErrorKind,
    os::unix::fs::{MetadataExt, PermissionsExt, chown},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{Context, Result, ensure};
use containerd_snapshots::{Info, Kind, Snapshotter, Usage, api::types::Mount, server};
use fuser::BackgroundSession;
use futures::{stream, stream::Iter};
use serde::{Deserialize, Serialize};
use tokio::{net::UnixListener, runtime::Handle, sync::Mutex};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Status, transport::Server};
use xlb::AssetClass;
use xlb_core::{BlakeHash, chunks::CAS_INDEX_LIMIT, image::blake3_digest_hex, layer::{LAYER_INDEX_LABEL, LAYER_INDEX_PREFIX, LayerIndex}};

use crate::{layer_mount::mount_layer, snapshot_store::{SnapshotRecord, SnapshotRecords, SnapshotStore, disk_usage, write_index}};

const SNAPSHOT_REF: &str = "containerd.io/snapshot.ref";
const SNAPSHOT_DIFF_ID: &str = "containerd.io/snapshot/diff-id";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SnapshotSettings {
    pub socket: PathBuf,
    pub root: PathBuf,
    pub class: String,
}

struct SnapshotState {
    records: SnapshotRecords,
    sessions: HashMap<String, BackgroundSession>,
}

struct CasSnapshotter {
    store: SnapshotStore,
    state: Mutex<SnapshotState>,
    class: AssetClass,
}

// [impl -> dsn~xlb-fuse-snapshots~1]
pub async fn serve_snapshots(config: SnapshotSettings, class: AssetClass) -> Result<()> {
    let (store, records) = SnapshotStore::open(config.root)?;
    let mut sessions = HashMap::new();
    for record in records.values() {
        if let Some(digest) = record.layer {
            let bytes = tokio::fs::read(store.index(digest)).await.context("restore OCI layer index")?;
            let diff_id = record.info.labels.get(SNAPSHOT_DIFF_ID).context("snapshot has no diff ID")?;
            let index = verify_index(&bytes, digest, diff_id)?;
            let session = start_layer(index, &store.upper(record), class.clone()).await?;
            sessions.insert(record.id.clone(), session);
        }
    }
    let snapshots = Arc::new(CasSnapshotter { store, state: Mutex::new(SnapshotState { records, sessions }), class });
    let directory = config.socket.parent().context("snapshot socket needs a parent directory")?;
    tokio::fs::create_dir_all(directory).await?;
    match tokio::fs::remove_file(&config.socket).await {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let socket = UnixListener::bind(&config.socket)?;
    tokio::fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o600)).await?;
    Server::builder().add_service(server(snapshots))
        .serve_with_incoming(UnixListenerStream::new(socket)).await?;
    Ok(())
}

fn verify_index(bytes: &[u8], digest: BlakeHash, diff_id: &str) -> Result<LayerIndex> {
    ensure!(BlakeHash::hash(bytes) == digest, "OCI filesystem index differs from its BLAKE3 digest");
    let index: LayerIndex = serde_json::from_slice(bytes).context("parse OCI layer filesystem index")?;
    ensure!(blake3_digest_hex(diff_id).is_some_and(|hex| index.diff_id.to_hex() == hex),
        "OCI filesystem index differs from containerd's BLAKE3 diff ID");
    Ok(index)
}

async fn start_layer(index: LayerIndex, path: &Path, class: AssetClass) -> Result<BackgroundSession> {
    let path = path.to_owned();
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        clear_disconnected_mount(&path)?;
        mount_layer(index, class, &path, runtime)
    }).await?
}

pub(crate) fn clear_disconnected_mount(path: &Path) -> Result<()> {
    match fs::metadata(path) {
        Ok(_) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => {
            let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
            // SAFETY: the path is NUL-terminated and lives through the syscall.
            if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn failure(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn lookup<'a>(records: &'a SnapshotRecords, key: &str) -> Result<&'a SnapshotRecord, Status> {
    records.get(key).ok_or_else(|| Status::not_found(format!("snapshot {key} is missing")))
}

impl CasSnapshotter {
    fn mounts_for(&self, record: &SnapshotRecord, records: &SnapshotRecords) -> Result<Vec<Mount>, Status> {
        let mut lower = Vec::new();
        let mut parent = &record.info.parent;
        if record.info.kind == Kind::Committed {
            lower.push(self.store.upper(record));
        }
        while !parent.is_empty() {
            let ancestor = lookup(records, parent)?;
            lower.push(self.store.upper(ancestor));
            parent = &ancestor.info.parent;
        }
        let is_active = record.info.kind == Kind::Active;
        let (source, mount_type, options) = if lower.is_empty() || (!is_active && lower.len() == 1) {
            let path = lower.first().cloned().unwrap_or_else(|| self.store.upper(record));
            (path.display().to_string(), "bind", vec!["rbind".into(), if is_active { "rw" } else { "ro" }.into()])
        } else {
            let lower = lower.iter().map(|path| path.display().to_string()).collect::<Vec<_>>();
            let mut options = vec![format!("lowerdir={}", lower.join(":")), "index=off".into()];
            if is_active {
                options.push(format!("upperdir={}", self.store.upper(record).display()));
                options.push(format!("workdir={}", self.store.work(record).display()));
            } else {
                options.push("ro".into());
            }
            ("overlay".into(), "overlay", options)
        };
        Ok(vec![Mount { source, r#type: mount_type.into(), options, ..Mount::default() }])
    }

    async fn layer_index(&self, digest: BlakeHash, diff_id: &str) -> Result<LayerIndex, Status> {
        let path = self.store.index(digest);
        match tokio::fs::read(&path).await {
            Ok(bytes) => verify_index(&bytes, digest, diff_id).map_err(failure),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let bytes = self.class.read_origin_object(&format!("{LAYER_INDEX_PREFIX}{digest}"), CAS_INDEX_LIMIT).await.map_err(failure)?
                    .ok_or_else(|| Status::not_found(format!("OCI filesystem index {digest} is missing")))?;
                let index = verify_index(&bytes, digest, diff_id).map_err(failure)?;
                tokio::task::spawn_blocking(move || write_index(&path, &bytes)).await.map_err(failure)?.map_err(failure)?;
                Ok(index)
            }
            Err(error) => Err(failure(error)),
        }
    }

    async fn create(&self, key: String, parent: String, labels: HashMap<String, String>, kind: Kind) -> Result<Vec<Mount>, Status> {
        let target = labels.get(SNAPSHOT_REF);
        let digest = labels.get(LAYER_INDEX_LABEL);
        let diff_id = labels.get(SNAPSHOT_DIFF_ID);
        if kind == Kind::Active && diff_id.is_some_and(|id| id.starts_with("blake3:")) && digest.is_none() {
            return Err(Status::failed_precondition("BLAKE3 image layer has no XLB filesystem index"));
        }
        let remote = match (kind, target, digest, diff_id) {
            (Kind::Active, Some(target), Some(digest), Some(diff_id)) if blake3_digest_hex(target).is_some() => {
                let digest = blake3_digest_hex(digest).and_then(|hex| BlakeHash::from_hex(hex).ok())
                    .ok_or_else(|| Status::invalid_argument("XLB filesystem index requires a BLAKE3 digest"))?;
                Some((digest, diff_id.clone()))
            }
            (_, _, Some(_), _) => return Err(Status::invalid_argument("XLB layers require containerd BLAKE3 snapshot and diff IDs")),
            _ => None,
        };

        let index = match &remote {
            Some((digest, diff_id)) => Some(self.layer_index(*digest, diff_id).await?),
            None => None,
        };
        let mut state = self.state.lock().await;
        if !parent.is_empty() && lookup(&state.records, &parent)?.info.kind != Kind::Committed {
            return Err(Status::failed_precondition("snapshot parent is not committed"));
        }
        if state.records.contains_key(&key) || remote.is_some() && state.records.values().any(|record| {
            record.info.kind == Kind::Committed && record.info.parent == parent && record.info.labels.get(SNAPSHOT_REF) == target
        }) {
            return Err(Status::already_exists(format!("snapshot {key} already exists")));
        }

        let record = SnapshotRecord {
            info: Info { kind: if remote.is_some() { Kind::Committed } else { kind }, name: key.clone(), parent, labels, ..Info::default() },
            id: format!("{:032x}", rand::random::<u128>()),
            layer: remote.as_ref().map(|(digest, _)| *digest),
        };
        let upper = self.store.upper(&record);
        fs::create_dir_all(&upper).map_err(failure)?;
        let prepare = async {
            if let Some(index) = index {
                let session = start_layer(index, &upper, self.class.clone()).await.map_err(failure)?;
                Ok(Some(session))
            } else {
                if kind == Kind::Active {
                    if !record.info.parent.is_empty() {
                        let parent = lookup(&state.records, &record.info.parent)?;
                        let attrs = tokio::fs::metadata(self.store.upper(parent)).await.map_err(failure)?;
                        chown(&upper, Some(attrs.uid()), Some(attrs.gid())).map_err(failure)?;
                        fs::set_permissions(&upper, fs::Permissions::from_mode(attrs.mode() & 0o7777)).map_err(failure)?;
                    }
                    fs::create_dir(self.store.work(&record)).map_err(failure)?;
                }
                Ok(None)
            }
        }.await;
        let mut next = state.records.clone();
        next.insert(key, record.clone());
        let result = prepare.and_then(|session| {
            let mounts = self.mounts_for(&record, &next)?;
            self.store.persist(&next).map_err(failure)?;
            state.records = next;
            if let Some(session) = session { state.sessions.insert(record.id.clone(), session); }
            Ok(mounts)
        });
        if result.is_err() {
            let _ = fs::remove_dir_all(self.store.directory(&record));
        } else if remote.is_some() {
            return Err(Status::already_exists("OCI FUSE layer is committed"));
        }
        result
    }
}

#[tonic::async_trait]
impl Snapshotter for CasSnapshotter {
    type Error = Status;
    type InfoStream = Iter<std::vec::IntoIter<Result<Info, Status>>>;

    async fn stat(&self, key: String) -> Result<Info, Status> {
        Ok(lookup(&self.state.lock().await.records, &key)?.info.clone())
    }

    async fn update(&self, info: Info, fields: Option<Vec<String>>) -> Result<Info, Status> {
        let mut state = self.state.lock().await;
        let mut next = state.records.clone();
        let record = next.get_mut(&info.name).ok_or_else(|| Status::not_found("snapshot is missing"))?;
        for field in fields.filter(|fields| !fields.is_empty()).unwrap_or_else(|| vec!["labels".into()]) {
            if field == "labels" {
                record.info.labels.clone_from(&info.labels);
            } else if let Some(label) = field.strip_prefix("labels.") {
                if let Some(value) = info.labels.get(label) {
                    record.info.labels.insert(label.to_string(), value.clone());
                } else {
                    record.info.labels.remove(label);
                }
            } else {
                return Err(Status::invalid_argument(format!("snapshot field {field} is immutable")));
            }
        }
        if record.layer.is_some() {
            for key in [LAYER_INDEX_LABEL, SNAPSHOT_REF, SNAPSHOT_DIFF_ID] {
                let previous = &state.records[&info.name].info.labels;
                if record.info.labels.get(key) != previous.get(key) {
                    return Err(Status::invalid_argument(format!("snapshot label {key} is immutable")));
                }
            }
        }
        record.info.updated_at = SystemTime::now();
        let result = record.info.clone();
        self.store.persist(&next).map_err(failure)?;
        state.records = next;
        Ok(result)
    }

    async fn usage(&self, key: String) -> Result<Usage, Status> {
        let upper = {
            let state = self.state.lock().await;
            let record = lookup(&state.records, &key)?;
            if record.layer.is_some() { return Ok(Usage::default()); }
            self.store.upper(record)
        };
        tokio::task::spawn_blocking(move || disk_usage(&upper)).await.map_err(failure)?.map_err(failure)
    }

    async fn mounts(&self, key: String) -> Result<Vec<Mount>, Status> {
        let state = self.state.lock().await;
        self.mounts_for(lookup(&state.records, &key)?, &state.records)
    }

    async fn prepare(&self, key: String, parent: String, labels: HashMap<String, String>) -> Result<Vec<Mount>, Status> {
        self.create(key, parent, labels, Kind::Active).await
    }

    async fn view(&self, key: String, parent: String, labels: HashMap<String, String>) -> Result<Vec<Mount>, Status> {
        self.create(key, parent, labels, Kind::View).await
    }

    async fn commit(&self, name: String, key: String, labels: HashMap<String, String>) -> Result<(), Status> {
        let mut state = self.state.lock().await;
        if state.records.contains_key(&name) { return Err(Status::already_exists("snapshot is already committed")); }
        let mut record = lookup(&state.records, &key)?.clone();
        if record.info.kind != Kind::Active { return Err(Status::failed_precondition("only active snapshots can be committed")); }
        record.info.name.clone_from(&name);
        record.info.kind = Kind::Committed;
        record.info.labels.extend(labels);
        record.info.updated_at = SystemTime::now();
        let mut next = state.records.clone();
        next.remove(&key);
        next.insert(name, record);
        self.store.persist(&next).map_err(failure)?;
        state.records = next;
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<(), Status> {
        let mut state = self.state.lock().await;
        let record = lookup(&state.records, &key)?.clone();
        if state.records.values().any(|child| child.info.parent == key) {
            return Err(Status::failed_precondition("snapshot still has children"));
        }
        let mut next = state.records.clone();
        next.remove(&key);
        self.store.persist(&next).map_err(failure)?;
        state.records = next;
        drop(state.sessions.remove(&record.id));
        if let Err(error) = fs::remove_dir_all(self.store.directory(&record)) {
            tracing::warn!(%key, %error, "defer cleanup of removed XLB snapshot");
        }
        Ok(())
    }

    async fn clear(&self) -> Result<(), Status> {
        let state = self.state.lock().await;
        for entry in self.store.entries().map_err(failure)? {
            let entry = entry.map_err(failure)?;
            if !state.records.values().any(|record| entry.file_name() == std::ffi::OsStr::new(&record.id)) {
                match clear_disconnected_mount(&entry.path().join("fs")) {
                    Ok(()) => {}
                    Err(error) if error.downcast_ref::<std::io::Error>().is_some_and(|err| err.kind() == ErrorKind::NotFound) => {}
                    Err(error) => return Err(failure(error)),
                }
                fs::remove_dir_all(entry.path()).map_err(failure)?;
            }
        }
        Ok(())
    }

    async fn list(&self, _snapshotter: String, filters: Vec<String>) -> Result<Self::InfoStream, Status> {
        let state = self.state.lock().await;
        let records = state.records.values().filter(|record| matches_filters(&record.info, &filters))
            .map(|record| Ok(record.info.clone())).collect::<Vec<_>>();
        Ok(stream::iter(records))
    }
}

fn matches_filters(info: &Info, filters: &[String]) -> bool {
    filters.is_empty() || filters.iter().any(|filter| filter.split(',').all(|selector| {
        let Some((field, expected)) = selector.split_once("==") else { return true; };
        let actual = match field {
            "name" => Some(info.name.as_str()),
            "parent" => Some(info.parent.as_str()),
            "kind" => Some(match info.kind { Kind::Active => "active", Kind::View => "view", Kind::Committed => "committed", Kind::Unknown => "unknown" }),
            field => field.strip_prefix("labels.").and_then(|label| info.labels.get(label.trim_matches('"'))).map(String::as_str),
        };
        actual == Some(expected.trim_matches('"'))
    }))
}
