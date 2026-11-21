use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use containerd_snapshots::{Info, Usage};
use serde::{Deserialize, Serialize};
use xlb_core::BlakeHash;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct SnapshotRecord {
    pub(crate) info: Info,
    pub(crate) id: String,
    pub(crate) layer: Option<BlakeHash>,
}

pub(crate) type SnapshotRecords = BTreeMap<String, SnapshotRecord>;

pub(crate) struct SnapshotStore {
    root: PathBuf,
}

impl SnapshotStore {
    pub(crate) fn open(root: PathBuf) -> anyhow::Result<(Self, SnapshotRecords)> {
        anyhow::ensure!(root.is_absolute() && !root.as_os_str().as_encoded_bytes().iter().any(|byte| matches!(byte, b',' | b':' | b'\\')),
            "snapshot root must be absolute and usable as an overlay mount option");
        fs::create_dir_all(root.join("snapshots"))?;
        fs::create_dir_all(root.join("indexes"))?;
        let records = match File::open(root.join("snapshots.json")) {
            Ok(file) => serde_json::from_reader(file)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        Ok((Self { root }, records))
    }

    pub(crate) fn persist(&self, records: &SnapshotRecords) -> anyhow::Result<()> {
        let staged = self.root.join("snapshots.tmp");
        let file = File::create(&staged)?;
        serde_json::to_writer(&file, records)?;
        file.sync_all()?;
        fs::rename(staged, self.root.join("snapshots.json"))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub(crate) fn index(&self, digest: BlakeHash) -> PathBuf {
        self.root.join("indexes").join(digest.to_hex())
    }

    pub(crate) fn upper(&self, record: &SnapshotRecord) -> PathBuf {
        self.directory(record).join("fs")
    }

    pub(crate) fn work(&self, record: &SnapshotRecord) -> PathBuf {
        self.directory(record).join("work")
    }

    pub(crate) fn directory(&self, record: &SnapshotRecord) -> PathBuf {
        self.root.join("snapshots").join(&record.id)
    }

    pub(crate) fn entries(&self) -> std::io::Result<fs::ReadDir> {
        fs::read_dir(self.root.join("snapshots"))
    }
}

pub(crate) fn write_index(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let staged = path.with_extension(format!("{:032x}.tmp", rand::random::<u128>()));
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&staged, path)?;
        File::open(path.parent().expect("index has a directory"))?.sync_all()
    })();
    if result.is_err() { let _ = fs::remove_file(staged); }
    result
}

pub(crate) fn disk_usage(root: &Path) -> std::io::Result<Usage> {
    let mut total = Usage::default();
    let mut seen = HashSet::new();
    let mut remaining = vec![root.to_path_buf()];
    while let Some(path) = remaining.pop() {
        let metadata = fs::symlink_metadata(&path)?;
        if seen.insert((metadata.dev(), metadata.ino())) {
            total.inodes += 1;
            total.size += metadata.blocks() as i64 * 512;
            if metadata.is_dir() {
                for entry in fs::read_dir(path)? {
                    remaining.push(entry?.path());
                }
            }
        }
    }
    Ok(total)
}
