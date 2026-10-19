use js_sys::{AsyncIterator, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream, Navigator, StorageManager,
};
use xlb_core::{BlakeHash, BlobStore, RetainingCache, RetentionPolicy, StoreError};

/// Top-level OPFS directory holding every class's subdirectory.
const ROOT: &str = "xlb";

fn js_err(what: &str, e: JsValue) -> StoreError {
    StoreError::Backend(format!(
        "{what}: {}",
        js_sys::JSON::stringify(&e)
            .ok()
            .and_then(|s| s.as_string())
            .or_else(|| e.as_string())
            .unwrap_or_else(|| "<js error>".into())
    ))
}

async fn await_as<T: JsCast>(what: &str, p: js_sys::Promise) -> Result<T, StoreError> {
    JsFuture::from(p)
        .await
        .map_err(|e| js_err(what, e))?
        .dyn_into::<T>()
        .map_err(|_| StoreError::Backend(format!("{what}: unexpected JS type")))
}

/// `navigator.storage`, from a window or a worker global.
fn storage_manager() -> Result<StorageManager, StoreError> {
    let nav = Reflect::get(&js_sys::global(), &JsValue::from_str("navigator"))
        .map_err(|e| js_err("navigator", e))?
        .dyn_into::<Navigator>()
        .map_err(|_| StoreError::Backend("no navigator in this global".into()))?;
    Ok(nav.storage())
}

fn is_not_found(e: &JsValue) -> bool {
    Reflect::get(e, &JsValue::from_str("name"))
        .ok()
        .and_then(|n| n.as_string())
        .is_some_and(|n| n == "NotFoundError")
}

/// A [`BlobStore`] over one OPFS directory (`xlb/<class>/`).
#[derive(Debug, Clone)]
pub struct OpfsBlobStore {
    dir: FileSystemDirectoryHandle,
}

impl OpfsBlobStore {
    /// Open (creating if needed) the directory for `class`.
    pub async fn open(class: &str) -> Result<Self, StoreError> {
        if class.is_empty() || class.contains(['/', '\\']) || class == "." || class == ".." {
            return Err(StoreError::Backend(format!("invalid class name {class:?}")));
        }
        let root: FileSystemDirectoryHandle =
            await_as("getDirectory", storage_manager()?.get_directory()).await?;
        let opts = FileSystemGetDirectoryOptions::new();
        opts.set_create(true);
        let xlb: FileSystemDirectoryHandle = await_as(
            "open xlb dir",
            root.get_directory_handle_with_options(ROOT, &opts),
        )
        .await?;
        let dir: FileSystemDirectoryHandle = await_as(
            "open class dir",
            xlb.get_directory_handle_with_options(class, &opts),
        )
        .await?;
        Ok(Self { dir })
    }

    async fn file_handle(
        &self,
        hash: &BlakeHash,
        create: bool,
    ) -> Result<Option<FileSystemFileHandle>, StoreError> {
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(create);
        match JsFuture::from(self.dir.get_file_handle_with_options(&hash.to_hex(), &opts)).await {
            Ok(v) => v
                .dyn_into()
                .map(Some)
                .map_err(|_| StoreError::Backend("file handle: unexpected JS type".into())),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(js_err("getFileHandle", e)),
        }
    }
}

impl BlobStore for OpfsBlobStore {
    async fn get(&self, hash: &BlakeHash) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(fh) = self.file_handle(hash, false).await? else {
            return Ok(None);
        };
        let file: File = await_as("getFile", fh.get_file()).await?;
        let buf = JsFuture::from(file.array_buffer())
            .await
            .map_err(|e| js_err("arrayBuffer", e))?;
        Ok(Some(js_sys::Uint8Array::new(&buf).to_vec()))
    }

    async fn put(&self, hash: &BlakeHash, data: &[u8]) -> Result<(), StoreError> {
        let fh = self
            .file_handle(hash, true)
            .await?
            .ok_or_else(|| StoreError::Backend("getFileHandle(create) returned NotFound".into()))?;
        let w: FileSystemWritableFileStream =
            await_as("createWritable", fh.create_writable()).await?;
        // createWritable stages into a swap file; contents only replace the
        // blob on close(), so a crash mid-write leaves no torn blob.
        let write = w
            .write_with_u8_array(data)
            .map_err(|e| js_err("write", e))?;
        if let Err(e) = JsFuture::from(write).await {
            let _ = JsFuture::from(w.abort()).await;
            return Err(js_err("write", e));
        }
        JsFuture::from(w.close())
            .await
            .map_err(|e| js_err("close", e))?;
        Ok(())
    }

    async fn delete(&self, hash: &BlakeHash) -> Result<(), StoreError> {
        match JsFuture::from(self.dir.remove_entry(&hash.to_hex())).await {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(js_err("removeEntry", e)),
        }
    }

    async fn list(&self) -> Result<Vec<(BlakeHash, u64)>, StoreError> {
        let iter: AsyncIterator = self.dir.values();
        let mut out = Vec::new();
        loop {
            let step = JsFuture::from(iter.next().map_err(|e| js_err("values.next", e))?)
                .await
                .map_err(|e| js_err("values.next", e))?;
            let done = Reflect::get(&step, &JsValue::from_str("done"))
                .map(|d| d.is_truthy())
                .unwrap_or(true);
            if done {
                break;
            }
            let value = Reflect::get(&step, &JsValue::from_str("value"))
                .map_err(|e| js_err("values.value", e))?;
            // Directories and stray files (non-hash names, e.g. editor
            // swap files) are skipped; only well-formed hash names count.
            let Ok(fh) = value.dyn_into::<FileSystemFileHandle>() else {
                continue;
            };
            let Ok(hash) = BlakeHash::from_hex(&fh.name()) else {
                continue;
            };
            let file: File = await_as("getFile", fh.get_file()).await?;
            out.push((hash, file.size() as u64));
        }
        Ok(out)
    }
}

/// Open `class`'s OPFS directory and wrap it in a [`RetainingCache`]. Pins
/// are in-memory: re-declare them from the manifest after opening.
pub async fn open_cache(
    class: &str,
    policy: RetentionPolicy,
) -> Result<RetainingCache<OpfsBlobStore>, StoreError> {
    RetainingCache::open(OpfsBlobStore::open(class).await?, policy).await
}
