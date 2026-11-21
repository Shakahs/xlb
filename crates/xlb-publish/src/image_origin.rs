//! Stream the declared rules_img outputs into the XLB origin.
use std::{
    collections::BTreeMap,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, ensure};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use opendal::Operator;
use serde::Deserialize;
use tokio::{io::{AsyncRead, AsyncReadExt}, process::Child, sync::Semaphore};
use tokio_util::io::SyncIoBridge;
use xlb_core::{
    image::{IMAGE_CHUNK_BYTES, IMAGE_INDEX_LIMIT, IMAGE_INDEX_PREFIX, ImageBlobIndex, blake3_digest_hex},
    layer::{LAYER_INDEX_LABEL, LAYER_INDEX_PREFIX, LayerIndex},
};

use crate::cas::{cas_chunk_stored, object_stored, publish_cas_chunk};

#[derive(Deserialize)]
struct ImageInputs {
    descriptor: PathBuf,
    manifest: PathBuf,
    config: PathBuf,
    layers: Vec<LayerInput>,
    img: PathBuf,
}

#[derive(Deserialize)]
struct LayerInput {
    path: PathBuf,
    cas: Option<PathBuf>,
    index: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Descriptor {
    digest: String,
    size: u64,
    media_type: String,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Manifest {
    config: Descriptor,
    layers: Vec<Descriptor>,
}

enum BlobInput {
    File(PathBuf),
    CompactLayer { stream: PathBuf, cas: PathBuf, img: PathBuf },
}

struct ImageBlob {
    descriptor: Descriptor,
    input: BlobInput,
    layer: Option<LayerPublication>,
}

struct LayerPublication {
    key: String,
    path: PathBuf,
}

pub struct ApplicationImage {
    manifest: ImageBlob,
    content: Vec<ImageBlob>,
}

impl ApplicationImage {
    pub fn open(plan: &Path, resolve: impl Fn(&Path) -> Option<PathBuf>) -> anyhow::Result<Self> {
        let inputs: ImageInputs = serde_json::from_slice(&read_metadata(plan)?)?;
        let runfile = |path: &Path| resolve(path).with_context(|| format!("locate image input {}", path.display()));
        let descriptor: Descriptor = serde_json::from_slice(&read_metadata(&runfile(&inputs.descriptor)?)?)?;
        let manifest_path = runfile(&inputs.manifest)?;
        let bytes = read_metadata(&manifest_path)?;
        ensure!(bytes.len() as u64 == descriptor.size && blake3::hash(&bytes).to_hex().as_str() == oci_digest_hex(&descriptor.digest)?,
            "OCI manifest digest or size differs from its descriptor");
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(!manifest.layers.is_empty() && manifest.layers.len() == inputs.layers.len(),
            "OCI manifest layers differ from the declared image inputs");
        let img = runfile(&inputs.img)?;
        let mut content = vec![ImageBlob { descriptor: manifest.config, input: BlobInput::File(runfile(&inputs.config)?), layer: None }];
        for (descriptor, layer) in manifest.layers.into_iter().zip(inputs.layers) {
            let path = runfile(&layer.path)?;
            let input = match layer.cas {
                Some(cas) => BlobInput::CompactLayer { stream: path, cas: runfile(&cas)?, img: img.clone() },
                None => BlobInput::File(path),
            };
            let layer = match (descriptor.annotations.get(LAYER_INDEX_LABEL), layer.index) {
                (Some(digest), Some(index)) => Some(LayerPublication {
                    key: format!("{LAYER_INDEX_PREFIX}{}", oci_digest_hex(digest)?),
                    path: runfile(&index)?,
                }),
                (None, None) => None,
                _ => anyhow::bail!("OCI layer index is missing its digest or publication input"),
            };
            content.push(ImageBlob { descriptor, input, layer });
        }
        Ok(Self { manifest: ImageBlob { descriptor, input: BlobInput::File(manifest_path), layer: None }, content })
    }

    pub fn digest(&self) -> &str {
        &self.manifest.descriptor.digest
    }

    /// Make the complete image visible when its manifest index is published.
    // [impl -> dsn~xlb-application-images~1]
    pub async fn publish(&self, origin: &Operator, concurrency: usize) -> anyhow::Result<()> {
        ensure!(concurrency > 0, "CAS upload concurrency must be positive");
        let uploads = Arc::new(Semaphore::new(concurrency));
        stream::iter(&self.content).map(Ok)
            .try_for_each_concurrent(concurrency, |blob| async {
                if let Some(layer) = &blob.layer {
                    layer.publish(blob, origin, uploads.clone(), concurrency).await?;
                }
                blob.publish(origin, uploads.clone(), concurrency).await
            }).await?;
        self.manifest.publish(origin, uploads, concurrency).await
    }
}

impl LayerPublication {
    // [impl -> dsn~xlb-fuse-snapshots~1]
    async fn publish(&self, blob: &ImageBlob, origin: &Operator, uploads: Arc<Semaphore>, concurrency: usize) -> anyhow::Result<()> {
        let bytes = read_metadata(&self.path)?;
        ensure!(self.key.ends_with(blake3::hash(&bytes).to_hex().as_str()), "OCI filesystem index differs from its descriptor");
        let index: LayerIndex = serde_json::from_slice(&bytes)?;
        let archive = index.archive;
        ensure!(archive.is_complete(), "OCI filesystem index has an incomplete archive");
        if object_stored(origin, &self.key, bytes.len() as u64).await? {
            let mut chunks = stream::iter(0..archive.chunks.len() as u64).map(|ordinal| {
                let uploads = uploads.clone();
                let chunk = archive.chunk(ordinal).expect("validated archive chunk list");
                async move {
                    let _permit = uploads.acquire_owned().await?;
                    cas_chunk_stored(origin, chunk.0, chunk.1).await
                }
            }).buffer_unordered(concurrency);
            let mut complete = true;
            while let Some(present) = chunks.try_next().await? { complete &= present; }
            if complete { return Ok(()); }
        }

        let (reader, mut writer) = io::pipe()?;
        let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader.into())?;
        let (input, mut process) = open_blob_input(&blob.input).await?;
        let media_type = blob.descriptor.media_type.clone();
        let input = SyncIoBridge::new(input);
        let decode = tokio::task::spawn_blocking(move || {
            crate::image_archive::decode_archive(&media_type, &mut BufReader::new(input), &mut writer)
        });
        stream::try_unfold((reader, 0), |(mut reader, ordinal)| {
            let chunk = archive.chunk(ordinal);
            let uploads = uploads.clone();
            async move {
                let Some((hash, size)) = chunk else {
                    ensure!(reader.read(&mut [0]).await? == 0, "OCI archive exceeds its index size");
                    return Ok::<_, anyhow::Error>(None);
                };
                let permit = uploads.acquire_owned().await?;
                let mut bytes = vec![0; size as usize];
                reader.read_exact(&mut bytes).await.context("read indexed OCI archive chunk")?;
                ensure!(blake3::hash(&bytes).as_bytes() == hash.as_bytes(), "OCI archive chunk {ordinal} differs from its index");
                Ok(Some(((Bytes::from(bytes), permit), (reader, ordinal + 1))))
            }
        }).map_ok(|(bytes, permit)| async move {
            let result = publish_cas_chunk(origin, bytes).await;
            drop(permit);
            result
        }).try_buffer_unordered(concurrency).try_for_each(|_| async { Ok(()) }).await?;
        decode.await??;
        if let Some(process) = process.as_mut() {
            ensure!(process.wait().await?.success(), "rules_img layer reconstruction failed");
        }
        origin.write(&self.key, bytes).await?;
        Ok(())
    }
}

impl ImageBlob {
    async fn publish(&self, origin: &Operator, uploads: Arc<Semaphore>, concurrency: usize) -> anyhow::Result<()> {
        let descriptor = &self.descriptor;
        let digest = oci_digest_hex(&descriptor.digest)?;
        let index_key = format!("{IMAGE_INDEX_PREFIX}{digest}");
        match origin.read(&index_key).await {
            Ok(bytes) => {
                let entry: ImageBlobIndex = serde_json::from_slice(&bytes.to_vec())?;
                ensure!(entry.has_complete_chunk_list() && entry.size_bytes == descriptor.size && entry.media_type == descriptor.media_type,
                    "published OCI index differs from the layout descriptor");
                let complete = stream::iter(entry.chunks.iter().enumerate().map(|(position, hash)| {
                    let size = (entry.size_bytes - position as u64 * IMAGE_CHUNK_BYTES as u64).min(IMAGE_CHUNK_BYTES as u64);
                    let uploads = uploads.clone();
                    async move {
                        let _permit = uploads.acquire_owned().await?;
                        cas_chunk_stored(origin, *hash, size).await
                    }
                })).buffer_unordered(concurrency).try_collect::<Vec<_>>().await?;
                if complete.into_iter().all(|present| present) {
                    return Ok(());
                }
                tracing::warn!(digest = %descriptor.digest, "repairing incomplete OCI blob");
            }
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        tracing::info!(digest = %descriptor.digest, bytes = descriptor.size, "publishing OCI blob to Tigris");
        let reader = BlobReader::open(&self.input, descriptor.size, digest).await?;
        let chunks = stream::try_unfold(reader, |mut reader| {
            let uploads = uploads.clone();
            async move {
                if reader.remaining == 0 {
                    reader.verify().await?;
                    return Ok(None);
                }
                let permit = uploads.acquire_owned().await?;
                let bytes = reader.read_chunk().await?;
                Ok(Some(((bytes, permit), reader)))
            }
        }).map_ok(|(bytes, permit)| async move {
            let result = publish_cas_chunk(origin, bytes).await;
            drop(permit);
            result
        })
            .try_buffered(concurrency).try_collect().await
            .with_context(|| format!("publish OCI blob {}", descriptor.digest))?;
        let index = ImageBlobIndex { chunks, size_bytes: descriptor.size, media_type: descriptor.media_type.clone() };
        let index = serde_json::to_vec(&index)?;
        ensure!(index.len() <= IMAGE_INDEX_LIMIT, "OCI blob index exceeds its read limit");
        origin.write(&index_key, index).await?;
        tracing::info!(digest = %descriptor.digest, bytes = descriptor.size, "published OCI blob index");
        Ok(())
    }
}

struct BlobReader<'a> {
    input: Box<dyn AsyncRead + Unpin + Send>,
    process: Option<Child>,
    remaining: u64,
    blake3: blake3::Hasher,
    expected: &'a str,
}

impl<'a> BlobReader<'a> {
    async fn open(input: &BlobInput, size: u64, expected: &'a str) -> anyhow::Result<Self> {
        let (input, process) = open_blob_input(input).await?;
        Ok(Self { input, process, remaining: size, blake3: blake3::Hasher::new(), expected })
    }

    async fn read_chunk(&mut self) -> anyhow::Result<Bytes> {
        let size = self.remaining.min(IMAGE_CHUNK_BYTES as u64) as usize;
        let mut bytes = vec![0; size];
        self.input.read_exact(&mut bytes).await?;
        self.blake3.update(&bytes);
        self.remaining -= size as u64;
        Ok(bytes.into())
    }

    async fn verify(mut self) -> anyhow::Result<()> {
        ensure!(self.input.read(&mut [0_u8]).await? == 0, "OCI stream exceeds its descriptor size");
        if let Some(mut process) = self.process {
            ensure!(process.wait().await?.success(), "rules_img layer reconstruction failed");
        }
        ensure!(self.blake3.finalize().to_hex().as_str() == self.expected, "OCI stream BLAKE3 differs from its descriptor");
        Ok(())
    }
}

async fn open_blob_input(input: &BlobInput) -> anyhow::Result<(Box<dyn AsyncRead + Unpin + Send>, Option<Child>)> {
    match input {
        BlobInput::File(path) => Ok((Box::new(tokio::fs::File::open(path).await?), None)),
        BlobInput::CompactLayer { stream, cas, img } => {
            let mut process = tokio::process::Command::new(img)
                .args(["compact-stream", "reconstruct", "--compact-stream"]).arg(stream)
                .arg("--cas-dir").arg(cas).args(["--output", "-"])
                .stdout(Stdio::piped()).kill_on_drop(true).spawn()?;
            let pipe = process.stdout.take().context("open rules_img layer stream")?;
            Ok((Box::new(pipe), Some(process)))
        }
    }
}

/// The hex of a BLAKE3 OCI digest; images are BLAKE3-addressed
/// (req~blake3-image-digests~1).
fn oci_digest_hex(digest: &str) -> anyhow::Result<&str> {
    blake3_digest_hex(digest).with_context(|| format!("expected a BLAKE3 OCI digest, got {digest}"))
}

fn read_metadata(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut reader = std::fs::File::open(path)?.take(IMAGE_INDEX_LIMIT as u64 + 1);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= IMAGE_INDEX_LIMIT, "OCI metadata exceeds its read limit");
    Ok(bytes)
}
