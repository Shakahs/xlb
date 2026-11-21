//! Converts a rules_img image to BLAKE3 addressing: BLAKE3 layer digests and
//! diff IDs, and the config, manifest and manifest descriptor that name them.
//! Runs as a Bazel action declared by `image_publication_inputs`.
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::PathBuf,
    process::{Child, Command, Stdio},
};

use anyhow::{Context, ensure};
use serde::Deserialize;
use serde_json::Value;
use xlb_core::layer::{LAYER_INDEX_LABEL, LAYER_INDEX_LIMIT};

use xlb_publish::image_archive;
mod index_image_layer;

const READ_BUFFER_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize)]
struct ConversionInputs {
    descriptor: PathBuf,
    manifest: PathBuf,
    config: PathBuf,
    img: PathBuf,
    layers: Vec<LayerSource>,
    out_descriptor: PathBuf,
    out_manifest: PathBuf,
    out_config: PathBuf,
}

#[derive(Deserialize)]
struct LayerSource {
    path: PathBuf,
    cas: Option<PathBuf>,
    index: PathBuf,
}

/// The BLAKE3 digests of one layer: its stored bytes and its uncompressed tar.
struct LayerDigests {
    blob: blake3::Hash,
    diff_id: blake3::Hash,
    index: blake3::Hash,
}

/// Feeds every byte read from `inner` to a BLAKE3 hasher.
struct HashingReader<R> {
    inner: R,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..count]);
        self.bytes += count as u64;
        Ok(count)
    }
}

fn digest(hash: blake3::Hash) -> String {
    format!("blake3:{}", hash.to_hex())
}

/// Opens a layer's stored bytes: the blob itself, or the exact compressed
/// stream the pinned rules_img reconstructor rebuilds from a compact layer.
fn open_layer(source: &LayerSource, img: &PathBuf) -> anyhow::Result<(Box<dyn Read + Send>, Option<Child>)> {
    match &source.cas {
        None => Ok((Box::new(File::open(&source.path).with_context(|| format!("open layer {}", source.path.display()))?), None)),
        Some(cas) => {
            let mut process = Command::new(img)
                .args(["compact-stream", "reconstruct", "--compact-stream"]).arg(&source.path)
                .arg("--cas-dir").arg(cas).args(["--output", "-"])
                .stdout(Stdio::piped()).spawn().context("start the rules_img layer reconstructor")?;
            let stream = process.stdout.take().context("open the rules_img layer stream")?;
            Ok((Box::new(stream), Some(process)))
        }
    }
}

/// Streams one layer once, hashing its stored bytes and its decoded tar.
// [impl -> dsn~xlb-application-images~1]
fn layer_digests(source: &LayerSource, img: &PathBuf, descriptor: &Value) -> anyhow::Result<LayerDigests> {
    let media_type = descriptor["mediaType"].as_str().context("layer descriptor has no media type")?;
    let size = descriptor["size"].as_u64().context("layer descriptor has no size")?;
    let (input, process) = open_layer(source, img)?;
    let mut stored = BufReader::with_capacity(READ_BUFFER_BYTES, HashingReader { inner: input, hasher: blake3::Hasher::new(), bytes: 0 });
    let (reader, mut writer) = io::pipe()?;
    let (index, diff_id) = std::thread::scope(|scope| {
        let indexing = scope.spawn(move || index_image_layer::read_layer(reader));
        let decoding = image_archive::decode_archive(media_type, &mut stored, &mut writer);
        drop(writer);
        let parsed = indexing.join().expect("layer index worker panicked");
        if let Err(error) = &decoding {
            // An early parser error closes the pipe and can interrupt the decoder.
            if error.downcast_ref::<io::Error>().is_none_or(|error| error.kind() != io::ErrorKind::BrokenPipe) {
                decoding?;
            }
        }
        parsed
    })?;
    io::copy(&mut stored, &mut io::sink())?;
    let stored = stored.into_inner();
    if let Some(mut process) = process {
        ensure!(process.wait()?.success(), "rules_img layer reconstruction failed");
    }
    ensure!(stored.bytes == size, "layer stream has {} bytes, its descriptor {size}", stored.bytes);
    let blob = stored.hasher.finalize();
    let entries = index.entries.len();
    let index = serde_json::to_vec(&index)?;
    ensure!(index.len() <= LAYER_INDEX_LIMIT, "OCI layer {} index has {} bytes for {entries} entries, limit {LAYER_INDEX_LIMIT}",
        source.path.display(), index.len());
    std::fs::write(&source.index, &index)?;
    Ok(LayerDigests { blob, diff_id, index: blake3::hash(&index) })
}

fn read_json(path: &PathBuf) -> anyhow::Result<Value> {
    serde_json::from_slice(&std::fs::read(path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

// [impl -> dsn~xlb-application-images~1]
fn main() -> anyhow::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let plan = PathBuf::from(arguments.next().context("expected CONVERSION_INPUTS")?);
    ensure!(arguments.next().is_none(), "expected CONVERSION_INPUTS");
    let inputs: ConversionInputs = serde_json::from_slice(&std::fs::read(&plan)?)?;
    let mut manifest = read_json(&inputs.manifest)?;
    let mut config = read_json(&inputs.config)?;
    let mut descriptor = read_json(&inputs.descriptor)?;
    let layers = manifest["layers"].as_array().context("OCI manifest has no layers")?.clone();
    ensure!(!layers.is_empty() && layers.len() == inputs.layers.len(), "OCI manifest layers differ from the declared layer inputs");
    ensure!(config["rootfs"]["diff_ids"].as_array().is_some_and(|ids| ids.len() == layers.len()),
        "OCI config diff IDs differ from the manifest layers");

    let digests = std::thread::scope(|scope| {
        let workers: Vec<_> = inputs.layers.iter().zip(&layers)
            .map(|(source, layer)| scope.spawn(|| layer_digests(source, &inputs.img, layer)))
            .collect();
        workers.into_iter().map(|worker| worker.join().expect("layer digest worker panicked")).collect::<anyhow::Result<Vec<_>>>()
    })?;

    config["rootfs"]["diff_ids"] = digests.iter().map(|layer| Value::from(digest(layer.diff_id))).collect();
    let config_bytes = serde_json::to_vec(&config)?;
    manifest["config"]["digest"] = digest(blake3::hash(&config_bytes)).into();
    manifest["config"]["size"] = config_bytes.len().into();
    for (position, layer) in digests.iter().enumerate() {
        manifest["layers"][position]["digest"] = digest(layer.blob).into();
        manifest["layers"][position]["annotations"][LAYER_INDEX_LABEL] = digest(layer.index).into();
    }
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    descriptor["digest"] = digest(blake3::hash(&manifest_bytes)).into();
    descriptor["size"] = manifest_bytes.len().into();

    std::fs::write(&inputs.out_config, config_bytes)?;
    std::fs::write(&inputs.out_manifest, manifest_bytes)?;
    std::fs::write(&inputs.out_descriptor, serde_json::to_vec(&descriptor)?)?;
    Ok(())
}
