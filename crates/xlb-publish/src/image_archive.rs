use std::io::{self, BufRead, Write};

use anyhow::{Result, bail, ensure};
use flate2::read::MultiGzDecoder;
use ruzstd::decoding::{BlockDecodingStrategy, FrameDecoder, errors::{FrameDecoderError, ReadFrameHeaderError}};

const ZSTD_BLOCK_BYTES: usize = 1024 * 1024;

pub fn decode_archive(media_type: &str, source: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    match media_type.rsplit(['+', '.']).next().unwrap_or_default() {
        "gzip" => { io::copy(&mut MultiGzDecoder::new(source), output)?; }
        "zstd" => decode_zstd_frames(source, output)?,
        "tar" => { io::copy(source, output)?; }
        other => bail!("layer media type {media_type} has unsupported compression {other}"),
    }
    Ok(())
}

/// Decode concatenated frames and skip the zstd skippable frames used by some layers.
fn decode_zstd_frames(source: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    let mut decoder = FrameDecoder::new();
    while !source.fill_buf()?.is_empty() {
        match decoder.reset(&mut *source) {
            Ok(()) => {}
            Err(FrameDecoderError::ReadFrameHeaderError(ReadFrameHeaderError::SkipFrame { length, .. })) => {
                let mut frame = io::Read::take(&mut *source, u64::from(length));
                let skipped = io::copy(&mut frame, &mut io::sink())?;
                ensure!(skipped == u64::from(length), "zstd skippable frame is truncated");
                continue;
            }
            Err(error) => bail!("decode zstd layer frame header: {error}"),
        }
        while !decoder.is_finished() {
            decoder.decode_blocks(&mut *source, BlockDecodingStrategy::UptoBytes(ZSTD_BLOCK_BYTES))
                .map_err(|error| anyhow::anyhow!("decode zstd layer block: {error}"))?;
            decoder.collect_to_writer(&mut *output)?;
        }
        decoder.collect_to_writer(&mut *output)?;
    }
    Ok(())
}
