//! Process allocator settings for chunk-sized buffers.

/// Allocations of at least this size get their own mapping. It is below the
/// CAS chunk size, so every full chunk buffer, and every final chunk at least
/// this long, returns to the kernel when it is freed.
const CHUNK_MMAP_THRESHOLD: usize = 1024 * 1024;

const _: () = assert!(CHUNK_MMAP_THRESHOLD < xlb_core::chunks::CAS_CHUNK_BYTES as usize);

/// Give chunk-sized allocations their own mappings for the life of the process.
///
/// glibc raises its mmap threshold each time it frees a mapped block, after
/// which freed chunk buffers stay resident in its arenas. Setting the
/// threshold explicitly disables that adjustment. Call it before XLB
/// allocates chunks.
// [impl -> dsn~xlb-foyer-cas~1]
pub fn map_chunk_buffers() -> std::io::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: mallopt only changes allocator parameters.
        let threshold = CHUNK_MMAP_THRESHOLD as libc::c_int;
        if unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, threshold) } != 1 {
            return Err(std::io::Error::other("glibc rejected the chunk mmap threshold"));
        }
    }
    Ok(())
}
