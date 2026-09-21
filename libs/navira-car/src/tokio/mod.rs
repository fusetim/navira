mod read;
mod write;

use std::num::NonZeroUsize;

pub use read::*;
pub use write::*;

pub const MAX_HEADER_SIZE: usize = 1 << 14; // 16 KiB
pub const MAX_SECTION_METADATA_SIZE: usize = 1 << 10; // 1 KiB (for the section metadata, which is typically small)

/// Resize the buffer based on the hint provided.
///
/// This function will only try to resize the buffer, if the hint suggests that the
/// buffer is too small or that the buffer is already full.
/// It will double the size of the buffer, unless it is already at or above the maximum size.
///
/// Returns false if the buffer needed to be resized but is already at or above the maximum size, true otherwise.
fn resize_buf_hint(
    buf: &mut Vec<u8>,
    bytes_read: usize,
    hint: Option<NonZeroUsize>,
    max_size: Option<NonZeroUsize>,
) -> bool {
    if bytes_read == buf.len() || hint.map_or(false, |h| (bytes_read + h.get()) > buf.len()) {
        // If the buffer is already at or above the maximum size, return an error
        if let Some(max) = max_size {
            if buf.len() >= max.get() {
                return false;
            }
        }
        buf.resize(buf.len() * 2, 0); // Double the buffer size
    }
    true
}
