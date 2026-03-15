//! # Index structure for CAR v2 files
//!
//! This module defines the index structure used in CAR v2 files,
//! which allows for efficient lookup of blocks by their CIDs.
//!
//! At the time of writing (2026-01), only two index types are defined in the CAR v2 specification:
//! - 0x0400 - IndexSorted: a sorted index of all the blocks in the CAR file, identified by their raw hash digest (not CID) and offset/length in the file.
//! - 0x0401 - MultihashIndexSorted: a sorted index of all the blocks in the CAR file, similarly identified but the hash function is also specified.
//!
//! The index is stored at the end of the CAR v2 file, and its start offset is indicated in the CAR v2 header.
//! The first bytes of the index indicate its type (LEB128 varint).
//!
//! ## IndexSorted (0x0400)
//!
//! The IndexSorted type consists of a sequence of entries, each containing:
//! - Raw hash digest of the block (length depends on the hash function used, e.g., 32 bytes for SHA-256)
//! - Offset of the block in the CAR file (u64, Little Endian)
//!
//! The entries are sorted by the raw hash digest for efficient binary search.
//!
//! Those entries are grouped into "buckets" that have a common hash size (32 bytes for SHA-256, etc).
//! Each bucket starts with the width of an entry (hash size + 8 bytes for offset) as u32le, and
//! the number of entries in that bucket as u64le, followed by the entries themselves.
//! All buckets are concatenated together to form the complete index, and sorted by hash size (smallest first).
//!
//! ## MultihashIndexSorted (0x0401)
//!
//! The MultihashIndexSorted type is similar to IndexSorted and reuses its structures. However, an additional
//! dimension is added to specify the hash function used for each bucket of entries.
//!
//! Buckets are now grouped by multihash code (u64, LEB128 varint), smallest first. The multihash code is
//! prefixed to each bucket, followed by the width of an entry (hash size + 8 bytes for offset) as u32le,
//! the number of entries in that bucket as u64le, and then the entries themselves.
//!
//! This allows the index to contain entries for blocks hashed with different algorithms.

use core::index;

use crate::wire::{v2::{indexsorted::InitReaderState, multihashindexsorted::InitMultihashIndexReaderState}, varint::UnsignedVarint};

pub mod indexsorted;
pub mod multihashindexsorted;

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enum representing the type of index in a CAR v2 file
pub enum IndexType {
    /// IndexSorted type (0x0400)
    IndexSorted = 0x0400,
    /// MultihashIndexSorted type (0x0401)
    MultihashIndexSorted = 0x0401,
}

impl IndexType {
    /// Creates an IndexType from a u64 value
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            0x0400 => Some(IndexType::IndexSorted),
            0x0401 => Some(IndexType::MultihashIndexSorted),
            _ => None,
        }
    }
}

pub trait IndexRead: Sized {
    /// Receives data for the index reader, copying from the provided buffer into
    /// the reader's internal state as needed.
    ///
    /// # Arguments
    /// * `buf` - Buffer containing the data to be read
    /// * `offset` - Offset in the CARv2 index section where this data has been read from.
    ///              This allows the reader to know where in the index section this data belongs,
    ///              which is necessary to correctly parse the index structure, especially when it
    ///              is large and read in chunks.
    fn receive_data(&mut self, buf: &[u8], offset: usize);
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OwnedIndexReader {
    IndexSorted(indexsorted::OwnedIndexReader),
    MultihashIndexSorted(multihashindexsorted::OwnedMultihashIndexReader),
    Unknown([u8; 16]), // Placeholder for unknown index types, we can store the raw data until we can determine the type
}

impl OwnedIndexReader {
    pub fn new(index_type: IndexType) -> Self {
        match index_type {
            IndexType::IndexSorted => OwnedIndexReader::IndexSorted(indexsorted::OwnedIndexReader::new()),
            IndexType::MultihashIndexSorted => OwnedIndexReader::MultihashIndexSorted(multihashindexsorted::OwnedMultihashIndexReader::new()),
        }
    }

    pub fn get_type(&self) -> Option<IndexType> {
        match self {
            OwnedIndexReader::IndexSorted(_) => Some(IndexType::IndexSorted),
            OwnedIndexReader::MultihashIndexSorted(_) => Some(IndexType::MultihashIndexSorted),
            OwnedIndexReader::Unknown(_) => None,
        }
    }
}

impl IndexRead for OwnedIndexReader {
    fn receive_data(&mut self, buf: &[u8], offset: usize) {
        match self {
            OwnedIndexReader::IndexSorted(reader) => reader.receive_data(buf, offset),
            OwnedIndexReader::MultihashIndexSorted(reader) => reader.receive_data(buf, offset),
            OwnedIndexReader::Unknown(data) => {
                // For unknown index types, we just accumulate the raw data
                let start = 0.max(offset);
                let end = 16.min(offset + buf.len());
                let len = end - start;
                if start < end {
                    data[start..end].copy_from_slice(&buf[..len]);
                }

                // TODO: Try to convert into a known index type if we have enough data
            }
        }
    }
}