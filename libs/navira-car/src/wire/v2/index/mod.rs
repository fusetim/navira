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

use std::{cmp::Ordering, ops::{Deref, DerefMut}};


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

/// Borrowable is a helper enum to represent either an owned value of type T or a mutable reference to T.
/// 
/// This is useful for the IndexReader to allow it to work with either owned or borrowed data buffers, enabling more flexible memory management.
#[derive(Debug)]
pub enum Borrowable<'a, T> {
    Owned(T),
    Borrowed(&'a mut T)
}

impl<'a, T> Borrowable<'a, T> {
    pub fn new_owned(value: T) -> Self {
        Borrowable::Owned(value)
    }

    pub fn new_borrowed(value: &'a mut T) -> Self {
        Borrowable::Borrowed(value)
    }

    pub fn as_borrowed(&mut self) -> Borrowable<'_, T> {
        match self {
            Borrowable::Owned(value) => Borrowable::Borrowed(value),
            Borrowable::Borrowed(value) => Borrowable::Borrowed(*value),
        }
    }

    pub fn get(&self) -> &T {
        match self {
            Borrowable::Owned(value) => value,
            Borrowable::Borrowed(value) => *value,
        }
    }

    /// Gets a mutable reference to the inner value, whether it's owned or borrowed.
    pub fn get_mut(&mut self) -> &mut T {
        match self {
            Borrowable::Owned(value) => value,
            Borrowable::Borrowed(value) => *value,
        }
    }
}

impl<'a, T> Deref for Borrowable<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl <'a, T> DerefMut for Borrowable<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

impl<'a, T> From<T> for Borrowable<'a, T> {
    fn from(value: T) -> Self {
        Borrowable::Owned(value)
    }
}

impl<'a, T> From<&'a mut T> for Borrowable<'a, T> {
    fn from(value: &'a mut T) -> Self {
        Borrowable::Borrowed(value)
    }
}

/// DataCursor is a structure for the internal buffer
/// of the IndexWriter / [IndexReader]. 
/// 
/// Goal of this structure is to enable to use the reader with owned and borrowed 
/// buffers. This allows the ability to share an exisiting data cursor between the different 
/// levels of an Index (MultiHash > IndexSorted > Buckets).
#[derive(Debug, Clone)]
pub struct DataCursor {
    data: Vec<u8>,
    offset: usize,
}

impl AsRef<[u8]> for DataCursor {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl DataCursor {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
        }
    }
}

impl DataCursor {
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_end_offset(&self) -> usize {
        self.offset + self.data.len()
    }

    pub fn consume(&mut self, len: usize) {
        self.offset += len;
        if len >= self.data.len() {
            self.data.clear();
        } else {
            self.data.drain(0..len);
        }
    }

    pub fn seek(&mut self, offset: usize) {
        match offset.cmp(&self.offset) {
            Ordering::Equal => { /* Do nothing */}
            Ordering::Less => {
                // New offset is before the current offset
                // Just go to this offset and reset the data buffer
                self.data.clear();
                self.offset = offset;
            }
            Ordering::Greater => {
                self.consume(offset - self.offset);
            }
        }
    }

    pub fn ingest(&mut self, buf: &[u8], pos: usize) {
        if pos != self.get_end_offset() {
            self.data.clear();
            self.offset = pos;
        }
        self.data.extend_from_slice(buf);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Deref for DataCursor {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for DataCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}