use std::io::Read;

use crate::{types::Sealed, wire::v2::indexsorted::{IndexReader, ReadyReaderState}};

use super::IndexRead;
pub use super::indexsorted::{IndexEntry, IndexSortedBucketHeader, IndexReaderOpenError, self};

/// Represents the header of a MultihashIndexSorted bucket
#[derive(Clone, PartialEq, Eq)]
pub struct MultihashIndexSortedBucketHeader {
    /// Multihash code for this bucket
    pub multihash_code: u64,
    /// Number of entries in this bucket
    pub entry_count: u32,
}

pub trait MultihashIndexReaderState: Sealed {}

pub struct MultihashIndexReader<S: MultihashIndexReaderState> {
    state: S,
}

pub struct InitMultihashIndexReaderState<'a> {
    data: &'a mut Vec<u8>,
    offset: &'a mut usize,
}

impl Sealed for InitMultihashIndexReaderState<'_> {}
impl MultihashIndexReaderState for InitMultihashIndexReaderState<'_> {}

impl<'a> MultihashIndexReader<InitMultihashIndexReaderState<'a>> {
    pub fn new(data: &'a mut Vec<u8>, offset: &'a mut usize) -> Self {
        Self {
            state: InitMultihashIndexReaderState { data, offset },
        }
    }

    pub fn open(self) -> Result<MultihashIndexReader<ReadyMultihashIndexReaderState<'a>>, (Self, IndexReaderOpenError)> {
        // Try to parse the index type from the initial data we have received
        if self.state.data.len() < 1 {
            // We don't have enough data to determine the index type, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        // The index type is stored as a LEB128 varint at the beginning of the index data
        let (index_type, index_type_len) =
            match crate::wire::varint::UnsignedVarint::decode(&self.state.data) {
                Some((value, len)) => (value, len),
                None => {
                    // If we fail to decode the varint, it means we don't have enough data to
                    // determine the index type, we need to receive more data
                    // However, if we have more than 8 bytes, it means we have enough data to determine the index type, but it is invalid
                    if self.state.data.len() > 8 {
                        return Err((self, IndexReaderOpenError::IndexTypeMismatch));
                    } else {
                        return Err((self, IndexReaderOpenError::InsufficientData));
                    }
                }
            };

        // Check if the index type is MultihashIndexSorted (0x0401)
        if index_type.0 != 0x0401 {
            // If the index type is not MultihashIndexSorted, it is invalid for this reader
            return Err((self, IndexReaderOpenError::IndexTypeMismatch));
        }

        // Parse the number of bucketgroups
        if self.state.data.len() < index_type_len + 4 {
            // We don't have enough data to parse the number of bucketgroups, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        let bucketgroups_count = u32::from_le_bytes(self.state.data[index_type_len..(index_type_len + 4)].try_into().unwrap());

        // If therer is at least one bucketgroup, try to "open it".
        let mut bucketgroup_cur = None;
        if bucketgroups_count > 0 {
            // Read the bucketgroup header
            if self.state.data.len() < index_type_len + 4 + 8 + 4 {
                // Not enough data to read the header
                return Err((self, IndexReaderOpenError::InsufficientData));
            }

            let multihash_code = u64::from_le_bytes(self.state.data[(index_type_len + 4)..(index_type_len + 4 + 8)].try_into().unwrap());
            let buckets_count = u32::from_le_bytes(self.state.data[(index_type_len + 12)..(index_type_len + 12 + 4)].try_into().unwrap());

            // Consume the data read (index type + bucket count)
            self.state.data.drain(0..index_type_len + 16);
            *self.state.offset += index_type_len + 16;

            bucketgroup_cur = Some(
                BucketGroup { 
                    header: MultihashIndexSortedBucketHeader { multihash_code, entry_count: buckets_count },
                    reader: IndexReader::<ReadyReaderState<'_>>::new(self.state.data, self.state.offset, buckets_count),
                }
            )
        } else {
            // Consume the data read (index type + bucket count)
            self.state.data.drain(0..index_type_len + 4);
            *self.state.offset += index_type_len + 4;
        }

        Ok(MultihashIndexReader {
            state: ReadyMultihashIndexReaderState {
                bucketgroups_count,
                bucketgroups_read: 0,
                bucketgroup_cur,
            },
        })
    }
}

struct BucketGroup<'a> {
    header: MultihashIndexSortedBucketHeader,
    reader: indexsorted::IndexReader<indexsorted::ReadyReaderState<'a>>
}

/// State of the IndexReader after successfully opening the index and confirming it is of the expected type (IndexSorted).
pub struct ReadyMultihashIndexReaderState<'a> {
    bucketgroups_count: u32,
    bucketgroups_read: u32,
    bucketgroup_cur: Option<BucketGroup<'a>>
}

impl Sealed for ReadyMultihashIndexReaderState<'_> {}
impl MultihashIndexReaderState for ReadyMultihashIndexReaderState<'_> {}

impl IndexRead for MultihashIndexReader<ReadyMultihashIndexReaderState<'_>> {
    fn receive_data(&mut self, buf: &mut [u8], offset: usize) {
        if let Some(bg) = self.state.bucketgroup_cur.as_mut() {
            bg.reader.receive_data(buf, offset);
        }
    }
}

impl<'a> MultihashIndexReader<ReadyMultihashIndexReaderState<'a>> {
    pub fn count(&self) -> usize {
        self.state.bucketgroups_count as usize
    }

    pub fn read_next_bucketgroup(&mut self) -> Result<&mut IndexReader<ReadyReaderState<'a>>, IndexReaderReadError> {
        if let Some(bg) = self.state.bucketgroup_cur.as_mut() {
            if self.state.bucketgroups_read == 0 {
                // Special case: the first one is always open!
                self.state.bucketgroups_read += 1;
                Ok(&mut bg.reader)
            } else if self.state.bucketgroups_read >= self.state.bucketgroups_count {
                // No more bucketgroup to read
                Err(IndexReaderReadError::EndOfIndex)
            } else {
                // 1. Seek to the end of the current bucket group
                // 2. Read the next bucketgroup header
                // 3. Bring down the current bucketgroup reader
                // 4. Create the new bucketgroup_cur
                // 5. Increment the number of bucketgroup read
                // 6. Return the new bucketgroup_reader
            }
        } else {
            Err(IndexReaderReadError::EndOfIndex)
        }
    }
}

/// Errors that can occur while reading the index data after successfully opening the index.
#[derive(thiserror::Error, Debug)]
pub enum IndexReaderReadError {
    /// Not enough data has been received to correctly read the next bucket,
    /// we need to receive more data before we can continue reading.
    #[error("Not enough data to read the next bucket, need to receive more data")]
    InsufficientData(usize, usize), // (current data length, expected data length)
    /// Unexpected format for the bucket header, it does not match the expected format for an IndexSorted bucket.
    #[error("Bad bucket format")]
    BadBucketFormat,
    /// End of index reached, there are no more buckets to read.
    #[error("End of index reached, no more buckets to read")]
    EndOfIndex,
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_MULTIHASHSORTED_INDEX: [u8; 150] = [
        // Index type (LEB128 varint) - 0x0401 (MultihashIndexSorted)
        0x81, 0x08, 
        // Count of MultihashIndexSorted buckets (4 bytes) - 1
        0x01, 0x00, 0x00, 0x00, 
        // First Multihash bucket
        // Multihash codec (8 bytes) - 0x12 (SHA-256)
        0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
        // Count of IndexSorted buckets (4 bytes) - 1
        0x01, 0x00, 0x00, 0x00,
        // Width (4 bytes) - 40 bytes (32 bytes hash + 8 bytes offset) -> 0x28
        0x28, 0x00, 0x00, 0x00,
        // Cumulative length of all entries in this bucket (8 bytes)
        0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // First entry
        // Hash (32 bytes)
        0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 
        0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C, 0xA2,
        0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 
        0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C, 0x29, 0x4D,
        // Offset (8 bytes) 
        0xB7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Second entry
        // Hash (32 bytes)
        0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C,
        0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14, 0xEF,
        0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27,
        0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5, 0x07, 0x2D,
        // Offset (8 bytes)
        0x8E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Third entry
        // Hash (32 bytes)
        0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77,
        0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8, 0x44,
        0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B,
        0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB, 0x5F, 0xF5, 
        // Offset (8 bytes)
        0xE3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    ];
}