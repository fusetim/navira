use crate::{
    types::Sealed,
    wire::v2::{Borrowable, DataCursor, indexsorted::{IndexReader, IndexReaderReadError}},
};

use super::IndexRead;
pub use super::indexsorted::{self, IndexEntry, IndexReaderOpenError, IndexSortedBucketHeader};

/// Represents the header of a MultihashIndexSorted bucket
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultihashIndexSortedBucketHeader {
    /// Multihash code for this bucket
    pub multihash_code: u64,
    /// Number of entries in this bucket
    pub entry_count: u32,
}

pub trait MultihashIndexReaderState: Sealed + core::fmt::Debug {}

#[derive(Debug)]
pub struct MultihashIndexReader<'a, S: MultihashIndexReaderState> {
    data: Borrowable<'a, DataCursor>,
    state: S,
}

#[derive(Debug, Clone)]
pub struct InitMultihashIndexReaderState(());

impl Sealed for InitMultihashIndexReaderState {}
impl MultihashIndexReaderState for InitMultihashIndexReaderState {}

impl<'a> MultihashIndexReader<'a, InitMultihashIndexReaderState> {
    pub fn new<D: Into<Borrowable<'a, DataCursor>>>(data: D) -> Self {
        Self {
            data: data.into(),
            state: InitMultihashIndexReaderState(()),
        }
    }

    pub fn open(
        mut self,
    ) -> Result<
        MultihashIndexReader<'a, ReadyMultihashIndexReaderState>,
        (Self, IndexReaderOpenError),
    > {
        // Try to parse the index type from the initial data we have received
        if self.data.len() < 1 {
            // We don't have enough data to determine the index type, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        // The index type is stored as a LEB128 varint at the beginning of the index data
        let (index_type, index_type_len) =
            match crate::wire::varint::UnsignedVarint::decode(&self.data) {
                Some((value, len)) => (value, len),
                None => {
                    // If we fail to decode the varint, it means we don't have enough data to
                    // determine the index type, we need to receive more data
                    // However, if we have more than 8 bytes, it means we have enough data to determine the index type, but it is invalid
                    if self.data.len() > 8 {
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
        if self.data.len() < index_type_len + 4 {
            // We don't have enough data to parse the number of bucketgroups, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        let bucketgroups_count = u32::from_le_bytes(
            self.data[index_type_len..(index_type_len + 4)]
                .try_into()
                .unwrap(),
        );

        // If therer is at least one bucketgroup, try to "open it".
        let mut bucketgroup_cur = None;
        if bucketgroups_count > 0 {
            // Read the bucketgroup header
            if self.data.len() < index_type_len + 4 + 8 + 4 {
                // Not enough data to read the header
                return Err((self, IndexReaderOpenError::InsufficientData));
            }

            let multihash_code = u64::from_le_bytes(
                self.data[(index_type_len + 4)..(index_type_len + 4 + 8)]
                    .try_into()
                    .unwrap(),
            );
            let buckets_count = u32::from_le_bytes(
                self.data[(index_type_len + 12)..(index_type_len + 12 + 4)]
                    .try_into()
                    .unwrap(),
            );

            // Consume the data read (index type + bucket count)
            self.data.consume(index_type_len + 16);

            bucketgroup_cur = Some(BucketGroupReader {
                header: MultihashIndexSortedBucketHeader {
                    multihash_code,
                    entry_count: buckets_count,
                },
                reader_state: indexsorted::IndexReader::new_state_with(buckets_count, self.data.get_offset()),
            })
        } else {
            // Consume the data read (index type + bucket count)
            self.data.consume(index_type_len + 4);
        }

        Ok(MultihashIndexReader {
            data: self.data,
            state: ReadyMultihashIndexReaderState {
                bucketgroups_count,
                bucketgroups_read: 0,
                bucketgroup_cur,
            },
        })
    }
}

impl <'a, S: MultihashIndexReaderState> IndexRead for MultihashIndexReader<'a, S> {
    fn receive_data(&mut self, buf: &[u8], offset: usize) {
        self.data.ingest(buf, offset);
    }
}

#[derive(Debug, Clone)]
pub struct BucketGroupReader {
    header: MultihashIndexSortedBucketHeader,
    reader_state: indexsorted::ReadyReaderState,
}

impl BucketGroupReader {
    pub fn header(&self) -> &MultihashIndexSortedBucketHeader {
        &self.header
    }

    pub fn with_reader<'a, F, R>(&mut self, data: Borrowable<'a, DataCursor>, f: F) -> R
    where
        F: FnOnce(&mut indexsorted::IndexReader<'a, indexsorted::ReadyReaderState>) -> R,
    {   
        let mut reader = indexsorted::IndexReader::<'a, indexsorted::ReadyReaderState>::from_parts(data, self.reader_state.clone());
        let result = f(&mut reader);
        self.reader_state = reader.into_parts().1;
        result
    }
}

/// State of the IndexReader after successfully opening the index and confirming it is of the expected type (IndexSorted).
#[derive(Debug, Clone)]
pub struct ReadyMultihashIndexReaderState {
    bucketgroups_count: u32,
    bucketgroups_read: u32,
    bucketgroup_cur: Option<BucketGroupReader>,
}

impl Sealed for ReadyMultihashIndexReaderState {}
impl MultihashIndexReaderState for ReadyMultihashIndexReaderState {}

impl<'a> MultihashIndexReader<'a, ReadyMultihashIndexReaderState> {
    pub fn count_bucketgroups(&self) -> usize {
        self.state.bucketgroups_count as usize
    }

    pub fn read_next_bucketgroup(
        &mut self,
    ) -> Result<&mut BucketGroupReader, IndexReaderReadError> {
        if self.state.bucketgroup_cur.is_some() {
            if self.state.bucketgroups_read == 0 {
                // Special case: the first one is always open!
                self.state.bucketgroups_read += 1;
                Ok(self.state.bucketgroup_cur.as_mut().unwrap())
            } else if self.state.bucketgroups_read >= self.state.bucketgroups_count {
                // No more bucketgroup to read
                Err(IndexReaderReadError::EndOfIndex)
            } else {
                // 1. Seek to the end of the current bucket group
                let reader = self.state.bucketgroup_cur.as_mut().unwrap();
                loop {
                    match reader.with_reader(self.data.as_borrowed(), |r| r.read_next_bucket().map(|_| ())) {
                        Ok(_) => continue, // Keep reading until we reach the end of the bucket
                        Err(IndexReaderReadError::EndOfIndex) => break, // End of bucket reached, we can move to the next bucketgroup
                        // Any other error means something went wrong while reading the bucket, we should return the error
                        // Especially if it is an InsufficientData error, it means we need to receive more data before we can
                        // continue reading the current bucket, and we should not try to read the next bucketgroup until we
                        // have enough data to read the current bucket
                        Err(e) => return Err(e),
                    }
                }

                // 2. Read the next bucketgroup header
                let multihash_code;
                let buckets_count;
                {
                    // Check if enough data for the next bucketgroup header
                    if self.data.len() < 8 + 4 {
                        return Err(IndexReaderReadError::InsufficientData(
                            self.data.get_end_offset(),
                            12 - self.data.len(),
                        ));
                    }

                    multihash_code = u64::from_le_bytes(self.data[0..8].try_into().unwrap());
                    buckets_count = u32::from_le_bytes(self.data[8..12].try_into().unwrap());

                    // Consume the data read for the bucketgroup header
                    self.data.consume(12);
                }

                // 4. Create the new bucketgroup_cur
                self.state.bucketgroup_cur = Some(BucketGroupReader {
                    header: MultihashIndexSortedBucketHeader {
                        multihash_code,
                        entry_count: buckets_count,
                    },
                    reader_state: IndexReader::new_state_with(buckets_count, self.data.get_offset()),
                });

                // 5. Increment the number of bucketgroup read
                self.state.bucketgroups_read += 1;

                // 6. Return the new bucketgroup_reader
                Ok(self.state.bucketgroup_cur.as_mut().unwrap())
            }
        } else {
            Err(IndexReaderReadError::EndOfIndex)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    const RAW_MULTIHASHSORTED_INDEX: [u8; 150] = [
        // Index type (LEB128 varint) - 0x0401 (MultihashIndexSorted)
        0x81, 0x08, // Count of MultihashIndexSorted buckets (4 bytes) - 1
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
        0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
        0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
        0x29, 0x4D, // Offset (8 bytes)
        0xB7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Second entry
        // Hash (32 bytes)
        0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
        0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
        0x07, 0x2D, // Offset (8 bytes)
        0x8E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Third entry
        // Hash (32 bytes)
        0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
        0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
        0x5F, 0xF5, // Offset (8 bytes)
        0xE3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn test_multihashindexsorted_count_bucketgroups() {
        let mut reader = MultihashIndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_MULTIHASHSORTED_INDEX.clone(), 0);
        let reader = reader
            .open()
            .expect("Failed to open MultihashIndexSorted index");
        assert_eq!(reader.count_bucketgroups(), 1);
    }

    #[test]
    fn test_multihashindexsorted_read_first_bucketgroup_header() {
        let mut reader = MultihashIndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_MULTIHASHSORTED_INDEX.clone(), 0);
        let mut reader = reader
            .open()
            .expect("Failed to open MultihashIndexSorted index");
        assert_eq!(reader.count_bucketgroups(), 1);
        let bg_reader = reader
            .read_next_bucketgroup()
            .expect("Failed to read first bucket group");
        let header = bg_reader.header();
        assert_eq!(header.multihash_code, 0x12);
        assert_eq!(header.entry_count, 1);
    }

    #[test]
    fn test_multihashindexsorted_read_first_bucketgroup() {
        let mut reader = MultihashIndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_MULTIHASHSORTED_INDEX.clone(), 0);
        let mut reader = reader
            .open()
            .expect("Failed to open MultihashIndexSorted index");
        assert_eq!(reader.count_bucketgroups(), 1);

        // Read the first bucket group and check its header
        let bg_reader = reader
            .read_next_bucketgroup()
            .expect("Failed to read first bucket group");
        let header = bg_reader.header();
        assert_eq!(header.multihash_code, 0x12);
        assert_eq!(bg_reader.count_buckets(), 1);

        // Read the first bucket and check its entries
        let mut bucket_reader = bg_reader
            .read_next_bucket()
            .expect("Failed to read first bucket");
        assert_eq!(bucket_reader.count_entries(), 3);

        let entry3 = read_entry(&mut bucket_reader, 2, &RAW_MULTIHASHSORTED_INDEX);
        assert_eq!(entry3.hash, vec![
            0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
            0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
            0x5F, 0xF5,
        ]);
        assert_eq!(entry3.offset, 227);

        let entry1 = read_entry(&mut bucket_reader, 0, &RAW_MULTIHASHSORTED_INDEX);
        assert_eq!(entry1.hash, vec![
            0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
            0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
            0x29, 0x4D, 
        ]);
        assert_eq!(entry1.offset, 183);

        let entry2 = read_entry(&mut bucket_reader, 1, &RAW_MULTIHASHSORTED_INDEX);
        assert_eq!(entry2.hash, vec![
            0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
            0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
            0x07, 0x2D, 
        ]);
        assert_eq!(entry2.offset, 142);
    }

    fn read_entry(bucket: &mut indexsorted::IndexSortedBucketReader, n: usize, index_data: &[u8]) -> IndexEntry {
        loop {
            match bucket.read_entry(n) {
                Ok(entry) => return entry,
                Err(indexsorted::IndexSortedBucketReadError::InsufficientData(offset, size)) => {
                    // We need to provide more data
                    let end = offset + size;
                    if end > index_data.len() {
                        panic!(
                            "Not enough data, got hint: start {}, end {}, size {}, but total data length is {}",
                            offset,
                            end,
                            size,
                            index_data.len()
                        );
                    }
                    bucket.receive_data(&mut index_data[offset..end].to_vec(), offset);
                }
                Err(err) => panic!("Failed to read entry 3: {}", err),
            }
        }
    }
}
