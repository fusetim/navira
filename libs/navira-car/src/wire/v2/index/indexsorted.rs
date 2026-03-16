use std::cmp::Ordering;

use super::IndexRead;
use crate::{types::Sealed, wire::v2::{Borrowable, DataCursor}};

/// A single entry in the CAR v2 index
#[derive(Clone, PartialEq, Eq)]
pub struct IndexEntry {
    /// Raw hash digest of the block
    pub hash: Vec<u8>,
    /// Offset of the block in the CAR file
    pub offset: u64,
}

/// Header of an IndexSorted bucket
#[derive(Clone, PartialEq, Eq)]
pub struct IndexSortedBucketHeader {
    /// Width of each entry (hash size + 8 bytes for offset)
    pub entry_width: u32,
    /// Cumulative length of all entries in this bucket (entry_width * number of entries)
    ///
    /// The CAR v2 specification says this should be the number of entries, but in practice it
    /// seems to be the total length of the bucket (entry_width * number of entries).
    /// This is what go-car does, but it seems odd.
    pub entries_len: u64,
}

impl IndexSortedBucketHeader {
    /// Get the width of each entry in bytes (hash size + 8 bytes for offset)
    pub fn get_entry_width(&self) -> usize {
        return self.entry_width as usize;
    }

    /// Get the width of the hash in bytes (entry width - 8 bytes for offset)
    pub fn get_hash_width(&self) -> usize {
        return (self.entry_width as usize) - 8; // The last 8 bytes are for the offset
    }

    /// Get the cumulative length of all entries in this bucket in bytes (entry_width * number of entries)
    pub fn get_entries_len(&self) -> usize {
        return self.entries_len as usize;
    }

    /// Get the number of entries in this bucket (entries_len / entry_width)
    pub fn count_entries(&self) -> usize {
        return (self.entries_len as usize) / (self.entry_width as usize);
    }
}

/// CAR v2 Index reader, specialized for the IndexSorted type (0x0400)
/// 
/// This reader is designed to read the IndexSorted type of index from a CAR v2 file.
/// It can receive data in chunks, and will parse the index structure as it receives data.
/// 
/// If you want to read an index from its separate file, you can use the [OwnedIndexReader]
/// which is a convenient wrapper around `IndexReader` that owns its own data buffer and offset.
#[derive(Debug)]
pub struct IndexReader<'a, S: IndexSortedReaderState> {
    data: Borrowable<'a, DataCursor>,
    state: S,
}

/// Marker trait for the state of an IndexReader specialized for IndexSorted type.
/// 
/// This trait is used to differentiate between the initial state of the reader 
/// (before opening the index) and the ready state (after opening the index).
pub trait IndexSortedReaderState: Sealed {}

/// Initial state of the IndexReader, before opening the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InitReaderState(());

impl Sealed for InitReaderState {}
impl IndexSortedReaderState for InitReaderState {}

impl<'a, S: IndexSortedReaderState> IndexReader<'a, S> {
    pub fn into_parts(self) -> (Borrowable<'a, DataCursor>, S) {
        (self.data, self.state)
    }

    pub fn from_parts(data: Borrowable<'a, DataCursor>, state: S) -> Self {
        Self { data, state }
    }
}

impl<S> IndexRead for IndexReader<'_, S> 
where S: IndexSortedReaderState
{
    fn receive_data(&mut self, buf: &[u8], offset: usize) {
        self.data.ingest(buf, offset);
    }
}

impl<'a> IndexReader<'a, InitReaderState> {
    /// Creates a new IndexReader, ready to absorbe and parse the IndexSorted data from a CAR v2 file.
    pub fn new(data: impl Into<Borrowable<'a, DataCursor>>) -> Self {
        Self {
            data: data.into(),
            state: InitReaderState(()),
        }
    }

    /// Open the index.
    ///
    /// This step is essential! It will confirm that the index is indeed of the expected type (IndexSorted)
    /// and will prepare the reader to parse the index data. After that, you will be able to read all the entries.
    ///
    /// # Returns
    /// * `Ok(IndexReader<ReadyReaderState>)` if the index is valid and ready to be read
    /// * `Err((IndexReader<InitReaderState>, IndexReaderOpenError))` if the index is not ready to be read, or invalid.
    ///   In particular, you might need to retry after providing more data, if the initial chunk of data was not enough to
    ///   determine the index type.
    pub fn open(mut self) -> Result<IndexReader<'a, ReadyReaderState>, (Self, IndexReaderOpenError)> {
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

        // Check if the index type is IndexSorted (0x0400)
        if index_type.0 != 0x0400 {
            // If the index type is not IndexSorted, it is invalid for this reader
            return Err((self, IndexReaderOpenError::IndexTypeMismatch));
        }

        // Parse the number of buckets
        if self.data.len() < index_type_len + 4 {
            // We don't have enough data to parse the number of buckets, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        let bucket_count = u32::from_le_bytes([
            self.data[index_type_len],
            self.data[index_type_len + 1],
            self.data[index_type_len + 2],
            self.data[index_type_len + 3],
        ]);

        // Consume the data read (index type + bucket count)
        self.data.consume(index_type_len + 4);
        let next_bucket_offset = self.data.get_offset();

        Ok(IndexReader {
            data: self.data,
            state: ReadyReaderState {
                bucket_count,
                buckets_read: 0,
                first_bucket_offset: next_bucket_offset,
                next_bucket_offset,
            },
        })
    }
}

#[derive(thiserror::Error, Debug)]
/// Errors that can occur when trying to open a new index.
pub enum IndexReaderOpenError {
    /// Not enough data has been received to determine the index type, we need to receive more
    /// data before we can open the index reader.
    #[error("Not enough data to determine index type, need to receive more data")]
    InsufficientData,
    /// The index type is invalid or it does not match the expected type for this reader.
    #[error("Index type mismatch (either invalid, or not the adequate type for this reader)")]
    IndexTypeMismatch,
}

/// State of the IndexReader after successfully opening the index and confirming it is of the expected type (IndexSorted).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadyReaderState {
    bucket_count: u32,
    buckets_read: u32,
    first_bucket_offset: usize,
    next_bucket_offset: usize
}

impl Sealed for ReadyReaderState {}
impl IndexSortedReaderState for ReadyReaderState {}

impl<'a> IndexReader<'a, ReadyReaderState> {
    pub fn rewind(&mut self) {
        // Reset the reader to the state it was in right after opening the index, 
        // allowing to read the buckets again from the beginning.
        self.state.buckets_read = 0;
        self.state.next_bucket_offset = self.state.first_bucket_offset;
    }

    pub fn count_buckets(&self) -> usize {
        self.state.bucket_count as usize
    }

    pub(crate) fn new_state_with(bucket_count: u32, next_bucket_offset: usize) -> ReadyReaderState {
        ReadyReaderState {
            bucket_count,
            buckets_read: 0,
            first_bucket_offset: next_bucket_offset,
            next_bucket_offset,
        }
    }

    pub(crate) fn new_with<D: Into<Borrowable<'a, DataCursor>>>(data: D, bucket_count: u32) -> Self {
        let data = data.into();
        let next_bucket_offset = data.get_offset();
        IndexReader {
            data,
            state: Self::new_state_with(bucket_count, next_bucket_offset),
        }
    }

    /// Reads the header of the next bucket from the index data, if available.
    /// 
    /// # Returns
    /// * `Ok(IndexSortedBucketHeader)` if the bucket header is successfully read,
    /// * `Err(IndexReaderReadError)` if there is an error while reading the bucket header (e.g., not enough data, invalid format, end of index).
    /// Note that if there are no more buckets to read (end of index), it will return an `Err(IndexReaderReadError::EndOfIndex)`.
    fn read_next_bucket_header(&mut self) -> Result<IndexSortedBucketHeader, IndexReaderReadError> {
        if self.state.buckets_read >= self.state.bucket_count {
            // We have read all the buckets, there are no more to read
            return Err(IndexReaderReadError::EndOfIndex);
        }

        self.data.seek(self.state.next_bucket_offset);

        if self.data.len() < 12 {
            // We don't have enough data to read the bucket header, we need to receive more data
            return Err(IndexReaderReadError::InsufficientData(self.data.len(), 12));
        }

        let entry_width = u32::from_le_bytes(self.data[0..4].try_into().unwrap());
        let entries_len = u64::from_le_bytes(self.data[4..12].try_into().unwrap());

        // Consume the data read
        self.data.consume(12);
        self.state.buckets_read += 1;
        self.state.next_bucket_offset = self.data.get_offset() + (entries_len as usize);

        Ok(IndexSortedBucketHeader {
            entry_width,
            entries_len,
        })
    }

    /// Reads the next bucket from the index data, if available.
    ///
    /// # Returns
    /// * `Ok(IndexSortedBucketReader)` if a bucket is successfully read,
    /// * `Err(IndexReaderReadError)` if there is an error while reading the bucket (e.g., not enough data, invalid format, end of index).
    pub fn read_next_bucket(&mut self) -> Result<IndexSortedBucketReader<'_>, IndexReaderReadError> {
        let header = self.read_next_bucket_header()?;
        let bucket_first_entry_offset = self.data.get_offset();

        Ok(IndexSortedBucketReader {
            data: self.data.as_borrowed(),
            header,
            bucket_first_entry_offset,
        })
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

/// Reader for a single bucket of an IndexSorted index.
pub struct IndexSortedBucketReader<'a> {
    data: Borrowable<'a, DataCursor>,
    header: IndexSortedBucketHeader,
    bucket_first_entry_offset: usize,
}

impl IndexRead for IndexSortedBucketReader<'_> {
    fn receive_data(&mut self, buf: &[u8], offset: usize) {
        self.data.ingest(buf, offset);
    }
}

impl<'a> IndexSortedBucketReader<'a> {
    /// Exhaust the bucket reader, consuming all the data of the bucket and purging it from 
    /// the reader's buffer.
    /// 
    /// This can be useful if you want to skip the remaining entries of the bucket.
    /// In particular, when you are looking for a specific hash, and that bucket does not contain it. 
    pub fn exhaust(&mut self) {
        let bucket_end_offset = self.bucket_first_entry_offset + self.header.get_entries_len();
        self.data.seek(bucket_end_offset);
    }


    /// Get the number of entries inside the bucket
    pub fn count_entries(&self) -> usize {
        return self.header.count_entries();
    }

    /// Read the n-th entry of the bucket, if available.
    /// 
    /// # Returns
    /// * `Ok(IndexEntry)` if the entry is successfully read,
    /// * `Err(IndexSortedBucketReadError)` if there is an error while reading the entry (e.g., not enough data, invalid format, end of bucket).
    pub fn read_entry(&mut self, n: usize) -> Result<IndexEntry, IndexSortedBucketReadError> {
        if n >= self.count_entries() {
            // The requested entry index is out of range, there are not that many entries in the bucket
            return Err(IndexSortedBucketReadError::OutOfBucket);
        }

        let entry_width = self.header.get_entry_width();
        let entry_start = self.bucket_first_entry_offset + (n * entry_width);

        self.data.seek(entry_start);

        if self.data.len() < entry_width {
            // We don't have enough data to read an entry, we need to receive more data
            return Err(IndexSortedBucketReadError::InsufficientData(self.data.get_end_offset(), entry_width - self.data.len()));
        }

        let hash_width = entry_width - 8; // The last 8 bytes are for the offset
        let hash = self.data[0..hash_width].to_vec();
        let offset = u64::from_le_bytes(self.data[hash_width..entry_width].try_into().unwrap());

        // Consume the data read
        self.data.consume(entry_width);

        Ok(IndexEntry { hash, offset })
    }

    pub fn find<'b>(&'b mut self, hash: &'b [u8]) -> IndexSortedBucketFinder<'a, 'b> {
        IndexSortedBucketFinder::new(self, hash)
    }
}

pub struct IndexSortedBucketFinder<'a, 'b> {
    reader: &'b mut IndexSortedBucketReader<'a>,
    hash: &'b[u8],
    left: usize, // left (included), start of search range
    right: usize, // right (excluded), end  of search range
}

impl<'a, 'b> IndexSortedBucketFinder<'a, 'b> {
    pub fn new(reader: &'b mut IndexSortedBucketReader<'a>, hash: &'b [u8]) -> Self {
        // Early check that the hash len matches the bucket
        if reader.header.get_hash_width() != hash.len() {
            return Self { reader, hash, left: 0, right: 0 }
        } 

        let right  = reader.count_entries();
        Self { reader, hash, left: 0, right }
    }

    pub fn find(&mut self) -> Result<Option<IndexEntry>, IndexSortedBucketReadError> {
        // If we have still some entries to look for
        while self.left < self.right {
            // Binary search
            let middle = (self.left + self.right) / 2;
            let middle_entry = self.reader.read_entry(middle)?;
            match self.hash.cmp(&middle_entry.hash) {
                Ordering::Equal => {
                    // Reduce the search window to this only element, such that
                    // future calls finds that item immediately.
                    self.left = middle;
                    self.right = middle + 1;
                    return Ok(Some(middle_entry));
                }
                Ordering::Less => {
                    // Hash is in the left subwindow
                    self.right = middle;
                }
                Ordering::Greater => {
                    // Hash is in the right subwindow
                    self.left = middle + 1;
                }
            }
        }
        Ok(None) // Item is not present
    }
}

impl IndexRead for IndexSortedBucketFinder<'_, '_> {
    fn receive_data(&mut self, buf: &[u8], offset: usize) {
        self.reader.receive_data(buf, offset);
    }
}

/// Errors that can occur while reading entries from an IndexSorted bucket.
#[derive(thiserror::Error, Debug)]
pub enum IndexSortedBucketReadError {
    /// Not enough data has been received to correctly read the next bucket entry,
    /// we need to receive more data before we can continue reading.
    #[error("Not enough data to read the next bucket entry, need to receive more data")]
    InsufficientData(usize, usize), // (current data length, expected data length)
    /// Out of range entry index, the requested entry index is greater than or equal to the number of entries in the bucket.
    #[error("Out of range entry index, the requested entry index is greater than or equal to the number of entries in the bucket")]
    OutOfBucket,
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW_SORTED_INDEX: [u8; 138] = [
        // Index type (LEB128 varint) - 0x0400 (IndexSorted)
        0x80, 0x08, // Count of IndexSorted buckets (4 bytes) - 1
        0x01, 0x00, 0x00, 0x00,
        // Width (4 bytes) - 40 bytes (32 bytes hash + 8 bytes offset) -> 0x28
        0x28, 0x00, 0x00, 0x00,
        // Cumulative length of all entries in this bucket (8 bytes)
        0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // First entry
        // Hash (32 bytes)
        0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
        0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
        0x29, 0x4D, 
        // Offset (8 bytes)
        0xB7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Second entry
        // Hash (32 bytes)
        0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
        0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
        0x07, 0x2D, 
        // Offset (8 bytes)
        0x8E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // Third entry
        // Hash (32 bytes)
        0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
        0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
        0x5F, 0xF5, 
        // Offset (8 bytes)
        0xE3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    pub fn test_index_sorted_reader() {
        let mut reader = IndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_SORTED_INDEX.clone(), 0);

        let mut reader = match reader.open() {
            Ok(reader) => reader,
            Err((_, err)) => panic!("Failed to open index reader: {}", err),
        };

        let mut bucket = match reader.read_next_bucket() {
            Ok(bucket) => bucket,
            Err(err) => panic!("Failed to read next bucket: {}", err),
        };

        assert_eq!(bucket.count_entries(), 3);

        let entry1 = bucket.read_entry(0).expect("Failed to read entry 1");
        assert_eq!(entry1.hash, vec![
            0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
            0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
            0x29, 0x4D, 
        ]);
        assert_eq!(entry1.offset, 183);

        let entry2 = bucket.read_entry(1).expect("Failed to read entry 2");
        assert_eq!(entry2.hash, vec![
            0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
            0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
            0x07, 0x2D, 
        ]);
        assert_eq!(entry2.offset, 142);

        let entry3 = bucket.read_entry(2).expect("Failed to read entry 3");
        assert_eq!(entry3.hash, vec![
            0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
            0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
            0x5F, 0xF5, 
        ]);
        assert_eq!(entry3.offset, 227);
    }

    #[test]
    pub fn test_index_sorted_reader_invalid_index_type() {
        let mut reader = IndexReader::new(DataCursor::new());
        // Provide an invalid index type (e.g., 0x0500 instead of 0x0400)
        reader.receive_data(&mut vec![0x80, 0x0A], 0);

        match reader.open() {
            Ok(_) => panic!("Expected to fail opening index reader due to invalid index type, but it succeeded"),
            Err((_, IndexReaderOpenError::IndexTypeMismatch)) => { /* Expected error */ }
            Err((_, err)) => panic!("Expected to fail opening index reader due to invalid index type, but got a different error: {}", err),
        }
    }

    #[test]
    pub fn test_index_sorted_bucket_reader_out_of_order() {
        let mut reader = IndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_SORTED_INDEX.clone(), 0);

        let mut reader = match reader.open() {
            Ok(reader) => reader,
            Err((_, err)) => panic!("Failed to open index reader: {}", err),
        };

        let mut bucket = match reader.read_next_bucket() {
            Ok(bucket) => bucket,
            Err(err) => panic!("Failed to read next bucket: {}", err),
        };

        assert_eq!(bucket.count_entries(), 3);

        fn read_entry(bucket: &mut IndexSortedBucketReader, n: usize) -> IndexEntry {
            loop {
                match bucket.read_entry(n) {
                    Ok(entry) => return entry,
                    Err(IndexSortedBucketReadError::InsufficientData(offset, size )) => {
                        // We need to provide more data
                        let end = offset + size;
                        if end > RAW_SORTED_INDEX.len() {
                            panic!("Not enough data, got hint: start {}, end {}, size {}, but total data length is {}", offset, end, size, RAW_SORTED_INDEX.len());
                        }
                        bucket.receive_data(&mut RAW_SORTED_INDEX[offset..end].to_vec(), offset);
                    }
                    Err(err) => panic!("Failed to read entry 3: {}", err),
                }
            }
        }

        // Ensure the entry all matches the expected values, 
        // even if we had to provide the data in a non-contiguous way
        let entry3 = read_entry(&mut bucket, 2);
        assert_eq!(entry3.hash, vec![
            0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
            0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
            0x5F, 0xF5,
        ]);
        assert_eq!(entry3.offset, 227);

        let entry1 = read_entry(&mut bucket, 0);
        assert_eq!(entry1.hash, vec![
            0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
            0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
            0x29, 0x4D, 
        ]);
        assert_eq!(entry1.offset, 183);

        let entry2 = read_entry(&mut bucket, 1);
        assert_eq!(entry2.hash, vec![
            0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
            0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
            0x07, 0x2D, 
        ]);
        assert_eq!(entry2.offset, 142);
    }

    #[test]
    pub fn test_index_sorted_bucket_finder_find_all() {
        let mut reader = IndexReader::new(DataCursor::new());
        reader.receive_data(&mut RAW_SORTED_INDEX[0..18], 0);

        let mut reader = match reader.open() {
            Ok(reader) => reader,
            Err((_, err)) => panic!("Failed to open index reader: {}", err),
        };

        let mut bucket = match reader.read_next_bucket() {
            Ok(bucket) => bucket,
            Err(err) => panic!("Failed to read next bucket: {}", err),
        };

        assert_eq!(bucket.count_entries(), 3);

        fn find_entry(bucket: &mut IndexSortedBucketReader, hash: &[u8]) -> Option<IndexEntry> {
            println!("looking for hash: {:x?}", hash);
            let mut finder = bucket.find(hash);
            loop {
                match finder.find() {
                    Ok(entry) => {
                        return entry;
                    },
                    Err(IndexSortedBucketReadError::InsufficientData(offset, size )) => {
                        println!("read > start: {}, end: {}, size: {}", offset, offset+size, size);
                        if offset + size > RAW_SORTED_INDEX.len() {
                            panic!("Unable to provide data for range: start: {}, end: {}, size: {} of index data (total size: {})", offset, offset + size, size, RAW_SORTED_INDEX.len());
                        }
                        finder.receive_data(&RAW_SORTED_INDEX[offset..(offset+size)], offset);
                    }
                    Err(err) => { panic!("Unexpected error: {}", err) }
                }
            }
        }

        let hash1 = &RAW_SORTED_INDEX[18..18 + 32];
        let hash2 = &RAW_SORTED_INDEX[58..58 + 32];
        let hash3 = &RAW_SORTED_INDEX[98..98 + 32];
        let hash_unknown = &RAW_SORTED_INDEX[0..32];
        let hash_size_mismatch = &RAW_SORTED_INDEX[0..20];

        let entry1 = find_entry(&mut bucket, hash1).unwrap();
        assert_eq!(entry1.hash, hash1);
        assert_eq!(entry1.offset, 183);

        let entry2 = find_entry(&mut bucket, hash2).unwrap();
        assert_eq!(entry2.hash, hash2);
        assert_eq!(entry2.offset, 142);

        let entry3 = find_entry(&mut bucket, hash3).unwrap();
        assert_eq!(entry3.hash, hash3);
        assert_eq!(entry3.offset, 227);

        let entry_hash_unknown = find_entry(&mut bucket, hash_unknown);
        assert!(entry_hash_unknown.is_none());

        let entry_hash_size_mismatch = find_entry(&mut bucket, hash_size_mismatch);
        assert!(entry_hash_size_mismatch.is_none());

        panic!();
    }
}
