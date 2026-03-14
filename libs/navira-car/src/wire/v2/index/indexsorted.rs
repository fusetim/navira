use super::IndexRead;
use crate::types::Sealed;

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
#[derive(Debug, Clone)]
pub struct IndexReader<S: IndexSortedReaderState> {
    state: S,
}

/// Marker trait for the state of an IndexReader specialized for IndexSorted type.
/// 
/// This trait is used to differentiate between the initial state of the reader 
/// (before opening the index) and the ready state (after opening the index).
pub trait IndexSortedReaderState: Sealed {}

/// Initial state of the IndexReader, before opening the index.
pub struct InitReaderState<'a> {
    data: &'a mut Vec<u8>,
    offset: &'a mut usize,
}

impl Sealed for InitReaderState<'_> {}
impl IndexSortedReaderState for InitReaderState<'_> {}

/// Internal function to handle receiving data for the index reader, managing the state of the received data and offset.
pub(crate) fn inner_receive_data(state_data: &mut Vec<u8>, state_offset: &mut usize, buf: &mut [u8], offset: usize) {
    // If offset is not equal to the current length of data, it means we are
    // receiving a chunk of data that is not contiguous with what we have already received.
    if offset != *state_offset + state_data.len() {
        // Purge the existing data, as it is not contiguous with the new data we are receiving.
        state_data.clear();
        *state_offset = offset;
        state_data.extend_from_slice(buf);
    } else {
        // If the offset is contiguous with the current data, we simply append the new data to it.
        state_data.extend_from_slice(buf);
    }
}

impl IndexRead for IndexReader<InitReaderState<'_>> {
    fn receive_data(&mut self, buf: &mut [u8], offset: usize) {
        inner_receive_data(&mut self.state.data, &mut self.state.offset, buf, offset);
    }
}

impl<'a> IndexReader<InitReaderState<'a>> {
    /// Creates a new IndexReader, ready to absorbe and parse the IndexSorted data from a CAR v2 file.
    pub fn new(data: &'a mut Vec<u8>, offset: &'a mut usize) -> Self {
        Self {
            state: InitReaderState {
                data,
                offset,
            },
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
    pub fn open(self) -> Result<IndexReader<ReadyReaderState<'a>>, (Self, IndexReaderOpenError)> {
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

        // Check if the index type is IndexSorted (0x0400)
        if index_type.0 != 0x0400 {
            // If the index type is not IndexSorted, it is invalid for this reader
            return Err((self, IndexReaderOpenError::IndexTypeMismatch));
        }

        // Parse the number of buckets
        if self.state.data.len() < index_type_len + 4 {
            // We don't have enough data to parse the number of buckets, we need to receive more data
            return Err((self, IndexReaderOpenError::InsufficientData));
        }

        let bucket_count = u32::from_le_bytes([
            self.state.data[index_type_len],
            self.state.data[index_type_len + 1],
            self.state.data[index_type_len + 2],
            self.state.data[index_type_len + 3],
        ]);

        // Consume the data read (index type + bucket count)
        self.state.data.drain(0..index_type_len + 4);
        *self.state.offset += index_type_len + 4;
        let next_bucket_offset = *self.state.offset;

        Ok(IndexReader {
            state: ReadyReaderState {
                data: self.state.data,
                offset: self.state.offset,
                bucket_count,
                buckets_read: 0,
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
pub struct ReadyReaderState<'a> {
    data: &'a mut Vec<u8>,
    offset: &'a mut usize,
    bucket_count: u32,
    buckets_read: u32,
    next_bucket_offset: usize
}

impl Sealed for ReadyReaderState<'_> {}
impl IndexSortedReaderState for ReadyReaderState<'_> {}

impl IndexRead for IndexReader<ReadyReaderState<'_>> {
    fn receive_data(&mut self, buf: &mut [u8], offset: usize) {
        inner_receive_data(&mut self.state.data, &mut self.state.offset, buf, offset);
    }
}

impl<'a> IndexReader<ReadyReaderState<'a>> {
    pub fn count(&self) -> usize {
        self.state.bucket_count as usize
    }

    pub(crate) fn bring_down(self) -> (&'a mut Vec<u8>, &'a mut usize) {
        (self.state.data, self.state.offset)
    }

    pub(crate) fn new(data: &'a mut Vec<u8>, offset: &'a mut usize, bucket_count: u32) -> Self {
        let next_bucket_offset = *offset;
        Self {
            state: ReadyReaderState {
                data,
                offset,
                bucket_count,
                buckets_read: 0,
                next_bucket_offset,
            },
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

        if *self.state.offset < self.state.next_bucket_offset {
            let moved = self.state.next_bucket_offset - *self.state.offset;
            *self.state.offset += moved;
            if moved > self.state.data.len() {
                // We don't have enough data to reach the next bucket, we need to receive more data
                self.state.data.clear();
                return Err(IndexReaderReadError::InsufficientData(*self.state.offset, 12));
            } else {
                // We have enough data to reach the next bucket, we can move the offset 
                // and purge the data we have already read
                self.state.data.drain(0..moved);
            }
        }

        if self.state.data.len() < 12 {
            // We don't have enough data to read the bucket header, we need to receive more data
            return Err(IndexReaderReadError::InsufficientData(self.state.data.len(), 12));
        }

        let entry_width = u32::from_le_bytes(self.state.data[0..4].try_into().unwrap());
        let entries_len = u64::from_le_bytes(self.state.data[4..12].try_into().unwrap());

        // Consume the data read
        *self.state.offset += 12;
        self.state.data.drain(0..12);
        self.state.buckets_read += 1;
        self.state.next_bucket_offset = *self.state.offset + (entries_len as usize);

        Ok(IndexSortedBucketHeader {
            entry_width,
            entries_len,
        })
    }

    /// Reads the next bucket from the index data, if available.
    ///
    /// # Returns
    /// * `Ok(Some(IndexSortedBucketReader))` if a bucket is successfully read,
    /// * `Ok(None)` if there are no more buckets to read (end of index).
    /// * `Err(IndexReaderReadError)` if there is an error while reading the bucket (e.g., not enough data, invalid format).
    pub fn read_next_bucket(&mut self) -> Result<Option<IndexSortedBucketReader<'_>>, IndexReaderReadError> {
        let header = self.read_next_bucket_header()?;
        let bucket_first_entry_offset = *self.state.offset;

        Ok(Some(IndexSortedBucketReader {
            data: &mut self.state.data,
            offset: &mut self.state.offset,
            header,
            bucket_first_entry_offset,
        }))
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
    data: &'a mut Vec<u8>,
    offset: &'a mut usize,
    header: IndexSortedBucketHeader,
    bucket_first_entry_offset: usize,
}

impl IndexRead for IndexSortedBucketReader<'_> {
    fn receive_data(&mut self, buf: &mut [u8], offset: usize) {
        inner_receive_data(self.data, self.offset, buf, offset);
    }
}

impl<'a> IndexSortedBucketReader<'a> {
    /// Get the number of entries inside the bucket
    pub fn count(&self) -> usize {
        return self.header.count_entries();
    }

    /// Read the n-th entry of the bucket, if available.
    /// 
    /// # Returns
    /// * `Ok(IndexEntry)` if the entry is successfully read,
    /// * `Err(IndexSortedBucketReadError)` if there is an error while reading the entry (e.g., not enough data, invalid format, end of bucket).
    pub fn read_entry(&mut self, n: usize) -> Result<IndexEntry, IndexSortedBucketReadError> {
        if n >= self.count() {
            // The requested entry index is out of range, there are not that many entries in the bucket
            return Err(IndexSortedBucketReadError::OutOfBucket);
        }

        let entry_width = self.header.get_entry_width();
        let entry_offset = self.bucket_first_entry_offset + (n * entry_width);

        if *self.offset < entry_offset {
            let moved = entry_offset - *self.offset;
            *self.offset += moved;
            if moved > self.data.len() {
                // We don't have enough data to reach the next entry, we need to receive more data
                self.data.clear();
                return Err(IndexSortedBucketReadError::InsufficientData(*self.offset, entry_offset));
            } else {
                // We have enough data to reach the next entry, we can move the offset 
                // and purge the data we have already read
                self.data.drain(0..moved);
            }
        }

        if self.data.len() < entry_width {
            // We don't have enough data to read an entry, we need to receive more data
            return Err(IndexSortedBucketReadError::InsufficientData(*self.offset, entry_width));
        }

        let hash_width = entry_width - 8; // The last 8 bytes are for the offset
        let hash = self.data[0..hash_width].to_vec();
        let offset = u64::from_le_bytes(self.data[hash_width..entry_width].try_into().unwrap());

        // Consume the data read
        *self.offset += entry_width;
        self.data.drain(0..entry_width);

        Ok(IndexEntry { hash, offset })
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

/// Convenient wrapper around [IndexReader] that owns its own data buffer and offset, 
/// allowing you to read an index from its separate file (or buffer) without having to
/// manage the data buffer and offset yourself.
pub struct OwnedIndexReader {
    data: Vec<u8>,
    offset: usize,
}

impl OwnedIndexReader {
    /// Creates a new OwnedIndexReader with an empty data buffer, ready to receive index data.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
        }
    }

    /// Open the index reader, confirming that the received data is of the expected type (IndexSorted) 
    /// and preparing it to read the index entries.
    /// 
    /// # Returns
    /// 
    /// * `Ok(IndexReader<ReadyReaderState>)` if the index is valid and ready to be read
    /// * `Err(IndexReaderOpenError)` if the index is not ready to be read, or invalid.
    pub fn open<'a>(&'a mut self) -> Result<IndexReader<ReadyReaderState<'a>>, IndexReaderOpenError> {
        if let Ok(reader) = IndexReader::<InitReaderState<'a>>::new(&mut self.data, &mut self.offset).open() {
            Ok(reader)
        } else {
            Err(IndexReaderOpenError::InsufficientData)
        }
    }
}

impl IndexRead for OwnedIndexReader {
    fn receive_data(&mut self, buf: &mut [u8], offset: usize) {
        inner_receive_data(&mut self.data, &mut self.offset, buf, offset);
    }
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
        let mut reader = OwnedIndexReader::new();
        reader.receive_data(&mut RAW_SORTED_INDEX.clone(), 0);

        let mut reader = match reader.open() {
            Ok(reader) => reader,
            Err(err) => panic!("Failed to open index reader: {}", err),
        };

        let mut bucket = match reader.read_next_bucket() {
            Ok(Some(bucket)) => bucket,
            Ok(None) => panic!("No buckets found in index"),
            Err(err) => panic!("Failed to read next bucket: {}", err),
        };

        assert_eq!(bucket.count(), 3);

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
    pub fn test_index_sorted_reader_insufficient_data() {
        let mut reader = OwnedIndexReader::new();
        reader.receive_data(&mut RAW_SORTED_INDEX[0..10].to_vec(), 0);

        match reader.open() {
            Ok(_) => panic!("Expected to fail opening index reader due to insufficient data, but it succeeded"),
            Err(err) => assert_eq!(err.to_string(), "Not enough data to determine index type, need to receive more data"),
        }
    }

    #[test]
    pub fn test_index_sorted_reader_invalid_index_type() {
        let mut reader = OwnedIndexReader::new();
        // Provide an invalid index type (e.g., 0x0500 instead of 0x0400)
        reader.receive_data(&mut vec![0x80, 0x0A], 0);

        match reader.open() {
            Ok(_) => panic!("Expected to fail opening index reader due to invalid index type, but it succeeded"),
            Err( err) => assert_eq!(err.to_string(), "Invalid index type, expected IndexSorted (0x0400)"),
        }
    }

    #[test]
    pub fn test_index_sorted_bucket_reader_out_of_order() {
        let mut reader = OwnedIndexReader::new();
        reader.receive_data(&mut RAW_SORTED_INDEX.clone(), 0);

        let mut reader = match reader.open() {
            Ok(reader) => reader,
            Err( err) => panic!("Failed to open index reader: {}", err),
        };

        let mut bucket = match reader.read_next_bucket() {
            Ok(Some(bucket)) => bucket,
            Ok(None) => panic!("No buckets found in index"),
            Err(err) => panic!("Failed to read next bucket: {}", err),
        };

        assert_eq!(bucket.count(), 3);

        let entry3 = bucket.read_entry(2).expect("Failed to read entry 3");
        assert_eq!(entry3.hash, vec![
            0xDD, 0x5D, 0x63, 0xC5, 0xF8, 0x3C, 0x2C, 0x77, 0x46, 0xF2, 0xF5, 0xC9, 0x31, 0x3D, 0xC8,
            0x44, 0xA9, 0xA5, 0x04, 0x9A, 0x27, 0x5B, 0x6D, 0x7B, 0x6A, 0x8D, 0xB0, 0x5B, 0xD8, 0xBB,
            0x5F, 0xF5, 
        ]);
        assert_eq!(entry3.offset, 227);

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
    }
}
