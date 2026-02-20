use crate::wire::cid::RawCid;
use crate::wire::v1::{CarHeader, Section, SectionLocation};
use crate::wire::varint::UnsignedVarint;

/// CAR v1 writer
///
/// This struct provides functionality to write CAR v1 files, in a sans-io manner
#[derive(Debug, Clone)]
pub struct CarWriter {
    /// Temporary write buffer for accumulating section data before writing to the underlying sink
    data: Vec<u8>,
    /// Current offset in the output stream (used for calculating section locations)
    ///
    /// The offset does not take into account the current data buffer, which is only flushed to the underlying sink when `flush` is called.
    offset: u64,
}

impl CarWriter {
    /// Internal method to write the header to the data buffer
    fn write_header(&mut self, header: CarHeader) {
        // Serialize the header using CBOR and write it to the data buffer
        ciborium::ser::into_writer(&header, &mut self.data)
            .expect("Failed to serialize CAR header -- it is a bug if this happens");
        // The header is prefixed by a varint-encoded length, so we need to insert that at the beginning of the data buffer
        let header_length = self.data.len() as u64;
        let header_length_varint = UnsignedVarint(header_length).encode();
        self.data.splice(0..0, header_length_varint);
    }
}

impl CarWriter {
    /// Create a new CarWriter with the specified roots.
    ///
    /// The roots are the CIDs of the root nodes of the DAG represented in the CAR file.
    /// They are included in the header and can be used by readers to identify the entry points of the DAG.
    ///
    /// It is expected that blocks which correspond to the root CIDs are written inside the CAR file, but this
    /// is not enforced by the CarWriter itself. It is the responsibility of the caller to ensure that the root
    /// CIDs are included in the CAR file's blocks.
    ///
    /// **Note:** This method will create a CAR writer with a maximal internal buffer size of 16 MiB.
    /// It should be enough to write all CAR file using standard block sizes (<= 2 MiB), but you MUST flush
    /// regularly to avoid running out of memory. If you need to write larger CAR files, you can create a
    /// [CarWriter] with a custom buffer size using the [CarWriter::with_buffer_size] method.
    pub fn new(roots: Vec<RawCid>) -> Self {
        Self::with_buffer_size(roots, 16 * 1024 * 1024)
    }

    /// Create a new CarWriter with the specified roots and a custom internal buffer size.
    ///
    /// The buffer size determines how much data can be accumulated in memory before it needs to be
    /// flushed to the underlying sink. A larger buffer size can improve performance by reducing the number
    /// of flushes, but it also increases memory usage.
    ///
    /// Reasonably, the buffer size should be larger than the size of the biggest section you expect to write (CID length + block size + a bit more),
    /// otherwise you might run into issues where a single section cannot fit in the buffer and causes an error.
    /// You should not go below 256 bytes for the buffer size, as the header itself can be around that size depending on the number of roots.
    ///
    /// See [CarWriter::new] for more details on the expected usage of the CarWriter and the roots.
    pub fn with_buffer_size(roots: Vec<RawCid>, buffer_size: usize) -> Self {
        debug_assert!(
            buffer_size > 256,
            "Buffer size must be greater than 256 bytes to accommodate the header"
        );
        let mut writer = Self {
            data: Vec::with_capacity(buffer_size),
            offset: 0,
        };
        writer.write_header(CarHeader::new(roots));
        writer
    }

    /// Write a section to the CAR stream.
    ///
    /// This method will serialize the section and append it to the current CAR stream.
    /// However, it does not actually write to the underlying sink until `send_data` is called.
    pub fn write_section(&mut self, section: &Section) -> Result<SectionLocation, CarWriterError> {
        let data_pos = self.data.len();
        let section_size = section.total_length();
        if data_pos + section_size > self.data.capacity() {
            return Err(CarWriterError::BufferFull);
        }
        let section_bytes = section.to_bytes();
        self.data.extend_from_slice(&section_bytes);
        let section_location = SectionLocation {
            offset: self.offset + data_pos as u64,
            length: section_bytes.len() as u64,
        };
        Ok(section_location)
    }

    /// Flush the current data buffer and return the bytes to be written to the underlying sink.
    ///
    /// The caller should write these bytes to the underlying sink and then call `send_data` again
    /// to flush more data if needed. If 0 bytes are written, it means that there is no more data to flush at the moment.
    ///
    /// ## Arguments
    ///
    /// * `buf` - A mutable byte slice to which the data will be written.
    ///
    /// ## Returns
    ///
    /// The number of bytes written to the buffer.
    pub fn send_data(&mut self, buf: &mut [u8]) -> usize {
        let bytes_to_send = self.data.len().min(buf.len());
        buf[..bytes_to_send].copy_from_slice(&self.data[..bytes_to_send]);
        self.data.drain(..bytes_to_send);
        self.offset += bytes_to_send as u64;
        bytes_to_send
    }

    /// Check if there is data ready to be sent to the underlying sink.
    ///
    /// This can be used by the caller to determine when to call `send_data` to flush the data buffer.
    pub fn has_data_to_send(&self) -> bool {
        !self.data.is_empty()
    }
}

/// Errors related to CarWriter operations
#[derive(thiserror::Error, Debug)]
pub enum CarWriterError {
    /// Buffer is full and cannot accommodate the new section
    ///
    /// This error occurs when trying to write a section that exceeds the remaining capacity of the internal buffer.
    /// To resolve this, you can either flush the current buffer to the underlying sink to free up space or increase the buffer size when creating the CarWriter.
    #[error("Buffer is full, cannot write section")]
    BufferFull,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::v1::Block;

    #[test]
    fn test_car_writer() {
        let root_cid = RawCid::from_hex(
            "015512200000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let cid2 = RawCid::from_hex(
            "01551220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let cid3 = RawCid::from_hex(
            "01551220ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )
        .unwrap();
        let first_block = Block::new(vec![1, 2, 3, 4]);
        let second_block = Block::new(vec![5, 6, 7, 8]);
        let third_block = Block::new(vec![9, 10, 11, 12]);
        let section1 = Section::new(root_cid.clone(), first_block);
        let section2 = Section::new(cid2, second_block);
        let section3 = Section::new(cid3, third_block);

        let mut writer = CarWriter::new(vec![root_cid]);
        let mut sink = Vec::new();
        let mut buf = [0u8; 32];
        let mut section_to_write = vec![section1, section2, section3];
        loop {
            // Write bytes to sink until there are no more sections to write and no more data to flush
            let written = writer.send_data(&mut buf);
            if written > 0 {
                sink.extend_from_slice(&buf[..written]);
            } else if section_to_write.is_empty() {
                break;
            }

            // If there are still sections to write, try to write the next one
            if let Some(section) = section_to_write.pop() {
                match writer.write_section(&section) {
                    Ok(location) => println!("Section written at location: {:?}", location),
                    Err(CarWriterError::BufferFull) => {
                        // Buffer is full, we need to flush before writing the next section
                        section_to_write.push(section); // Put the section back to try writing it again after flushing
                        continue;
                    }
                }
            }
        }
        println!("Final CAR data: {:?}", hex::encode(&sink));
        assert_eq!(sink.len(), 182);
    }

    // TODO: Tests writer and reader match, by writing a CAR file with the writer and then reading 
    // it with the reader and checking that the header and sections are the same.
}
