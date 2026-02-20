use std::io::Write;

use crate::wire::{cid::RawCid, v1, v2::{CAR_V2_PRAGMA, CarV2Header, Characteristics, Section, SectionLocation}};
use crate::types::Sealed;

/// CAR v2 writer
///
/// This struct provides functionality to write CAR v2 files, in a sans-io manner
#[derive(Debug, Clone)]
pub struct CarWriter<S: CarWriteV2State> {
    state: S,
}
pub trait CarWriteV2State: Sealed {}

#[derive(Debug, Clone)]
pub struct SectionWritingState {
    data_start: u64,
    inner_written_bytes: u64,
    inner: v1::CarWriter,
}

#[derive(Debug, Clone)]
pub struct IndexWritingState {
    data: Vec<u8>,
    data_start: u64,
    data_end: u64,
    index_start: u64,
    index_offset: u64, // Offset from index_start
}

#[derive(Debug, Clone)]
pub struct FinalizedWritingState {
    header: CarV2Header,
    header_saved: bool,
}

impl Sealed for SectionWritingState {}
impl Sealed for IndexWritingState {}
impl Sealed for FinalizedWritingState {}
impl CarWriteV2State for SectionWritingState {}
impl CarWriteV2State for IndexWritingState {}
impl CarWriteV2State for FinalizedWritingState {}

impl CarWriter<SectionWritingState> {
    pub fn new(roots: Vec<RawCid>) -> Self {
        Self::with_buffer_size(roots, 16 * 1024 * 1024)
    }

    pub fn with_buffer_size(roots: Vec<RawCid>, buffer_size: usize) -> Self {
        let inner = v1::CarWriter::with_buffer_size(roots, buffer_size);
        let state = SectionWritingState {
            data_start: 51, // CARv2 pragma + header is 11 + 40 bytes long, so the data starts right after it
            inner_written_bytes: 0,
            inner,
        };
        Self { state }
    }

    /// Write a section to the CAR stream.
    ///
    /// This method will serialize the section and append it to the current CAR stream.
    /// However, it does not actually write to the underlying sink until `send_data` is called.
    pub fn write_section(&mut self, section: &Section) -> Result<SectionLocation, CarWriterError> {
        self.state.inner.write_section(section)
            .map(|loc| SectionLocation {
                offset: self.state.data_start + loc.offset,
                length: loc.length,
            })
            .map_err(|err| match err {
                v1::CarWriterError::BufferFull => CarWriterError::BufferFull,
            })
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
    /// A tuple (offset, length) indicating the range of bytes in the underlying sink that should be written.
    pub fn send_data(&mut self, buf: &mut [u8]) -> (usize, usize) {
        let bytes_to_send = self.state.inner.send_data(buf);
        let offset = self.state.data_start + self.state.inner_written_bytes;
        self.state.inner_written_bytes += bytes_to_send as u64;
        (offset as usize, bytes_to_send)
    }

    /// Check if there is data ready to be sent to the underlying sink.
    /// 
    /// This can be used by the caller to determine when to call `send_data` to flush the data buffer.
    pub fn has_data_to_send(&self) -> bool {
        self.state.inner.has_data_to_send()
    }

    pub fn finalize_sections(self) -> Result<CarWriter<IndexWritingState>, CarWriterError> {
        if self.has_data_to_send() {
            return Err(CarWriterError::BufferNotFlushed);
        }

        // TODO: Write the correct data size (in header) to file
        Ok(CarWriter {
            state: IndexWritingState {
                data: Vec::new(),
                data_start: self.state.data_start,
                data_end: self.state.data_start + self.state.inner_written_bytes,
                index_start: 0,
                index_offset: 0,
            },
        })
    }
}

impl CarWriter<IndexWritingState> {
    pub fn finalize_index(self) -> Result<CarWriter<FinalizedWritingState>, CarWriterError> {
        if !self.state.data.is_empty() {
            return Err(CarWriterError::BufferNotFlushed);
        }

        let header = CarV2Header {
            characteristics: Characteristics(0), 
            data_offset: self.state.data_start,
            data_size: self.state.data_end - self.state.data_start,
            index_offset: self.state.index_start,
        };

        Ok(CarWriter {
            state: FinalizedWritingState {
                header,
                header_saved: false,
            },
        })
    }

    pub fn finalize_full_index(self) -> Result<CarWriter<FinalizedWritingState>, CarWriterError> {
        if !self.state.data.is_empty() {
            return Err(CarWriterError::BufferNotFlushed);
        }

        let mut c = Characteristics(0);
        c.set_has_full_index(true);
        let header = CarV2Header {
            characteristics: c,
            data_offset: self.state.data_start,
            data_size: self.state.data_end - self.state.data_start,
            index_offset: self.state.index_start,
        };

        Ok(CarWriter {
            state: FinalizedWritingState {
                header,
                header_saved: false,
            },
        })
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
    /// A tuple (offset, length) indicating the range of bytes in the underlying sink that should be written.
    pub fn send_data(&mut self, buf: &mut [u8]) -> (usize, usize) {
        let bytes_to_send = self.state.data.len().min(buf.len());
        if bytes_to_send == 0 {
            return (0, 0);
        }
        buf[..bytes_to_send].copy_from_slice(&self.state.data[..bytes_to_send]);
        self.state.data.drain(..bytes_to_send);
        let offset = self.state.index_start + self.state.index_offset;
        self.state.index_offset += bytes_to_send as u64;
        (offset as usize, bytes_to_send)
    }

    /// Check if there is data ready to be sent to the underlying sink.
    /// 
    /// This can be used by the caller to determine when to call `send_data` to flush the data buffer.
    pub fn has_data_to_send(&self) -> bool {
        self.state.data.len() > 0
    }
}

impl CarWriter<FinalizedWritingState> {
    pub fn header(&self) -> &CarV2Header {
        &self.state.header
    }

    /// Flush the current data buffer and return the bytes to be written to the underlying sink.
    ///
    /// The caller should write these bytes to the underlying sink and then call `send_data` again
    /// to flush more data if needed. If 0 bytes are written, it means that there is no more data to flush at the moment.
    ///
    /// # Arguments
    ///
    /// * `buf` - A mutable byte slice to which the data will be written.
    /// 
    /// **Assumption**: The header is always 51 bytes and is written at the very beginning of the CARv2 file, 
    /// so the offset is always 0. Therefore, it is necessary that **buf is at least 51 bytes long to accommodate the header**. 
    /// Otherwise, it will be truncated and the reader will fail to read the header correctly.
    ///
    /// # Returns
    ///
    /// A tuple (offset, length) indicating the range of bytes in the underlying sink that should be written.
    pub fn send_data(&mut self, mut buf: &mut [u8]) -> (usize, usize) {
        debug_assert!(
            buf.len() >= 51,
            "Buffer size must be at least 51 bytes to accommodate the CARv2 header"
        );
        if self.state.header_saved {
            return (0, 0);
        }
        let header_bytes : [u8; 40] = (&self.state.header).into();
        buf.write(&CAR_V2_PRAGMA).unwrap();
        buf.write(&header_bytes).unwrap();
        self.state.header_saved = true;
        (0, 51)
    }

    /// Check if there is data ready to be sent to the underlying sink.
    /// 
    /// This can be used by the caller to determine when to call `send_data` to flush the data buffer.
    pub fn has_data_to_send(&self) -> bool {
        !self.state.header_saved
    }
}

/// Errors related to CarWriter operations
#[derive(thiserror::Error, Debug)]
pub enum CarWriterError {
    /// Buffer is full and cannot accommodate the new section
    ///
    /// This error occurs when trying to write a section that exceeds the remaining capacity of the internal buffer.
    /// To resolve this, you can either flush the current buffer to the underlying sink to free up space 
    /// or increase the buffer size when creating the CarWriter.
    #[error("Buffer is full, cannot write section")]
    BufferFull,
    /// Cannot finalize because the buffer has not been fully flushed
    /// 
    /// This error occurs when trying to finalize the CARv2 file (either sections or index) while there is
    /// still data in the internal buffer that has not been flushed to the underlying sink.  
    /// To resolve this, you should call `send_data` repeatedly until it returns 0 bytes to flush all remaining data
    /// before finalizing.
    #[error("Cannot finalize, buffer has not been fully flushed")]
    BufferNotFlushed,
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::v1::Block;

    #[test]
    fn test_car_writer_no_index() {
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
        let mut buf = [0u8; 64];
        let mut section_to_write = vec![section1, section2, section3];
        loop {
            // Write bytes to sink until there are no more sections to write and no more data to flush
            if writer.has_data_to_send() {
                let (pos, len) = writer.send_data(&mut buf);
                if pos + len > sink.len() {
                    sink.resize(pos + len, 0);
                }
                sink[pos..pos + len].copy_from_slice(&mut buf[..len]);
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
                    Err(e) => panic!("Unexpected error while writing section: {:?}", e),
                }
            }
        }
        let writer = writer.finalize_sections().unwrap();
        let mut writer = writer.finalize_index().unwrap();
        while writer.has_data_to_send() {
            let (pos, len) = writer.send_data(&mut buf);
            if pos + len > sink.len() {
                sink.resize(pos + len, 0);
            }
            sink[pos..pos + len].copy_from_slice(&mut buf[..len]);
        }
        println!("Final CAR data: {:?}", hex::encode(&sink));
        assert_eq!(sink.len(), 233);
    }

    // TODO: Tests writer and reader match, by writing a CAR file with the writer and then reading 
    // it with the reader and checking that the header and sections are the same.
}