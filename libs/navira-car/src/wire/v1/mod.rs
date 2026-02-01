use crate::wire::{cid::RawCid, varint::UnsignedVarint};

pub use data::{Block, Section, SectionFormatError};
pub use header::CarHeader;

pub mod data;
pub mod header;

/// CAR v1 reader
///
/// This struct provides functionality to read CAR v1 files, in a sans-io manner
#[derive(Debug, Clone)]
pub struct CarReader {
    /// Internal data buffer
    data: Vec<u8>,
    /// Internal data start position
    start: usize,
    /// Parsed header, if available
    /// (CarHeader, total_header_size including length varint)
    header: Option<(header::CarHeader, usize)>,
}

impl CarReader {
    /// Creates a new CarReader
    pub fn new() -> Self {
        CarReader {
            data: Vec::new(),
            start: 0,
            header: None,
        }
    }

    /// Has the header already been parsed?
    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    /// Get the header if parsed
    pub fn header(&self) -> Option<&header::CarHeader> {
        self.header.as_ref().map(|(header, _)| header)
    }

    /// Seek to the first section (after the header)
    ///
    /// # Returns
    ///
    /// * Ok(()) - Successfully seeked to the first section
    /// * Err(CarReaderError) - Error occurred during seeking
    ///
    /// Precondition: Header must be parsed before calling this method.
    pub fn seek_first_section(&mut self) -> Result<(), CarReaderError> {
        match self.header {
            Some((_, total_header_size)) => {
                if self.start == total_header_size {
                    // Already at the first section
                    return Ok(());
                }
                // Clear the buffer and set start to the end of the header
                self.data.clear();
                self.start = total_header_size;
                Ok(())
            }
            None => Err(CarReaderError::PreconditionNotMet),
        }
    }

    /// Receive data into the reader's buffer
    ///
    /// # Arguments
    /// * `buf` - Buffer to fill from
    /// * `pos` - Offset position inside the CAR file which the buffer has been read from
    pub fn receive_data(&mut self, buf: &[u8], pos: usize) {
        // Internal behavior:
        // If pos == start + data.len(), append to the end
        // Otherwise, a "seek" has occurred, so reset the buffer
        if pos == self.start + self.data.len() {
            self.data.extend_from_slice(buf);
        } else {
            self.data.clear();
            self.data.extend_from_slice(buf);
            self.start = pos;
        }
    }

    /// Attempt to read and parse the CAR header
    ///
    //// # Returns
    ///
    /// * Ok(CarHeader) - Parsed CAR header
    /// * Err(CarReaderError) - Error occurred during header reading
    ///
    /// Based on the events, the caller may need to provide more data via `receive_data()`.
    /// In particular when it received CarReaderError::InsufficientData(read_from, hint_length),
    /// you should try to read at least `hint_length` bytes starting from `read_from` offset.
    pub fn read_header(&mut self) -> Result<(), CarReaderError> {
        // If header is not yet parsed, attempt to parse it
        if self.header.is_none() {
            // If start != 0, that means we are not at the beginning of the file
            // Seek at the beginning is required for CAR v1
            if self.start != 0 {
                return Err(CarReaderError::InsufficientData(0, 8));
            }

            // CARv1 header length is stored as an unsigned varint at the start of the file
            match UnsignedVarint::decode(&self.data) {
                Some((varint_len, varint_size)) => {
                    let header_len = varint_len.0 as usize;
                    let total_header_size = varint_size + header_len;

                    if self.data.len() < total_header_size {
                        // Not enough data to parse the full header
                        return Err(CarReaderError::InsufficientData(
                            self.start + self.data.len(),
                            total_header_size - self.data.len(),
                        ));
                    }

                    // Parse the header
                    let header: CarHeader =
                        match ciborium::from_reader(&self.data[varint_size..total_header_size]) {
                            Ok(h) => h,
                            Err(err) => {
                                return Err(CarReaderError::InvalidHeader(err));
                            }
                        };

                    // Store the parsed header
                    self.header = Some((header.clone(), total_header_size));

                    // Remove the parsed header from the buffer
                    self.data.drain(0..total_header_size);
                    self.start += total_header_size;
                }
                None => {
                    // Not enough data to parse the varint (which is very strange, but possible)
                    if self.data.len() > 8 {
                        // If we have more than 8 bytes and still can't parse varint, it's an error
                        return Err(CarReaderError::InvalidFormat);
                    }
                    return Err(CarReaderError::InsufficientData(
                        self.start + self.data.len(),
                        8,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Attempt to read and parse the next block (aka section) from the CAR file
    ///
    /// # Returns
    ///
    /// * Ok(Section) - Parsed section
    /// * Err(CarReaderError) - Error occurred during section reading
    ///
    /// Based on the events, the caller may need to provide more data via `receive_data()`.
    /// In particular when it received CarReaderError::InsufficientData(read_from, hint_length),
    /// you should try to read at least `hint_length` bytes starting from `read_from` offset.
    ///
    /// Precondition: Header must be parsed before calling this method.
    pub fn read_section(&mut self) -> Result<data::Section, CarReaderError> {
        // Header must be parsed before reading sections
        if !self.has_header() {
            return Err(CarReaderError::PreconditionNotMet);
        }

        // Attempt to parse a section
        match Section::try_read_bytes(&self.data) {
            Ok((section, section_size)) => {
                // Remove the parsed section from the buffer
                self.data.drain(0..section_size);
                self.start += section_size;

                Ok(section)
            }
            Err(SectionFormatError::InsufficientData) => {
                // Not enough data to parse a full section
                Err(CarReaderError::InsufficientData(
                    self.start + self.data.len(),
                    0,
                ))
            }
            Err(err) => {
                // Some other error occurred during section parsing
                Err(CarReaderError::InvalidSectionFormat(err))
            }
        }
    }

    /// Find and return the section with the given CID
    ///
    /// This method will read through sections until it finds the one with the specified CID.
    ///
    /// # Arguments
    /// * `cid` - The CID of the section to find
    ///
    /// # Returns
    ///
    /// * Ok(Section) - The found section with the specified CID
    /// * Err(CarReaderError) - Error occurred during searching
    ///
    /// Precondition: Header must be parsed before calling this method.
    ///
    /// Note: If you have no knowledge of the section position in advance, you must
    /// seek to the first section before calling this method. Otherwise, it will start searching
    /// from the current position, which may lead to missing the desired section.
    pub fn find_section(&mut self, cid: &RawCid) -> Result<Section, CarReaderError> {
        // Header must be parsed before searching sections
        if !self.has_header() {
            return Err(CarReaderError::PreconditionNotMet);
        }

        loop {
            match Section::try_read_header_bytes(&self.data) {
                Ok((section, section_size)) => {
                    // Check if the CID matches
                    if section.cid() == cid {
                        // CID matches, now read the full section
                        return self.read_section();
                    } else {
                        // CID does not match, continue searching
                        if self.data.len() <= section_size {
                            self.data.clear();
                        } else {
                            self.data.drain(0..section_size);
                        }
                        self.start += section_size;
                    }
                }
                Err(SectionFormatError::InsufficientData) => {
                    // Not enough data to parse a full section
                    return Err(CarReaderError::InsufficientData(
                        self.start + self.data.len(),
                        0,
                    ));
                }
                Err(err) => {
                    // Some other error occurred during section parsing
                    return Err(CarReaderError::InvalidSectionFormat(err));
                }
            }
        }
    }
}

/// Errors related to CarReader operations
#[derive(thiserror::Error, Debug)]
pub enum CarReaderError {
    /// Invalid data format
    #[error("Invalid data format")]
    InvalidFormat,
    #[error("Invalid header format")]
    InvalidHeader(ciborium::de::Error<std::io::Error>),
    #[error("Invalid CAR version, expected 1, got {0}")]
    InvalidVersion(usize),
    #[error("Invalid section format")]
    InvalidSectionFormat(#[from] SectionFormatError),
    /// Precondition not met for operation
    #[error("Precondition not met for operation")]
    PreconditionNotMet,
    /// Insufficient data to proceed
    ///
    /// # Arguments
    /// * usize - Need to read from this offset
    /// * usize - Hint length of data to read (if known, otherwise 0)
    #[error("Insufficient data to proceed")]
    InsufficientData(usize, usize),
}

#[cfg(test)]
mod tests {
    use super::{CarReader, CarReaderError};
    use crate::wire::cid::RawCid;
    use crate::wire::v1::data::Section;
    use crate::wire::v1::header::CarHeader;

    const CAR_V1: [u8; 715] = [
        // Offset 0x00000000 to 0x000002CA
        0x63, 0xA2, 0x65, 0x72, 0x6F, 0x6F, 0x74, 0x73, 0x82, 0xD8, 0x2A, 0x58, 0x25, 0x00, 0x01,
        0x71, 0x12, 0x20, 0xF8, 0x8B, 0xC8, 0x53, 0x80, 0x4C, 0xF2, 0x94, 0xFE, 0x41, 0x7E, 0x4F,
        0xA8, 0x30, 0x28, 0x68, 0x9F, 0xCD, 0xB1, 0xB1, 0x59, 0x2C, 0x51, 0x02, 0xE1, 0x47, 0x4D,
        0xBC, 0x20, 0x0F, 0xAB, 0x8B, 0xD8, 0x2A, 0x58, 0x25, 0x00, 0x01, 0x71, 0x12, 0x20, 0x69,
        0xEA, 0x07, 0x40, 0xF9, 0x80, 0x7A, 0x28, 0xF4, 0xD9, 0x32, 0xC6, 0x2E, 0x7C, 0x1C, 0x83,
        0xBE, 0x05, 0x5E, 0x55, 0x07, 0x2C, 0x90, 0x26, 0x6A, 0xB3, 0xE7, 0x9D, 0xF6, 0x3A, 0x36,
        0x5B, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x01, 0x5B, 0x01, 0x71, 0x12, 0x20,
        0xF8, 0x8B, 0xC8, 0x53, 0x80, 0x4C, 0xF2, 0x94, 0xFE, 0x41, 0x7E, 0x4F, 0xA8, 0x30, 0x28,
        0x68, 0x9F, 0xCD, 0xB1, 0xB1, 0x59, 0x2C, 0x51, 0x02, 0xE1, 0x47, 0x4D, 0xBC, 0x20, 0x0F,
        0xAB, 0x8B, 0xA2, 0x64, 0x6C, 0x69, 0x6E, 0x6B, 0xD8, 0x2A, 0x58, 0x23, 0x00, 0x12, 0x20,
        0x02, 0xAC, 0xEC, 0xC5, 0xDE, 0x24, 0x38, 0xEA, 0x41, 0x26, 0xA3, 0x01, 0x0E, 0xCB, 0x1F,
        0x8A, 0x59, 0x9C, 0x8E, 0xFF, 0x22, 0xFF, 0xF1, 0xA1, 0xDC, 0xFF, 0xE9, 0x99, 0xB2, 0x7F,
        0xD3, 0xDE, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x64, 0x62, 0x6C, 0x69, 0x70, 0x83, 0x01, 0x12,
        0x20, 0x02, 0xAC, 0xEC, 0xC5, 0xDE, 0x24, 0x38, 0xEA, 0x41, 0x26, 0xA3, 0x01, 0x0E, 0xCB,
        0x1F, 0x8A, 0x59, 0x9C, 0x8E, 0xFF, 0x22, 0xFF, 0xF1, 0xA1, 0xDC, 0xFF, 0xE9, 0x99, 0xB2,
        0x7F, 0xD3, 0xDE, 0x12, 0x2E, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20, 0xB6, 0xFB, 0xD6, 0x75,
        0xF9, 0x8E, 0x2A, 0xBD, 0x22, 0xD4, 0xED, 0x29, 0xFD, 0xC8, 0x31, 0x50, 0xFE, 0xDC, 0x48,
        0x59, 0x7E, 0x92, 0xDD, 0x1A, 0x7A, 0x24, 0x38, 0x1D, 0x44, 0xA2, 0x74, 0x51, 0x12, 0x04,
        0x62, 0x65, 0x61, 0x72, 0x18, 0x04, 0x12, 0x2F, 0x0A, 0x22, 0x12, 0x20, 0x79, 0xA9, 0x82,
        0xDE, 0x3C, 0x99, 0x07, 0x95, 0x3D, 0x4D, 0x32, 0x3C, 0xEE, 0x1D, 0x0F, 0xB1, 0xED, 0x8F,
        0x45, 0xF8, 0xEF, 0x02, 0x87, 0x0C, 0x0C, 0xB9, 0xE0, 0x92, 0x46, 0xBD, 0x53, 0x0A, 0x12,
        0x06, 0x73, 0x65, 0x63, 0x6F, 0x6E, 0x64, 0x18, 0x95, 0x01, 0x28, 0x01, 0x55, 0x12, 0x20,
        0xB6, 0xFB, 0xD6, 0x75, 0xF9, 0x8E, 0x2A, 0xBD, 0x22, 0xD4, 0xED, 0x29, 0xFD, 0xC8, 0x31,
        0x50, 0xFE, 0xDC, 0x48, 0x59, 0x7E, 0x92, 0xDD, 0x1A, 0x7A, 0x24, 0x38, 0x1D, 0x44, 0xA2,
        0x74, 0x51, 0x63, 0x63, 0x63, 0x63, 0x80, 0x01, 0x12, 0x20, 0x79, 0xA9, 0x82, 0xDE, 0x3C,
        0x99, 0x07, 0x95, 0x3D, 0x4D, 0x32, 0x3C, 0xEE, 0x1D, 0x0F, 0xB1, 0xED, 0x8F, 0x45, 0xF8,
        0xEF, 0x02, 0x87, 0x0C, 0x0C, 0xB9, 0xE0, 0x92, 0x46, 0xBD, 0x53, 0x0A, 0x12, 0x2D, 0x0A,
        0x24, 0x01, 0x55, 0x12, 0x20, 0x81, 0xCC, 0x5B, 0x17, 0x01, 0x86, 0x74, 0xB4, 0x01, 0xB4,
        0x2F, 0x35, 0xBA, 0x07, 0xBB, 0x79, 0xE2, 0x11, 0x23, 0x9C, 0x23, 0xBF, 0xFE, 0x65, 0x8D,
        0xA1, 0x57, 0x7E, 0x3E, 0x64, 0x68, 0x77, 0x12, 0x03, 0x64, 0x6F, 0x67, 0x18, 0x04, 0x12,
        0x2D, 0x0A, 0x22, 0x12, 0x20, 0xE7, 0xDC, 0x48, 0x6E, 0x97, 0xE6, 0xEB, 0xE5, 0xCD, 0xAB,
        0xAB, 0x3E, 0x39, 0x2B, 0xDA, 0xD1, 0x28, 0xB6, 0xE0, 0x9A, 0xCC, 0x94, 0xBB, 0x4E, 0x2A,
        0xA2, 0xAF, 0x7B, 0x98, 0x6D, 0x24, 0xD0, 0x12, 0x05, 0x66, 0x69, 0x72, 0x73, 0x74, 0x18,
        0x33, 0x28, 0x01, 0x55, 0x12, 0x20, 0x81, 0xCC, 0x5B, 0x17, 0x01, 0x86, 0x74, 0xB4, 0x01,
        0xB4, 0x2F, 0x35, 0xBA, 0x07, 0xBB, 0x79, 0xE2, 0x11, 0x23, 0x9C, 0x23, 0xBF, 0xFE, 0x65,
        0x8D, 0xA1, 0x57, 0x7E, 0x3E, 0x64, 0x68, 0x77, 0x62, 0x62, 0x62, 0x62, 0x51, 0x12, 0x20,
        0xE7, 0xDC, 0x48, 0x6E, 0x97, 0xE6, 0xEB, 0xE5, 0xCD, 0xAB, 0xAB, 0x3E, 0x39, 0x2B, 0xDA,
        0xD1, 0x28, 0xB6, 0xE0, 0x9A, 0xCC, 0x94, 0xBB, 0x4E, 0x2A, 0xA2, 0xAF, 0x7B, 0x98, 0x6D,
        0x24, 0xD0, 0x12, 0x2D, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20, 0x61, 0xBE, 0x55, 0xA8, 0xE2,
        0xF6, 0xB4, 0xE1, 0x72, 0x33, 0x8B, 0xDD, 0xF1, 0x84, 0xD6, 0xDB, 0xEE, 0x29, 0xC9, 0x88,
        0x53, 0xE0, 0xA0, 0x48, 0x5E, 0xCE, 0xE7, 0xF2, 0x7B, 0x9A, 0xF0, 0xB4, 0x12, 0x03, 0x63,
        0x61, 0x74, 0x18, 0x04, 0x28, 0x01, 0x55, 0x12, 0x20, 0x61, 0xBE, 0x55, 0xA8, 0xE2, 0xF6,
        0xB4, 0xE1, 0x72, 0x33, 0x8B, 0xDD, 0xF1, 0x84, 0xD6, 0xDB, 0xEE, 0x29, 0xC9, 0x88, 0x53,
        0xE0, 0xA0, 0x48, 0x5E, 0xCE, 0xE7, 0xF2, 0x7B, 0x9A, 0xF0, 0xB4, 0x61, 0x61, 0x61, 0x61,
        0x36, 0x01, 0x71, 0x12, 0x20, 0x69, 0xEA, 0x07, 0x40, 0xF9, 0x80, 0x7A, 0x28, 0xF4, 0xD9,
        0x32, 0xC6, 0x2E, 0x7C, 0x1C, 0x83, 0xBE, 0x05, 0x5E, 0x55, 0x07, 0x2C, 0x90, 0x26, 0x6A,
        0xB3, 0xE7, 0x9D, 0xF6, 0x3A, 0x36, 0x5B, 0xA2, 0x64, 0x6C, 0x69, 0x6E, 0x6B, 0xF6, 0x64,
        0x6E, 0x61, 0x6D, 0x65, 0x65, 0x6C, 0x69, 0x6D, 0x62, 0x6F,
    ];

    #[test]
    fn test_car_v1_reader_read_header() {
        let mut reader = CarReader::new();
        let chunk_size = 50;

        loop {
            match reader.read_header() {
                Ok(()) => {
                    // Header read successfully
                    break;
                }
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    // Provide more data
                    let end = std::cmp::min(read_from + chunk_size, CAR_V1.len());
                    if read_from >= end {
                        panic!("Test data exhausted before header could be read");
                    }
                    reader.receive_data(&CAR_V1[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading header: {:?}", err);
                }
            }
        }

        let header = reader.header().unwrap();
        assert_eq!(header.version(), 1);
        assert_eq!(header.roots().len(), 2);
    }

    #[test]
    fn test_car_v1_reader_count_sections() {
        let mut reader = CarReader::new();
        let chunk_size = 50;
        let mut block_bytes = 0;
        let mut block_count = 0;

        // First, read the header
        loop {
            match reader.read_header() {
                Ok(()) => break,
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + chunk_size, CAR_V1.len());
                    if read_from >= end {
                        panic!("Test data exhausted before header could be read");
                    }
                    reader.receive_data(&CAR_V1[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading header: {:?}", err);
                }
            }
        }

        // Seek to the first section - Not needed here

        // Now, read sections
        loop {
            match reader.read_section() {
                Ok(section) => {
                    block_bytes += section.block().data().len();
                    block_count += 1;
                    println!("Read section with CID: {:?}", section.cid());
                }
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + chunk_size, CAR_V1.len());
                    if read_from >= end {
                        // No more data to read
                        break;
                    }
                    reader.receive_data(&CAR_V1[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading section: {:?}", err);
                }
            }
        }
        assert_eq!(block_count, 8);
        assert_eq!(block_bytes, 323);
    }

    #[test]
    fn test_car_v1_reader_find_block() {
        let mut reader = CarReader::new();
        let chunk_size = 50;
        let mut block_bytes = 0;
        let mut block_count = 0;

        // First, read the header
        loop {
            match reader.read_header() {
                Ok(()) => break,
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + chunk_size, CAR_V1.len());
                    if read_from >= end {
                        panic!("Test data exhausted before header could be read");
                    }
                    reader.receive_data(&CAR_V1[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading header: {:?}", err);
                }
            }
        }

        // Seek to the first section - Not needed here

        // Now, find the block with the given CID
        let target_cid = RawCid::from_hex(
            "01551220b6fbd675f98e2abd22d4ed29fdc83150fedc48597e92dd1a7a24381d44a27451",
        )
        .unwrap();
        loop {
            match reader.find_section(&target_cid) {
                Ok(section) => {
                    block_bytes += section.block().data().len();
                    block_count += 1;
                    assert_eq!(section.cid(), &target_cid);
                }
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + chunk_size, CAR_V1.len());
                    if read_from >= end {
                        // No more data to read
                        break;
                    }
                    reader.receive_data(&CAR_V1[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading section: {:?}", err);
                }
            }
        }

        assert_eq!(block_count, 1);
        assert_eq!(block_bytes, 4);
    }
}
