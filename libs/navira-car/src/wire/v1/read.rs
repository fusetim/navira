use crate::wire::cid::RawCid;
use crate::wire::v1::{CarHeader, LocatableSection, Section, SectionFormatError, SectionLocation};
use crate::wire::varint::UnsignedVarint;

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
    header: Option<(CarHeader, usize)>,
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
    pub fn header(&self) -> Option<&CarHeader> {
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
    /// * Ok(LocatableSection) - Parsed section with its location in the CAR file
    /// * Err(CarReaderError) - Error occurred during section reading
    ///
    /// Based on the events, the caller may need to provide more data via `receive_data()`.
    /// In particular when it received CarReaderError::InsufficientData(read_from, hint_length),
    /// you should try to read at least `hint_length` bytes starting from `read_from` offset.
    ///
    /// Precondition: Header must be parsed before calling this method.
    pub fn read_section(&mut self) -> Result<LocatableSection, CarReaderError> {
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

                Ok(LocatableSection {
                    section,
                    location: SectionLocation {
                        offset: (self.start - section_size) as u64,
                        length: section_size as u64,
                    },
                })
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
    /// * Ok(LocatableSection) - The found section with the specified CID
    /// * Err(CarReaderError) - Error occurred during searching
    ///
    /// Precondition: Header must be parsed before calling this method.
    ///
    /// Note: If you have no knowledge of the section position in advance, you must
    /// seek to the first section before calling this method. Otherwise, it will start searching
    /// from the current position, which may lead to missing the desired section.
    pub fn find_section(&mut self, cid: &RawCid) -> Result<LocatableSection, CarReaderError> {
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
