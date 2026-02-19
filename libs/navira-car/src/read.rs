//! Read(er) utilities for CAR files
//!
//! This module contains utilities for reading CAR files, including the main [CarReader] type which can
//! read both CAR v1 and v2 formats transparently. It enforce the sans-io principle, so it does not perform
//! any actual I/O operations itself.
//!
//! Instead, it operates on byte slices (`&[u8]`) and provides methods to read headers, sections, and blocks from those byte slices.

use crate::wire::cid::RawCid;
use crate::wire::v1::CarHeader as CarHeaderV1;
use crate::wire::v1::CarReader as CarReaderV1;
use crate::wire::v1::CarReaderError as CarReaderV1Error;
use crate::wire::v1::LocatableSection;
use crate::wire::v1::SectionFormatError;
use crate::wire::v2::CAR_V2_PRAGMA;
use crate::wire::v2::CarReader as CarReaderV2;
use crate::wire::v2::CarReaderError as CarReaderV2Error;
use crate::wire::v2::CarV2Header as CarHeaderV2;

/// Main CAR reader type that can read both CAR v1 and v2 formats transparently.
#[derive(Debug)]
pub struct CarReader(CarReaderState);

/// Internal state of the CarReader, which can be either:
/// - Unclear: The reader has not yet determined whether the input is CAR v1 or v2, and
///   is accumulating bytes until it can make that determination.
/// - V1: The reader has determined that the input is CAR v1 and is using a CarReaderV1 to read the data.
/// - V2: The reader has determined that the input is CAR v2 and is using a CarReaderV2 to read the data.
#[derive(Debug)]
enum CarReaderState {
    Unclear(Vec<u8>),
    V1(CarReaderV1),
    V2(CarReaderV2),
}

/// CAR format indicates the version of the CAR file being read/write, which can be either v1 or v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarFormat {
    /// CAR v1 format
    ///
    /// See [CAR v1 specification](https://ipld.io/specs/transport/car/carv1/) for more details.
    V1,
    /// CAR v2 format
    ///
    /// See [CAR v2 specification](https://ipld.io/specs/transport/car/carv2/) for more details.
    V2,
}

/// Underlying reader for the CarReader, which can be either a CarReaderV1 or CarReaderV2 depending on the determined format.
#[derive(Debug)]
pub enum CarUnderlyingReader<'a> {
    /// CAR v1 reader
    V1(&'a mut CarReaderV1),
    /// CAR v2 reader
    V2(&'a mut CarReaderV2),
}

impl CarReader {
    /// Creates a new CarReader, capable of reading both CAR v1 and v2 formats.
    ///
    /// Initially, the reader is in an "unclear" state where it has not yet determined the format of the input data.
    pub fn new() -> Self {
        CarReader(CarReaderState::Unclear(Vec::new()))
    }

    /// Receives more data to process
    ///
    /// This method is used to feed more bytes into the CarReader, that will ultimately
    /// be processed by either the CarReaderV1 or CarReaderV2 once the format is determined.
    ///
    /// ## Arguments
    /// * `buf` - A slice of bytes containing the new data to process.
    /// * `pos` - The position in the overall input stream where these bytes belong.
    pub fn receive_data(&mut self, buf: &[u8], pos: usize) {
        match &mut self.0 {
            CarReaderState::Unclear(buffer) => {
                if pos != buffer.len() {
                    // This means that the caller is trying to provide bytes at a position that
                    // does not match the current buffer length, which indicates a logic error in the
                    // caller's code (e.g., providing bytes out of order).
                    return;
                }

                buffer.extend_from_slice(buf);
                // Try to determine the format (CAR v1 or v2) based on the accumulated bytes
                if let Some(format) = Self::determine_format(buffer) {
                    // If we can determine the format, transition to the appropriate state
                    let new_state = match format {
                        CarFormat::V1 => {
                            let mut v1 = CarReaderV1::new();
                            v1.receive_data(buffer, 0); // Assuming buffer is fully valid
                            CarReaderState::V1(v1)
                        }
                        CarFormat::V2 => {
                            let mut v2 = CarReaderV2::new();
                            v2.receive_data(buffer, 0); // Assuming buffer is fully valid
                            CarReaderState::V2(v2)
                        }
                    };
                    self.0 = new_state;
                }
            }
            CarReaderState::V1(reader) => reader.receive_data(buf, pos),
            CarReaderState::V2(reader) => reader.receive_data(buf, pos),
        }
    }

    /// Determines the CAR format (v1 or v2) based on the accumulated bytes.
    /// Returns `Some(CarFormat)` if the format can be determined, or `None` if more bytes are needed.
    fn determine_format(bytes: &[u8]) -> Option<CarFormat> {
        // Check for CAR v2 pragma
        if bytes.len() >= CAR_V2_PRAGMA.len() {
            if bytes.starts_with(CAR_V2_PRAGMA) {
                Some(CarFormat::V2)
            } else {
                Some(CarFormat::V1)
            }
        } else {
            None
        }
    }

    /// Gets the determined CAR format, if it has been determined.
    ///
    /// ## Returns
    /// - `Some(CarFormat::V1)` if the reader has determined that the input is CAR v1.
    /// - `Some(CarFormat::V2)` if the reader has determined that the input is CAR v2.
    /// - `None` if the reader has not yet determined the format.
    pub fn get_format(&self) -> Option<CarFormat> {
        match &self.0 {
            CarReaderState::Unclear(_) => None,
            CarReaderState::V1(_) => Some(CarFormat::V1),
            CarReaderState::V2(_) => Some(CarFormat::V2),
        }
    }

    /// Gets a mutable reference to the underlying reader (CarReaderV1 or CarReaderV2)
    /// if the format has been determined, or `None` if the format is still unclear.
    ///
    /// This allows the caller to interact with the specific reader once the format is known,
    /// while still using the unified CarReader interface.
    pub fn get_underlying_reader(&'_ mut self) -> Option<CarUnderlyingReader<'_>> {
        match &mut self.0 {
            CarReaderState::Unclear(_) => None,
            CarReaderState::V1(reader) => Some(CarUnderlyingReader::V1(reader)),
            CarReaderState::V2(reader) => Some(CarUnderlyingReader::V2(reader)),
        }
    }

    /// Has the header been read?
    pub fn has_header(&self) -> bool {
        match self.0 {
            CarReaderState::Unclear(_) => false,
            CarReaderState::V1(ref reader) => reader.has_header(),
            CarReaderState::V2(ref reader) => reader.has_header(),
        }
    }

    /// Get the CAR headers if available
    ///
    /// ## Returns
    /// - `None` if the header has not been read yet or if the reader is still in an unclear state.
    /// - `Some((&CarHeaderV1, None))` if the reader has read the CAR v1 header (and is in CAR v1 format).
    /// - `Some((&CarHeaderV1, Some(&CarHeaderV2)))` if the reader has read both the CAR v1 and v2 headers (and is in CAR v2 format).
    pub fn header(&self) -> Option<(&CarHeaderV1, Option<&CarHeaderV2>)> {
        match self.0 {
            CarReaderState::Unclear(_) => None,
            CarReaderState::V1(ref reader) => reader.header().map(|h| (h, None)),
            CarReaderState::V2(ref reader) => {
                if let Some((v1, v2)) = reader.header() {
                    Some((v1, Some(v2)))
                } else {
                    None
                }
            }
        }
    }

    /// Read the CAR headers if not already read
    pub fn read_header(&mut self) -> Result<(), CarReaderError> {
        match &mut self.0 {
            CarReaderState::Unclear(_) => Err(CarReaderError::InsufficientData(0,12)), // We need at least 12 bytes to determine the format and read the header
            CarReaderState::V1(reader) => reader.read_header().map_err(CarReaderError::from),
            CarReaderState::V2(reader) => reader.read_header().map_err(CarReaderError::from),
        }
    }

    /// Finds a section by its CID
    ///
    /// If an index is available, it will be used to efficiently locate the section.
    /// Otherwise, the reader will fall back to a linear search through the sections.
    ///
    /// ## Assumptions
    ///
    /// This function might not resolve on first call if the reader has not yet
    /// read enough data to determine the format or to read the headers, or if
    /// for instance, the reader is still awaiting more bytes to read the index.
    ///
    /// In such cases, it will return an error indicating that the preconditions for
    /// finding a section are not met, and the caller should try again after providing
    /// more data or after the reader has read the necessary headers/index.
    ///
    /// **IMPORTANT**: In case, the CID is not found in the index (or not available),
    /// a linear search will be performed. In that case, at the first call, the reader must
    /// point to the beginning of the sections (see [CarReader::seek_first_section]) otherwise
    /// it might skip some sections and return an error that the section is not found,
    /// even if it is present in the file.
    ///
    /// ## Arguments
    /// - `cid` - The CID of the section to find.
    ///
    /// ## Returns
    /// - `Ok(Section)` if a section with the specified CID is found.
    /// - `Err(CarReaderError)` if an error occurs during the search, such as an invalid section
    ///   format or if the reader is still in an unclear state.
    pub fn find_section(&mut self, cid: &RawCid) -> Result<LocatableSection, CarReaderError> {
        match &mut self.0 {
            CarReaderState::Unclear(_) => Err(CarReaderError::PreconditionNotMet),
            CarReaderState::V1(reader) => reader.find_section(cid).map_err(CarReaderError::from),
            CarReaderState::V2(reader) => reader.find_section(cid).map_err(CarReaderError::from),
        }
    }

    /// Reads the next section from the current position in the reader.
    ///
    /// This method will read the next section based on the current position of the reader.
    /// It assumes that the reader is already positioned at the beginning of a section (e.g., after seeking to the first section).
    ///
    /// ## Returns
    /// - `Ok(Section)` if a section is successfully read.
    /// - `Err(CarReaderError)` if an error occurs during reading, such as an invalid section format
    ///    or if the reader is still in an unclear state.
    pub fn read_section(&mut self) -> Result<LocatableSection, CarReaderError> {
        match &mut self.0 {
            CarReaderState::Unclear(_) => Err(CarReaderError::PreconditionNotMet),
            CarReaderState::V1(reader) => reader.read_section().map_err(CarReaderError::from),
            CarReaderState::V2(reader) => reader.read_section().map_err(CarReaderError::from),
        }
    }

    /// Seeks to the first section in the reader, which is necessary before performing a linear search for sections by CID.
    ///
    /// This method will position the reader at the beginning of the sections, which is typically right
    /// after the header(s) and any index (if present). This is important for ensuring that subsequent calls
    /// to `find_section` will not skip any sections during a linear search.
    pub fn seek_first_section(&mut self) -> Result<(), CarReaderError> {
        match &mut self.0 {
            CarReaderState::Unclear(_) => Err(CarReaderError::PreconditionNotMet),
            CarReaderState::V1(reader) => reader.seek_first_section().map_err(CarReaderError::from),
            CarReaderState::V2(reader) => reader.seek_first_section().map_err(CarReaderError::from),
        }
    }
}

/// Errors that can occur while reading CAR files with CarReader
///
/// This enum encapsulates errors from both the CAR v1 and v2 readers,
/// allowing the CarReader to return a unified error type regardless of the underlying format.
#[derive(thiserror::Error, Debug)]
pub enum CarReaderError {
    /// Invalid data format
    #[error("Invalid data format")]
    InvalidFormat,
    #[error("Invalid header format")]
    InvalidHeader(ciborium::de::Error<std::io::Error>),
    #[error("Invalid CAR version, expected 2")]
    InvalidVersion,
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
    /// No more sections available in the CAR file
    /// 
    /// This error is returned when attempting to read a section but there are no more sections available in the CAR file.  
    /// For instance, when you reached the end of the inner CARv1 data in a CARv2 file and try to read another section, you will get this error.
    #[error("No more sections available in the CAR file")]
    EndOfSections,
}

impl From<CarReaderV1Error> for CarReaderError {
    fn from(e: CarReaderV1Error) -> Self {
        match e {
            CarReaderV1Error::InvalidFormat => CarReaderError::InvalidFormat,
            CarReaderV1Error::InvalidVersion(_) => CarReaderError::InvalidVersion,
            CarReaderV1Error::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
            CarReaderV1Error::InvalidSectionFormat(e) => CarReaderError::InvalidSectionFormat(e),
            CarReaderV1Error::PreconditionNotMet => CarReaderError::PreconditionNotMet,
            CarReaderV1Error::InsufficientData(offset, hint) => {
                CarReaderError::InsufficientData(offset, hint)
            }
        }
    }
}

impl From<CarReaderV2Error> for CarReaderError {
    fn from(e: CarReaderV2Error) -> Self {
        match e {
            CarReaderV2Error::InvalidFormat => CarReaderError::InvalidFormat,
            CarReaderV2Error::InvalidVersion => CarReaderError::InvalidVersion,
            CarReaderV2Error::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
            CarReaderV2Error::InvalidSectionFormat(e) => CarReaderError::InvalidSectionFormat(e),
            CarReaderV2Error::PreconditionNotMet => CarReaderError::PreconditionNotMet,
            CarReaderV2Error::InsufficientData(offset, hint) => {
                CarReaderError::InsufficientData(offset, hint)
            }
            CarReaderV2Error::EndOfSections => CarReaderError::EndOfSections,
        }
    }
}
