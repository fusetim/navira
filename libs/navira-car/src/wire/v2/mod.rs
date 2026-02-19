//! CAR v2 related types and utilities
//!
//! This module contains types and utilities related to the CAR v2 format.
//! If you are looking for the main CAR reader/writer, you probably want to use the
//! [CarReader](crate::CarReader) types in the parent module instead, which can handle both CAR v1
//! and v2 formats transparently.
//!
//! However, if you only need to work with CAR v2 headers or sections, you can use the types in this module directly.

mod header;
mod index;
use crate::wire::{cid::RawCid, v1};

pub use header::{CarV2Header, Characteristics};
pub use v1::{Block, Section, SectionFormatError};
pub use index::*;

/// CAR v2 pragma bytes
///
/// These bytes are used to identify the CAR v2 format in a file header.  
/// The pragma consists of a fixed sequence of bytes that includes
/// the version number of the CAR format.
pub const CAR_V2_PRAGMA: &[u8] = &[
    0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
];

/// CARv2 Reader
#[derive(Debug, Clone)]
pub struct CarReader(CarReaderState);

#[derive(Debug, Clone)]
enum CarReaderState {
    NoHeader(NoHeaderState),
    HeaderV2(HeaderState),
    HeaderV1(HeaderState),
}

#[derive(Debug, Clone)]
struct NoHeaderState {
    /// Internal data buffer
    data: Vec<u8>,
    /// Internal data start position
    start: usize,
}

#[derive(Debug, Clone)]
struct HeaderState {
    /// CAR v2 header
    header: header::CarV2Header,
    /// Inner CAR v1 reader
    ///
    /// Used to read the CAR v1 sections within the CAR v2 file.
    v1_reader: v1::CarReader,
}

impl CarReader {
    /// Creates a new CAR v2 reader
    pub fn new() -> Self {
        CarReader(CarReaderState::NoHeader(NoHeaderState {
            data: Vec::new(),
            start: 0,
        }))
    }

    /// Has the header been read?
    pub fn has_header(&self) -> bool {
        matches!(self.0, CarReaderState::HeaderV1(_))
    }

    /// Get the CAR headers if available
    pub fn header(&self) -> Option<(&v1::CarHeader, &header::CarV2Header)> {
        match &self.0 {
            CarReaderState::HeaderV1(state) => Some((
                state
                    .v1_reader
                    .header()
                    .expect("Header CARv1 should be present in this state"),
                &state.header,
            )),
            _ => None,
        }
    }

    /// Receives more data to process
    pub fn receive_data(&mut self, buf: &[u8], pos: usize) {
        match &mut self.0 {
            CarReaderState::NoHeader(state) => {
                if pos != state.start + state.data.len() {
                    // Out of order data, ignore
                    return;
                }
                state.data.extend_from_slice(buf);
            }
            CarReaderState::HeaderV2(state) | CarReaderState::HeaderV1(state) => {
                let v1_data_start = state.header.data_offset as usize;
                let v1_data_end = v1_data_start + state.header.data_size as usize;
                if pos < v1_data_start || pos >= v1_data_end {
                    // Out of bounds data, ignore
                    return;
                }
                let pos = pos - v1_data_start;
                let len = buf.len().min(v1_data_end - pos);
                state.v1_reader.receive_data(&buf[..len], pos);
            }
        }
    }

    /// Read the CAR headers if not already read
    ///
    /// This methods will attempt to read the CAR v2 and v1 headers from the internal buffer.
    pub fn read_header(&mut self) -> Result<(), CarReaderError> {
        match &mut self.0 {
            CarReaderState::NoHeader(state) => {
                if state.data.len() < 51 {
                    return Err(CarReaderError::InsufficientData(
                        state.data.len(),
                        51 - state.data.len(),
                    ));
                }

                if &state.data[0..11] != CAR_V2_PRAGMA {
                    return Err(CarReaderError::InvalidVersion);
                }

                let header_bytes: [u8; 40] = state.data[11..51].try_into().unwrap();
                let header = header::CarV2Header::from(header_bytes);
                let mut v1_reader = v1::CarReader::new();
                if state.data.len() > header.data_offset as usize {
                    // Feed any available data to the CAR v1 reader
                    let v1_data_end = (header.data_offset as usize + header.data_size as usize)
                        .min(state.data.len());
                    v1_reader
                        .receive_data(&state.data[header.data_offset as usize..v1_data_end], 0);
                }

                // Try to read the CAR v1 header
                match v1_reader.read_header().map_err(|e| match e {
                    v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                    v1::CarReaderError::PreconditionNotMet => CarReaderError::PreconditionNotMet,
                    v1::CarReaderError::InsufficientData(offset, hint) => {
                        CarReaderError::InsufficientData(header.data_offset as usize + offset, hint)
                    }
                    v1::CarReaderError::InvalidSectionFormat(e) => {
                        CarReaderError::InvalidSectionFormat(e)
                    }
                }) {
                    Ok(_) => {
                        // Successfully read both headers -> Fully initialized
                        self.0 = CarReaderState::HeaderV1(HeaderState { header, v1_reader });
                        Ok(())
                    }
                    Err(e) => {
                        // Could not read CAR v1 header yet -> Keep as HeaderV2 state
                        self.0 = CarReaderState::HeaderV2(HeaderState { header, v1_reader });
                        Err(e)
                    }
                }
            }
            CarReaderState::HeaderV2(state) => {
                // Try to read the CAR v1 header
                state.v1_reader.read_header().map_err(|e| match e {
                    v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                    v1::CarReaderError::PreconditionNotMet => CarReaderError::PreconditionNotMet,
                    v1::CarReaderError::InsufficientData(offset, hint) => {
                        CarReaderError::InsufficientData(
                            state.header.data_offset as usize + offset,
                            hint,
                        )
                    }
                    v1::CarReaderError::InvalidSectionFormat(e) => {
                        CarReaderError::InvalidSectionFormat(e)
                    }
                })?;

                // Successfully read both headers -> Fully initialized
                self.0 = CarReaderState::HeaderV1(state.clone());
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn find_section(&mut self, cid: &RawCid) -> Result<Section, CarReaderError> {
        match &mut self.0 {
            CarReaderState::HeaderV1(state) => {
                state.v1_reader.find_section(cid).map_err(|e| match e {
                    v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                    v1::CarReaderError::InvalidSectionFormat(e) => {
                        CarReaderError::InvalidSectionFormat(e)
                    }
                    v1::CarReaderError::PreconditionNotMet => CarReaderError::PreconditionNotMet,
                    v1::CarReaderError::InsufficientData(offset, hint) => {
                        CarReaderError::InsufficientData(
                            state.header.data_offset as usize + offset,
                            hint,
                        )
                    }
                })
            }
            _ => Err(CarReaderError::PreconditionNotMet),
        }
    }

    pub fn read_section(&mut self) -> Result<Section, CarReaderError> {
        match &mut self.0 {
            CarReaderState::HeaderV1(state) => {
                state.v1_reader.read_section().map_err(|e| match e {
                    v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                    v1::CarReaderError::InvalidSectionFormat(e) => {
                        CarReaderError::InvalidSectionFormat(e)
                    }
                    v1::CarReaderError::PreconditionNotMet => CarReaderError::PreconditionNotMet,
                    v1::CarReaderError::InsufficientData(offset, hint) => {
                        CarReaderError::InsufficientData(
                            state.header.data_offset as usize + offset,
                            hint,
                        )
                    }
                })
            }
            _ => Err(CarReaderError::PreconditionNotMet),
        }
    }

    pub fn seek_first_section(&mut self) -> Result<(), CarReaderError> {
        match &mut self.0 {
            CarReaderState::HeaderV1(state) => {
                state.v1_reader.seek_first_section().map_err(|e| match e {
                    v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                    v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                    v1::CarReaderError::InvalidSectionFormat(e) => {
                        CarReaderError::InvalidSectionFormat(e)
                    }
                    v1::CarReaderError::PreconditionNotMet => CarReaderError::PreconditionNotMet,
                    v1::CarReaderError::InsufficientData(offset, hint) => {
                        CarReaderError::InsufficientData(
                            state.header.data_offset as usize + offset,
                            hint,
                        )
                    }
                })
            }
            _ => Err(CarReaderError::PreconditionNotMet),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAR_V2: [u8; 715] = [
        // Offset 0x00000000 to 0x000002CA
        0x0A, 0xA1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF3, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0xA2, 0x65, 0x72, 0x6F, 0x6F, 0x74, 0x73, 0x81,
        0xD8, 0x2A, 0x58, 0x23, 0x00, 0x12, 0x20, 0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13,
        0x71, 0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39, 0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3,
        0xCD, 0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2, 0x32, 0xF8, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69,
        0x6F, 0x6E, 0x01, 0x51, 0x12, 0x20, 0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13, 0x71,
        0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39, 0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3, 0xCD,
        0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2, 0x32, 0xF8, 0x12, 0x2D, 0x0A, 0x22, 0x12, 0x20, 0xD9,
        0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A, 0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC,
        0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E, 0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6,
        0x26, 0x12, 0x04, 0xF0, 0x9F, 0x8D, 0xA4, 0x18, 0xA4, 0x01, 0x85, 0x01, 0x12, 0x20, 0xD9,
        0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A, 0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC,
        0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E, 0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6,
        0x26, 0x12, 0x31, 0x0A, 0x22, 0x12, 0x20, 0xD7, 0x45, 0xB7, 0x75, 0x7F, 0x5B, 0x45, 0x93,
        0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E, 0xB4, 0x96, 0xA7, 0x41, 0x0A, 0x0D,
        0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1, 0x12, 0x09, 0x62, 0x61, 0x72, 0x72,
        0x65, 0x6C, 0x65, 0x79, 0x65, 0x18, 0x3A, 0x12, 0x2E, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20,
        0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
        0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
        0x29, 0x4D, 0x12, 0x04, 0xF0, 0x9F, 0x90, 0xA1, 0x18, 0x07, 0x58, 0x12, 0x20, 0xD7, 0x45,
        0xB7, 0x75, 0x7F, 0x5B, 0x45, 0x93, 0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E,
        0xB4, 0x96, 0xA7, 0x41, 0x0A, 0x0D, 0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1,
        0x12, 0x34, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20, 0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2,
        0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14, 0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9,
        0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5, 0x07, 0x2D, 0x12, 0x0A, 0x66, 0x69, 0x73,
        0x68, 0x6D, 0x6F, 0x6E, 0x67, 0x65, 0x72, 0x18, 0x04, 0x28, 0x01, 0x55, 0x12, 0x20, 0xB4,
        0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14, 0xEF,
        0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5, 0x07,
        0x2D, 0x66, 0x69, 0x73, 0x68, 0x2B, 0x01, 0x55, 0x12, 0x20, 0xA2, 0xE1, 0xC4, 0x0D, 0xA1,
        0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C, 0xA2, 0x3B, 0x74, 0xB9, 0xE5,
        0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C, 0x29, 0x4D, 0x6C, 0x6F, 0x62,
        0x73, 0x74, 0x65, 0x72, 0x01, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF,
        0xE7, 0x29, 0xEB, 0x4D, 0x5C, 0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8,
        0x04, 0xA2, 0x61, 0x08, 0x0C, 0x29, 0x4D, 0x94, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
        0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
        0x07, 0x2D, 0x6B, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD7, 0x45, 0xB7, 0x75, 0x7F,
        0x5B, 0x45, 0x93, 0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E, 0xB4, 0x96, 0xA7,
        0x41, 0x0A, 0x0D, 0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1, 0x12, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xD9, 0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A,
        0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC, 0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E,
        0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6, 0x26, 0x8B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13, 0x71, 0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39,
        0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3, 0xCD, 0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2,
        0x32, 0xF8, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn test_car_v2_header_deserialization() {
        let mut reader = CarReader::new();
        reader.receive_data(&CAR_V2, 0);
        reader.read_header().unwrap();
        let (v1h, v2h) = reader.header().unwrap();
        assert_eq!(v2h.characteristics.0, 0);
        assert_eq!(v2h.data_offset, 51);
        assert_eq!(v2h.data_size, 448);
        assert_eq!(v2h.index_offset, 499);
        assert_eq!(v1h.roots().len(), 1);
    }

    #[test]
    fn test_car_v2_header_deserialization_partial() {
        let mut reader = CarReader::new();

        let (v1h, v2h);
        loop {
            match reader.read_header() {
                Ok(_) => {
                    let headers = reader.header().unwrap();
                    v1h = headers.0.clone();
                    v2h = headers.1.clone();
                    break;
                }
                Err(CarReaderError::InsufficientData(offset, hint)) => {
                    let end = offset + hint;
                    let data = &CAR_V2[offset..end.min(CAR_V2.len())];
                    reader.receive_data(data, offset);
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        assert_eq!(v2h.characteristics.0, 0);
        assert_eq!(v2h.data_offset, 51);
        assert_eq!(v2h.data_size, 448);
        assert_eq!(v2h.index_offset, 499);
        assert_eq!(v1h.roots().len(), 1);
    }

    #[test]
    fn test_car_v2_header_count_blocks() {
        let mut reader = CarReader::new();
        reader.receive_data(&CAR_V2, 0);
        reader.read_header().unwrap();

        let mut block_count = 0;
        let mut block_bytes = 0;
        loop {
            match reader.read_section() {
                Ok(section) => {
                    println!("Read section: {:?}", section);
                    block_count += 1;
                    block_bytes += section.block().data().len();
                }
                Err(CarReaderError::InsufficientData(_, _)) => {
                    break;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        assert_eq!(block_count, 5);
        assert_eq!(block_bytes, 211);
    }
}
