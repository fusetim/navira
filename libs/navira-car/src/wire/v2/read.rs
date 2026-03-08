use crate::wire::cid::RawCid;
use crate::wire::v1;
use crate::wire::v2::{
    CAR_V2_PRAGMA, LocatableSection, SectionFormatError, SectionLocation, header,
};

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

    pub fn find_section(&mut self, cid: &RawCid) -> Result<LocatableSection, CarReaderError> {
        // TODO: Use the index if available to find the section location more efficiently instead of searching sequentially
        match &mut self.0 {
            CarReaderState::HeaderV1(state) => state
                .v1_reader
                .find_section(cid)
                .map(|locsec| LocatableSection {
                    section: locsec.section,
                    location: SectionLocation {
                        offset: state.header.data_offset + locsec.location.offset,
                        length: locsec.location.length,
                    },
                })
                .map_err(|e| match e {
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
                }),
            _ => Err(CarReaderError::PreconditionNotMet),
        }
    }

    pub fn read_section(&mut self) -> Result<LocatableSection, CarReaderError> {
        match &mut self.0 {
            CarReaderState::HeaderV1(state) => {
                state
                    .v1_reader
                    .read_section()
                    .map(|locsec| LocatableSection {
                        section: locsec.section,
                        location: SectionLocation {
                            offset: state.header.data_offset + locsec.location.offset,
                            length: locsec.location.length,
                        },
                    })
                    .map_err(|e| match e {
                        v1::CarReaderError::InvalidFormat => CarReaderError::InvalidFormat,
                        v1::CarReaderError::InvalidVersion(_) => CarReaderError::InvalidFormat,
                        v1::CarReaderError::InvalidHeader(e) => CarReaderError::InvalidHeader(e),
                        v1::CarReaderError::InvalidSectionFormat(e) => {
                            CarReaderError::InvalidSectionFormat(e)
                        }
                        v1::CarReaderError::PreconditionNotMet => {
                            CarReaderError::PreconditionNotMet
                        }
                        v1::CarReaderError::InsufficientData(offset, hint) => {
                            // Check if the offset is within the CAR v1 data range
                            if offset < state.header.data_size as usize {
                                CarReaderError::InsufficientData(
                                    state.header.data_offset as usize + offset,
                                    hint,
                                )
                            } else {
                                CarReaderError::EndOfSections
                            }
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
    /// No more sections available in the CAR file
    ///
    /// This error is returned when attempting to read a section but there are no more sections available in the CAR file.  
    /// For instance, when you reached the end of the inner CARv1 data in a CARv2 file and try to read another section, you will get this error.
    #[error("No more sections available in the CAR file")]
    EndOfSections,
}
