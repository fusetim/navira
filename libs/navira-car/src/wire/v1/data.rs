use std::ops::Deref;

use crate::wire::cid::{CidFormatError, RawCid};

const MAX_BLOCK_SIZE: usize = 1 << 21; // 2 MiB by spec
const MAX_SECTION_SIZE: usize = MAX_BLOCK_SIZE + 128; // Allow some overhead for CID and varint

/// A Block represents a data block in a CAR file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block(Vec<u8>);

impl Block {
    pub fn new(data: Vec<u8>) -> Self {
        Block(data)
    }

    pub fn data(&self) -> &[u8] {
        &self.0
    }
}

/// A LocatableSection represents a Section that has been read from a CAR file
/// and has information about its location (offset and length) in the CAR file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatableSection {
    /// The actual Section data (length, CID, block)
    pub section: Section,
    /// The section location in the CAR file (offset and length)
    pub location: SectionLocation,
}

impl Deref for LocatableSection {
    type Target = Section;

    fn deref(&self) -> &Self::Target {
        &self.section
    }
}

/// A SectionLocation represents the location of a section in a CAR file (and its length), without the actual section data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionLocation {
    /// Offset of the section in the CAR file
    pub offset: u64,
    /// Length of the section in bytes (including the length prefix, CID, and block data)
    pub length: u64,
}

/// A Section represents a section in a CAR v1 file,
/// which includes the length prefix, CID, and data block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// Length of the section in bytes (excluding the length prefix)
    /// Varint encoded (LEB128)
    length: u64,
    /// CID of the block
    cid: RawCid,
    /// Data block
    block: Block,
}

impl Section {
    /// Creates a new Section
    pub fn new(length: u64, cid: RawCid, block: Block) -> Self {
        Section { length, cid, block }
    }

    /// Returns the length of the section
    pub fn length(&self) -> u64 {
        self.length
    }

    /// Returns the CID of the section
    pub fn cid(&self) -> &RawCid {
        &self.cid
    }

    /// Returns the data block of the section
    pub fn block(&self) -> &Block {
        &self.block
    }

    /// Tries to read a section header (length and CID) from the given bytes
    ///
    /// It returns the Section but it will not read the block data (the block will be empty).
    ///
    /// # Returns
    ///
    /// * Ok((Section, total_section_size)) - Successfully read the section header and return the whole size of the section
    /// * Err(SectionFormatError) - Error occurred during parsing
    pub fn try_read_header_bytes(bytes: &[u8]) -> Result<(Self, usize), SectionFormatError> {
        // Read the first 16 bytes looking for the length varint
        let (length_varint, varint_size) = match crate::wire::varint::UnsignedVarint::decode(bytes)
        {
            Some((varint, size)) => (varint.0, size),
            None => {
                if bytes.len() > 16 {
                    return Err(SectionFormatError::InvalidSize(MAX_BLOCK_SIZE + 1));
                } else {
                    return Err(SectionFormatError::InsufficientData);
                }
            }
        };
        // Validate length
        if length_varint as usize > MAX_SECTION_SIZE {
            return Err(SectionFormatError::InvalidSize(length_varint as usize));
        }
        // Try to read the CID
        let cid_start = varint_size;
        let (cid, cid_size) = match RawCid::try_read_bytes(&bytes[cid_start..]) {
            Ok((cid, size)) => (cid, size),
            Err(CidFormatError::InsufficientData) => {
                return Err(SectionFormatError::InsufficientData);
            }
            Err(e) => return Err(SectionFormatError::InvalidCid(e)),
        };
        let block_size = length_varint as usize - cid_size;
        Ok((
            Section::new(length_varint, cid, Block::new(Vec::new())),
            varint_size + cid_size + block_size,
        ))
    }

    /// Tries to read a Section from the given bytes
    pub fn try_read_bytes(bytes: &[u8]) -> Result<(Self, usize), SectionFormatError> {
        // Read the first 16 bytes looking for the length varint
        let (length_varint, varint_size) = match crate::wire::varint::UnsignedVarint::decode(bytes)
        {
            Some((varint, size)) => (varint.0, size),
            None => {
                if bytes.len() > 16 {
                    return Err(SectionFormatError::InvalidSize(MAX_BLOCK_SIZE + 1));
                } else {
                    return Err(SectionFormatError::InsufficientData);
                }
            }
        };
        // Validate length
        if length_varint as usize > MAX_SECTION_SIZE {
            return Err(SectionFormatError::InvalidSize(length_varint as usize));
        }
        // Try to read the CID
        let cid_start = varint_size;
        let (cid, cid_size) = match RawCid::try_read_bytes(&bytes[cid_start..]) {
            Ok((cid, size)) => (cid, size),
            Err(CidFormatError::InsufficientData) => {
                return Err(SectionFormatError::InsufficientData);
            }
            Err(e) => return Err(SectionFormatError::InvalidCid(e)),
        };
        // Calculate block size
        let block_size = length_varint as usize - cid_size;
        if bytes.len() < varint_size + cid_size + block_size {
            return Err(SectionFormatError::InsufficientData);
        }
        // Read the block data
        let block_start = varint_size + cid_size;
        let block_data = &bytes[block_start..block_start + block_size];
        let block = Block::new(block_data.to_vec());
        Ok((
            Section::new(length_varint, cid, block),
            varint_size + cid_size + block_size,
        ))
    }

    /// Converts the Section into bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Encode length varint
        let length_varint = crate::wire::varint::UnsignedVarint(self.length);
        bytes.extend_from_slice(&length_varint.encode());
        // Append CID bytes
        bytes.extend_from_slice(self.cid.bytes());
        // Append block data
        bytes.extend_from_slice(self.block.data());
        bytes
    }
}

/// Errors related to Section parsing
#[derive(thiserror::Error, Debug)]
pub enum SectionFormatError {
    /// Not enough data to parse the section
    #[error("Insufficient data for Section")]
    InsufficientData,

    /// Invalid CID format
    #[error("Invalid CID format: {0}")]
    InvalidCid(#[from] crate::wire::cid::CidFormatError),

    /// Invalid size or length
    #[error("Invalid size or length: {0}")]
    InvalidSize(usize),
}
