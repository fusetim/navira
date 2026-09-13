use crate::sio::ReadCursor;
use crate::wire::v1::{CarBlock, CarHeader as CarV1Header, CarSection, CarSectionMetadata};
use crate::wire::varint::UnsignedVarint;
use ciborium::de::Error as CborError;
use std::io;
use std::num::NonZeroUsize;

/// Maximum block size for a CARv1 block.
///
/// This is a limit to prevent reading excessively large blocks that could lead
/// to memory exhaustion or denial of service attacks.
/// The maximum block size is set to 2 MiB (2 * 1024 * 1024 bytes).
pub const MAX_BLOCK_SIZE: usize = 1 << 21; // 2 MiB

pub fn read_header(
    cursor: &ReadCursor<'_>,
) -> Result<(CarV1Header, usize), CarV1HeaderDeserializationError> {
    // Read the header length
    let Some((header_length, varlen)) = UnsignedVarint::decode(&cursor) else {
        return Err(CarV1HeaderDeserializationError::InsufficientData { hint: None });
    };
    let header_length = header_length.0 as usize;

    // Check if we have enough bytes to read the header
    if cursor.len() < varlen + header_length {
        return Err(CarV1HeaderDeserializationError::InsufficientData {
            hint: NonZeroUsize::new(varlen + header_length - cursor.len()),
        });
    }

    // Read the header
    let mut cursor = io::Cursor::new(&cursor[varlen..varlen + header_length]);
    let parsed: Result<CarV1Header, CborError<io::Error>> = ciborium::de::from_reader(&mut cursor);
    match parsed {
        Ok(header) => {
            if header.version() != 1 {
                return Err(CarV1HeaderDeserializationError::InvalidVersion(
                    header.version(),
                ));
            }
            Ok((header, varlen + header_length))
        }
        Err(CborError::Io(err)) => match err.kind() {
            io::ErrorKind::UnexpectedEof => {
                Err(CarV1HeaderDeserializationError::InsufficientData { hint: None })
            }
            _ => Err(CarV1HeaderDeserializationError::InvalidCbor(CborError::Io(
                err,
            ))),
        },
        Err(err) => Err(CarV1HeaderDeserializationError::InvalidCbor(err)),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CarV1HeaderDeserializationError {
    #[error("Invalid CBOR data")]
    InvalidCbor(#[from] CborError<io::Error>),
    #[error("Invalid CARv1 header, found version: {}", .0)]
    InvalidVersion(u64),
    #[error("Insufficient data provided to read the header")]
    InsufficientData {
        // Additional bytes needed (at least) to read the header.
        // This is a hint for the caller to know how many more bytes are needed.
        hint: Option<NonZeroUsize>,
    },
}

/// Errors related to deserialization of CAR sections
#[derive(thiserror::Error, Debug)]
pub enum CarSectionDeserialiationError {
    /// CID used by this section is of an unknown version, which is not
    /// supported by this library. More info in [CidFormatError::UnknownCidVersion].
    #[error("Unsupported CID")]
    UnsupportedCid,
    /// The input bytes are not enough to completely read the section.
    ///
    /// Hint is provided with the minimum number of additional bytes that are
    /// necessary to advance further in the deserialization process.
    /// *If the section metadata have been successfully read, this number will be exact.*
    #[error("Insufficient data to read section metadata")]
    InsufficientData {
        /// Minimum number of additional bytes necessary.
        hint: Option<NonZeroUsize>,
    },
    /// The block size of the section exceeds the maximum allowed size.
    #[error("Too large block size: {0} bytes exceeds maximum allowed size")]
    TooLargeBlockSize(usize),
}

pub fn read_section_metadata(
    cursor: &ReadCursor<'_>,
) -> Result<(CarSectionMetadata, usize), CarSectionDeserialiationError> {
    let (metadata, consumed) =
        CarSectionMetadata::try_from_bytes(&cursor).map_err(|err| match err {
            crate::wire::v1::CarSectionDeserialiationError::UnsupportedCid => {
                CarSectionDeserialiationError::UnsupportedCid
            }
            crate::wire::v1::CarSectionDeserialiationError::InsufficientData { hint } => {
                CarSectionDeserialiationError::InsufficientData { hint }
            }
        })?;

    Ok((metadata, consumed))
}

pub fn read_section(
    cursor: &ReadCursor<'_>,
) -> Result<(CarSection, usize), CarSectionDeserialiationError> {
    let (metadata, consumed) = read_section_metadata(&cursor)?;

    // Check if the block size exceeds the maximum allowed size
    if metadata.length as usize > MAX_BLOCK_SIZE {
        return Err(CarSectionDeserialiationError::TooLargeBlockSize(
            metadata.length as usize,
        ));
    }

    // Check if we have enough bytes to read the block
    if cursor.len() < consumed + (metadata.length as usize) {
        return Err(CarSectionDeserialiationError::InsufficientData {
            hint: NonZeroUsize::new(consumed + (metadata.length as usize) - cursor.len()),
        });
    }

    let block_start = consumed;
    let block_length = metadata.length as usize;
    let block_end = block_start + block_length;
    let block = CarBlock(cursor[block_start..block_end].to_vec());
    Ok((CarSection { metadata, block }, consumed + (block_length)))
}
