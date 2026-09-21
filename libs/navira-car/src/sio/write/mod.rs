use std::num::NonZeroUsize;

use crate::wire::{
    v1::{CarHeader as CarV1Header, CarSection},
    varint::UnsignedVarint,
};

#[derive(thiserror::Error, Debug)]
pub enum CarV1HeaderSerializationError {
    #[error("CBOR serialization error: {0}")]
    CborSerialization(ciborium::ser::Error<std::io::Error>),
    #[error("Not enough space in output buffer to write header, needed: {needed:?} bytes")]
    BufferTooSmall { needed: Option<NonZeroUsize> },
}

pub fn write_v1_header(
    out: &mut [u8],
    header: &CarV1Header,
) -> Result<usize, CarV1HeaderSerializationError> {
    // Serialize the header to CBOR
    let mut cbor_data = Vec::new();
    ciborium::ser::into_writer(header, &mut cbor_data)
        .map_err(CarV1HeaderSerializationError::CborSerialization)?;

    // Write the length of the header as a varint
    let header_length = cbor_data.len();
    let varint = UnsignedVarint(header_length as u64).encode();

    // Check if the output buffer is large enough
    if out.len() < varint.len() + header_length {
        return Err(CarV1HeaderSerializationError::BufferTooSmall {
            needed: NonZeroUsize::new(varint.len() + header_length),
        });
    }

    // Write the serialized header data
    out[..varint.len()].copy_from_slice(&varint);
    out[varint.len()..varint.len() + header_length].copy_from_slice(&cbor_data);

    Ok(varint.len() + header_length)
}

#[derive(thiserror::Error, Debug)]
pub enum CarSectionSerializationError {
    #[error("CBOR serialization error: {0}")]
    CborSerialization(ciborium::ser::Error<std::io::Error>),
    #[error("Not enough space in output buffer to write section, needed: {needed:?} bytes")]
    BufferTooSmall { needed: Option<NonZeroUsize> },
}

pub fn write_section(
    out: &mut [u8],
    section: &CarSection,
) -> Result<usize, CarSectionSerializationError> {
    // Preemptively serialize the CAR section metadata, to
    // check how much space it will take in the output buffer.
    let metadata = section.metadata.to_bytes();

    if out.len() < metadata.len() + section.block.len() {
        return Err(CarSectionSerializationError::BufferTooSmall {
            needed: NonZeroUsize::new(metadata.len() + section.block.len()),
        });
    }

    // Write the serialized data
    out[..metadata.len()].copy_from_slice(&metadata);
    out[metadata.len()..metadata.len() + section.block.len()].copy_from_slice(&section.block.0);
    Ok(metadata.len() + section.block.len())
}
