use std::{
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
};

use crate::wire::{
    cid::{CidFormatError, RawCid},
    varint::UnsignedVarint,
};

/// CARv1 Block, just a simple wrapper over the raw bytes of the block
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CarBlock(pub Vec<u8>);

impl Deref for CarBlock {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CarBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// CARv1 Section metadata
///
/// Simple wrapper over the "header" of a CARv1 Section, which is the
/// length and the CID of the block.
#[derive(Clone, PartialEq, Eq)]
pub struct CarSectionMetadata {
    /// The length of the block data in bytes
    ///
    /// IMPORTANT: Contrary to the in-file length prefix, this length does not
    /// include the length of the CID itself.
    pub length: u64,
    /// The CID of the block
    pub cid: RawCid,
}

/// CARv1 Section
///
/// A CARv1 Section is a the storage representation of a block with its metadata
/// inside a CARv1 file. It consists of a length prefix, a CID, and the block data.
#[derive(Clone, PartialEq, Eq)]
pub struct CarSection {
    pub metadata: CarSectionMetadata,
    pub block: CarBlock,
}

impl CarSection {
    /// Creates a new CARv1 Section from the given metadata and block
    pub fn new(metadata: CarSectionMetadata, block: CarBlock) -> Self {
        CarSection { metadata, block }
    }

    /// Get the raw CID of the block
    pub fn cid(&self) -> &RawCid {
        &self.metadata.cid
    }

    /// Get the length of the block data in bytes
    pub fn length(&self) -> u64 {
        self.metadata.length
    }
}

impl CarSectionMetadata {
    /// Creates a new CARv1 Section metadata from the given length and CID
    pub fn new(length: u64, cid: RawCid) -> Self {
        CarSectionMetadata { length, cid }
    }

    /// Try to deserialize a CARv1 section metadata from a byte slice.
    ///
    /// ## Returns
    /// - `Ok((CarSectionMetadata, bytes_read))` if deserialization is successful,
    /// - `Err(CarSectionDeserialiationError)` otherwise,
    ///     if CarSectionDeserialiationError::InsufficientData is returned, it means that the input
    ///     bytes are not enough to read a complete section metadata.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<(Self, usize), CarSectionDeserialiationError> {
        // First read the length prefix as a varint
        let Some((length_varint, lv_read)) = UnsignedVarint::decode(bytes) else {
            return Err(CarSectionDeserialiationError::InsufficientData { hint: None });
        };

        // Then read the CID from the remaining bytes
        let cid_start = lv_read;
        let (cid, cid_read) = RawCid::try_read_bytes(&bytes[cid_start..]).map_err(|e| match e {
            CidFormatError::UnsupportedVersion => CarSectionDeserialiationError::UnsupportedCid,
            CidFormatError::InsufficientData => {
                CarSectionDeserialiationError::InsufficientData { hint: None }
            }
        })?;

        Ok((
            CarSectionMetadata {
                length: length_varint.0 - (cid_read as u64),
                cid,
            },
            lv_read + cid_read,
        ))
    }

    /// Serialize the CARv1 section metadata into a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write the length prefix as a varint
        let length_varint = UnsignedVarint(self.length + (self.cid.bytes().len() as u64));
        bytes.extend(length_varint.encode());
        bytes.extend(self.cid.bytes());
        return bytes;
    }
}

impl CarSection {
    /// Serialize the CARv1 section into a byte vector.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.metadata.to_bytes());
        bytes.extend(&self.block.0);
        return bytes;
    }

    /// Try to deserialize a CARv1 section from a byte slice.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<(Self, usize), CarSectionDeserialiationError> {
        let (metadata, metadata_read) = CarSectionMetadata::try_from_bytes(bytes)?;
        let block_start = metadata_read;
        let block_end = block_start + (metadata.length as usize);
        if bytes.len() < block_end {
            return Err(CarSectionDeserialiationError::InsufficientData {
                hint: NonZeroUsize::new(block_end - bytes.len()),
            });
        }
        let block = CarBlock(bytes[block_start..block_end].to_vec());
        Ok((CarSection { metadata, block }, block_end))
    }
}

/// Errors related to deserialization of CARv1 sections
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
        /// If zero, it means that no hint can be provided.
        hint: Option<NonZeroUsize>,
    },
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use cid::{Cid, multihash::Multihash};

    use super::*;

    #[test]
    fn test_car_section_metadata_serialization_identity() {
        // Identity CID: bafkqaaa (CIDv1, raw, multihash identity)
        // CID bytes: <cidv1><codec=raw><multihash code=identity><multihash: length=0>
        let identity = Cid::new_v1(0x55, Multihash::wrap(0x00, &[]).unwrap());
        assert_eq!(
            identity
                .to_string_of_base(cid::multibase::Base::Base32Lower)
                .unwrap(),
            "bafkqaaa"
        );
        assert_eq!(identity.to_bytes(), vec![0x01, 0x55, 0x00, 0x00]);

        let metadata = CarSectionMetadata {
            length: 0,
            cid: identity.into(),
        };
        let encoded = metadata.to_bytes();
        assert_eq!(encoded, vec![0x04, 0x01, 0x55, 0x00, 0x00]);
    }

    #[test]
    fn test_car_section_metadata_deserialization_identity() {
        let raw = vec![0x04, 0x01, 0x55, 0x00, 0x00];
        let (metadata, bytes_read) = CarSectionMetadata::try_from_bytes(&raw).unwrap();
        assert_eq!(bytes_read, raw.len()); // All bytes should be consumed
        assert_eq!(metadata.length, 0); // Block length is 0
        assert_eq!(metadata.cid.bytes(), [0x01, 0x55, 0x00, 0x00]); // CID bytes match    
    }

    #[test]
    fn test_car_section_metadata_serialize_realblock_cidv0() {
        // Example CIDv0: QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D
        let cidv0 = Cid::try_from("QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D").unwrap();
        let metadata = CarSectionMetadata {
            length: 1234,
            cid: cidv0.into(),
        };
        assert_eq!(
            metadata.cid.to_hex(),
            "1220422896a1ce82a7b1cc0ba27c7d8de2886c7df95588473d5e88a28a9fcfa0e43e"
        );
        let encoded = metadata.to_bytes();
        assert_eq!(
            encoded,
            vec![
                0xf4, 0x09, 0x12, 0x20, 0x42, 0x28, 0x96, 0xA1, 0xCE, 0x82, 0xA7, 0xB1, 0xCC, 0x0B,
                0xA2, 0x7C, 0x7D, 0x8D, 0xE2, 0x88, 0x6C, 0x7D, 0xF9, 0x55, 0x88, 0x47, 0x3D, 0x5E,
                0x88, 0xA2, 0x8A, 0x9F, 0xCF, 0xA0, 0xE4, 0x3E
            ]
        );
    }

    #[test]
    fn test_car_section_metadata_serialize_realblock_cidv1() {
        // Example CIDv0: QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D
        let cidv0 = Cid::try_from("QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D").unwrap();
        let cidv1 = cidv0.into_v1().unwrap();
        let metadata = CarSectionMetadata {
            length: 1234,
            cid: cidv1.into(),
        };
        assert_eq!(
            metadata.cid.to_hex(),
            "01701220422896a1ce82a7b1cc0ba27c7d8de2886c7df95588473d5e88a28a9fcfa0e43e"
        );
        let encoded = metadata.to_bytes();
        assert_eq!(
            encoded,
            vec![
                0xf6, 0x09, 0x01, 0x70, 0x12, 0x20, 0x42, 0x28, 0x96, 0xA1, 0xCE, 0x82, 0xA7, 0xB1,
                0xCC, 0x0B, 0xA2, 0x7C, 0x7D, 0x8D, 0xE2, 0x88, 0x6C, 0x7D, 0xF9, 0x55, 0x88, 0x47,
                0x3D, 0x5E, 0x88, 0xA2, 0x8A, 0x9F, 0xCF, 0xA0, 0xE4, 0x3E
            ]
        );
    }

    #[test]
    fn test_car_section_metadata_deserialize_realblock_cidv1() {
        let raw = vec![
            0xf6, 0x09, 0x01, 0x70, 0x12, 0x20, 0x42, 0x28, 0x96, 0xA1, 0xCE, 0x82, 0xA7, 0xB1,
            0xCC, 0x0B, 0xA2, 0x7C, 0x7D, 0x8D, 0xE2, 0x88, 0x6C, 0x7D, 0xF9, 0x55, 0x88, 0x47,
            0x3D, 0x5E, 0x88, 0xA2, 0x8A, 0x9F, 0xCF, 0xA0, 0xE4, 0x3E,
        ];
        let (metadata, bytes_read) = CarSectionMetadata::try_from_bytes(&raw).unwrap();
        assert_eq!(bytes_read, raw.len()); // All bytes should be consumed
        assert_eq!(metadata.length, 1234); // Block length is 1234
        assert_eq!(
            metadata.cid.to_hex(),
            "01701220422896a1ce82a7b1cc0ba27c7d8de2886c7df95588473d5e88a28a9fcfa0e43e"
        ); // CID hex matches
    }

    #[test]
    fn test_car_section_metadata_deserialize_realblock_cidv0() {
        let raw = vec![
            0xf4, 0x09, 0x12, 0x20, 0x42, 0x28, 0x96, 0xA1, 0xCE, 0x82, 0xA7, 0xB1, 0xCC, 0x0B,
            0xA2, 0x7C, 0x7D, 0x8D, 0xE2, 0x88, 0x6C, 0x7D, 0xF9, 0x55, 0x88, 0x47, 0x3D, 0x5E,
            0x88, 0xA2, 0x8A, 0x9F, 0xCF, 0xA0, 0xE4, 0x3E,
        ];
        let (metadata, bytes_read) = CarSectionMetadata::try_from_bytes(&raw).unwrap();
        assert_eq!(bytes_read, raw.len()); // All bytes should be consumed
        assert_eq!(metadata.length, 1234); // Block length is 1234
        assert_eq!(
            metadata.cid.to_hex(),
            "1220422896a1ce82a7b1cc0ba27c7d8de2886c7df95588473d5e88a28a9fcfa0e43e"
        ); // CID hex matches
        assert_eq!(
            Cid::try_from(metadata.cid).unwrap().to_string(),
            "QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D"
        ); // CID string matches
    }

    // TODO: Test partial deserialization of section metadata, with insufficient
    // data errors and hints.

    // TODO: test car section serialization and deserialization, with real block data.
}
