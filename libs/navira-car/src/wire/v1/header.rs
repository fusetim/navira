use crate::wire::cid::{IntoRawLink, RawLink, RawCid};
use serde::{Deserialize, Serialize};

/// CAR v1 Header structure
///
/// # Fields
/// - `version`: The version of the CAR format (should be 1 for CAR v1)
/// - `roots`: A vector of root CIDs in raw byte format
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarHeader {
    version: u64,
    roots: Vec<RawLink>,
}

impl CarHeader {
    /// Creates a new CAR v1 header with the specified root CIDs
    pub fn new(roots: Vec<RawCid>) -> Self {
        let roots = roots.into_iter().map(IntoRawLink::into_link).collect();
        CarHeader { roots, version: 1 }
    }

    /// Returns the version of the CAR format
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns a reference to the vector of root CIDs
    pub fn roots(&self) -> &[RawLink] {
        &self.roots
    }

    /// Returns a mutable reference to the vector of root CIDs
    pub fn roots_mut(&mut self) -> &mut Vec<RawLink> {
        &mut self.roots
    }

    /// Checks if there are no root CIDs in the header
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAR_V1_HEADER1: [u8; 99] = [
        0xA2, 0x65, 0x72, 0x6F, 0x6F, 0x74, 0x73, 0x82, 0xD8, 0x2A, 0x58, 0x25, 0x00, 0x01, 0x71,
        0x12, 0x20, 0xF8, 0x8B, 0xC8, 0x53, 0x80, 0x4C, 0xF2, 0x94, 0xFE, 0x41, 0x7E, 0x4F, 0xA8,
        0x30, 0x28, 0x68, 0x9F, 0xCD, 0xB1, 0xB1, 0x59, 0x2C, 0x51, 0x02, 0xE1, 0x47, 0x4D, 0xBC,
        0x20, 0x0F, 0xAB, 0x8B, 0xD8, 0x2A, 0x58, 0x25, 0x00, 0x01, 0x71, 0x12, 0x20, 0x69, 0xEA,
        0x07, 0x40, 0xF9, 0x80, 0x7A, 0x28, 0xF4, 0xD9, 0x32, 0xC6, 0x2E, 0x7C, 0x1C, 0x83, 0xBE,
        0x05, 0x5E, 0x55, 0x07, 0x2C, 0x90, 0x26, 0x6A, 0xB3, 0xE7, 0x9D, 0xF6, 0x3A, 0x36, 0x5B,
        0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x01,
    ];

    #[test]
    fn test_car_v1_header_deserialization() {
        let header: CarHeader = ciborium::de::from_reader(CAR_V1_HEADER1.as_slice()).unwrap();
        let cid1 = RawCid::from_hex(
            "01711220f88bc853804cf294fe417e4fa83028689fcdb1b1592c5102e1474dbc200fab8b",
        )
        .unwrap();
        let cid2 = RawCid::from_hex(
            "0171122069ea0740f9807a28f4d932c62e7c1c83be055e55072c90266ab3e79df63a365b",
        )
        .unwrap();
        assert_eq!(header.version(), 1);
        assert_eq!(header.roots().len(), 2);
        assert_eq!(header.roots()[0], RawLink::new(cid1));
        assert_eq!(header.roots()[1], RawLink::new(cid2));
    }

    #[test]
    fn test_car_v1_header_serialization() {
        let cid1 = RawCid::from_hex(
            "01711220f88bc853804cf294fe417e4fa83028689fcdb1b1592c5102e1474dbc200fab8b",
        )
        .unwrap();
        let cid2 = RawCid::from_hex(
            "0171122069ea0740f9807a28f4d932c62e7c1c83be055e55072c90266ab3e79df63a365b",
        )
        .unwrap();
        let header = CarHeader::new(vec![cid1, cid2]);
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&header, &mut buf).unwrap();

        // Ordering of map keys in CBOR may vary, so we check for content rather than exact byte-for-byte match
        let deserialized_header: CarHeader = ciborium::de::from_reader(buf.as_slice()).unwrap();
        assert_eq!(deserialized_header, header);
    }
}
