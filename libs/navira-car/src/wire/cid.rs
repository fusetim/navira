//! CID (Content Identifier) handling for CAR files.
//!
//! A CAR archive contains CIDs that identify the content of the blocks in the archive.
//! However, in most contexts (outside of validation), there is no need to actually parse the
//! CIDs, but just to treat them as opaque byte sequences.
//!
//! This module provides the [RawCid] struct, which is a simple wrapper around a byte vector
//! that represents a CID in its raw binary form.
//!
//! However, it also provides a method to try to parse a CID from a byte stream, which can be useful
//! for validating that the bytes conform to the expected structure of a CID (e.g., CIDv0 or CIDv1)
//! without needing to fully understand the internal structure of the CID (e.g., multihash coherence).
//!
//! ***TODO:** In the future, we will add the conversion fuctions to convert between RawCid and a
//! more structured CID type (e.g., using the [cid crate](https://crates.io/crates/cid)) to make CAR operations easier.*

use ciborium::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::wire::varint::UnsignedVarint;

/// Raw CID (Content Identifier), basically a dumb wrapper around a byte vector.
///
/// This struct is used to represent CIDs in their raw byte form, without any parsing or interpretation.
/// It can be used to store and manipulate CIDs as opaque byte sequences, which is useful for handling CIDs
/// in CAR files without needing to understand their internal structure.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawCid(Vec<u8>);

impl RawCid {
    /// Creates a new RawCid from a vector of bytes
    ///
    /// This function does not perform any validation on the input bytes, it will
    /// just wrap the bytes in a RawCid struct.
    pub fn new(bytes: Vec<u8>) -> Self {
        RawCid(bytes)
    }

    /// Creates a RawCid from a hexadecimal string representation
    ///
    /// The input string should be a valid hexadecimal representation of the CID bytes.
    /// If the input string is not a valid hexadecimal string, it will return an error.
    ///
    /// Importantly, as [RawCid::new], this function does not perform any validation on the
    /// content of the bytes, it will just decode the hex string and wrap the resulting bytes in a RawCid struct.
    ///
    /// ## Returns
    /// - `Ok(RawCid)` if the input string is successfully parsed into bytes and wrapped in a RawCid struct.
    /// - `Err(hex::FromHexError)` if the input string is not a valid hexadecimal string.
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        let bytes = hex::decode(hex_str)?;
        Ok(RawCid::new(bytes))
    }

    /// Returns the byte representation of the RawCid
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the hexadecimal string representation of the RawCid bytes
    ///
    /// This function encodes the raw bytes of the CID into a hexadecimal string,
    /// which can be useful for debugging or display purposes.
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    /// Tries to read a properly formed CID from the given bytes
    ///
    /// This function attempts to parse the input bytes as a CID, supporting both CIDv0 and CIDv1 formats.
    /// It acts as a dumb parser, therefore it does not validate the multihash coherence, but only that
    /// the CID conforms to the expected binary structure of either CIDv0 or CIDv1.
    ///
    /// ## Returns
    /// - `Ok((RawCid, bytes_read))` if the input bytes contain a valid CID, where
    ///   `RawCid` is the parsed CID and `bytes_read` is the number of bytes consumed during parsing.
    /// - `Err(CidFormatError)` if the input bytes do not represent a valid CID (e.g., insufficient data, unsupported version).
    ///
    /// ## Examples
    /// ```
    /// use navira_car::wire::cid::RawCid;
    /// // Test with binary data representing a CIDv0 (DagProtobuf, SHA256-256, 32 bytes hash)
    /// let cidv0_bytes = hex::decode("12200E7071C59DF3B9454D1D18A15270AA36D54F89606A576DC621757AFD44AD1D2E").unwrap();
    /// let (parsed_cidv0, size_v0) = RawCid::try_read_bytes(&cidv0_bytes).unwrap();
    /// assert_eq!(size_v0, 34);
    /// assert_eq!(parsed_cidv0.bytes(), &cidv0_bytes[..34]);
    /// ```
    pub fn try_read_bytes(bytes: &[u8]) -> Result<(Self, usize), CidFormatError> {
        if bytes.len() < 2 {
            return Err(CidFormatError::InsufficientData);
        }
        // Handle CIDv0 (DagProtobuf, SHA256-256, 32 bytes hash) - prefix Qm...
        if bytes.starts_with(&[0x12, 0x20]) {
            if bytes.len() < 34 {
                return Err(CidFormatError::InsufficientData);
            }
            let cid_bytes = bytes[..34].to_vec();
            return Ok((RawCid::new(cid_bytes), 34));
        }
        // Handle CIDv1 (multibase, multicodec, multihash)
        if bytes[0] == 0x01 {
            // Read the multicodec
            let (_multicodec, mc_size) = match UnsignedVarint::decode(&bytes[1..]) {
                Some((mc, size)) => (mc.0, size),
                None => return Err(CidFormatError::InsufficientData),
            };
            // Read the multihash
            let mh_start = 1 + mc_size;
            let (_mh_code, mh_code_size) = match UnsignedVarint::decode(&bytes[mh_start..]) {
                Some((code, size)) => (code.0, size),
                None => return Err(CidFormatError::InsufficientData),
            };
            let mh_len_start = mh_start + mh_code_size;
            let (mh_len, mh_len_size) = match UnsignedVarint::decode(&bytes[mh_len_start..]) {
                Some((len, size)) => (len.0 as usize, size),
                None => return Err(CidFormatError::InsufficientData),
            };
            let total_cid_size = 1 + mc_size + mh_code_size + mh_len_size + mh_len;
            if bytes.len() < total_cid_size {
                return Err(CidFormatError::InsufficientData);
            }
            let cid_bytes = bytes[..total_cid_size].to_vec();
            return Ok((RawCid::new(cid_bytes), total_cid_size));
        }
        // Otherwise it is not supported yet
        Err(CidFormatError::UnsupportedVersion)
    }
}

impl std::fmt::Debug for RawCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawCid({})", self.to_hex())
    }
}

impl std::fmt::Display for RawCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawCid({})", self.to_hex())
    }
}

impl Serialize for RawCid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = Value::Tag(42, Box::new(Value::Bytes(self.0.clone())));
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RawCid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if let Value::Tag(42, boxed_value) = value {
            if let Value::Bytes(bytes) = *boxed_value {
                return Ok(RawCid::new(bytes));
            }
        }
        Err(D::Error::custom("Invalid CID format"))
    }
}

/// Errors related to CID parsing
#[derive(thiserror::Error, Debug)]
pub enum CidFormatError {
    /// Indicates that there is not enough data to parse a complete CID from the input bytes.
    ///
    /// This error generally indicate the byte stream provided was too short to contain a valid CID,
    /// either because it is truncated or because it does not conform to the expected structure of a CID.
    ///
    /// Either way, you can try to provide more bytes (until you have a complete CID) or
    /// propagate the error up the call stack (for instance if you believe it will never be a valid CID).
    #[error("Insufficient data for CID")]
    InsufficientData,

    /// Indicates that the CID version specified in the input bytes is not supported by the parser.
    ///
    /// This error generally indicates that the input bytes start with a CID version prefix that the parser
    /// does not recognize or support.
    ///
    /// Currently, the parser only supports:
    ///
    /// * CIDv0 (prefix 0x12 0x20)
    /// * CIDv1 (prefix 0x01 followed by varints)
    ///
    /// So if the input bytes do not match either of these patterns, this error will be returned.
    #[error("Unsupported CID version")]
    UnsupportedVersion,
}

#[cfg(test)]
mod tests {
    use super::RawCid;

    #[test]
    fn test_raw_cid_serialization() {
        let raw_cid = RawCid::new(vec![0x01, 0x55, 0x02, 0x03, 0x04]);

        let mut buf = Vec::new();
        ciborium::ser::into_writer(&raw_cid, &mut buf).unwrap();
        let expected = vec![0xD8, 0x2A, 0x45, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 42
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_raw_cid_deserialization() {
        let data = vec![0xD8, 0x2A, 0x45, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 42
        let raw_cid: RawCid = ciborium::de::from_reader(data.as_slice()).unwrap();
        let expected = RawCid::new(vec![0x01, 0x55, 0x02, 0x03, 0x04]);
        assert_eq!(raw_cid, expected);
    }

    #[test]
    fn test_raw_cid_deserialization_invalid_tag() {
        let invalid_cid_data = vec![0xD8, 0x1A, 0x45, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 1 instead of 42
        let result: Result<RawCid, _> = ciborium::de::from_reader(invalid_cid_data.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn test_raw_cid_bin_parsing_cidv0() {
        let cidv0_bytes =
            hex::decode("12200E7071C59DF3B9454D1D18A15270AA36D54F89606A576DC621757AFD44AD1D2E")
                .unwrap();
        let (parsed_cidv0, size_v0) = RawCid::try_read_bytes(&cidv0_bytes).unwrap();
        assert_eq!(size_v0, 34);
        assert_eq!(parsed_cidv0.bytes(), &cidv0_bytes[..34]);
    }

    #[test]
    fn test_raw_cid_bin_parsing_cidv1() {
        let cidv1_bytes = vec![
            1, 112, 18, 32, 44, 95, 104, 130, 98, 224, 236, 232, 86, 154, 166, 249, 77, 96, 170,
            213, 92, 168, 217, 216, 55, 52, 228, 167, 67, 13, 12, 255, 101, 136, 236, 43,
        ];
        let (parsed_cidv1, size_v1) = RawCid::try_read_bytes(&cidv1_bytes).unwrap();
        assert_eq!(size_v1, cidv1_bytes.len());
        assert_eq!(parsed_cidv1.bytes(), &cidv1_bytes[..]);
    }

    #[test]
    fn test_raw_cid_bin_parsing_cidv1_insufficient() {
        let cidv1_bytes = vec![
            1, 112, 18, 32, 44, 95, 104, 130, 98, 224, 236, 232, 86, 154, 166, 249, 77, 96, 170,
            213, 92, 168, 217, 216, 55, 52, 228, 167, 67, 13, 12, 255, 101, 136,
        ];
        let result = RawCid::try_read_bytes(&cidv1_bytes);
        assert!(matches!(
            result,
            Err(super::CidFormatError::InsufficientData)
        ));
    }
}
