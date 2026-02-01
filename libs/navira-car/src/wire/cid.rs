use ciborium::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::wire::varint::UnsignedVarint;

/// Represents a raw CID (Content Identifier) in byte format
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawCid(Vec<u8>);

impl RawCid {
    /// Creates a new RawCid from a vector of bytes
    pub fn new(bytes: Vec<u8>) -> Self {
        RawCid(bytes)
    }

    /// Creates a RawCid from a hexadecimal string representation
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        let bytes = hex::decode(hex_str)?;
        Ok(RawCid::new(bytes))
    }

    /// Returns the byte representation of the RawCid
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

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
        write!(f, "RawCid({})", hex::encode(&self.0))
    }
}

impl std::fmt::Display for RawCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawCid({})", hex::encode(&self.0))
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

#[derive(thiserror::Error, Debug)]
pub enum CidFormatError {
    #[error("Insufficient data for CID")]
    InsufficientData,
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
