use std::ops::Deref;

use super::RawCid;
use ciborium::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

/// RawLink is the equivalent of a IPLD Link in the context of CAR files.
///
/// Link is essentially a wrapper around a CID, and for historical reasons, both exists
/// separately in the specs.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawLink(RawCid);

impl RawLink {
    /// Creates a new Link from a RawCid
    pub fn new(cid: RawCid) -> Self {
        RawLink(cid)
    }

    /// Returns a reference to the underlying RawCid
    pub fn to_raw_cid(&self) -> &RawCid {
        &self.0
    }
}

pub trait IntoRawLink {
    fn into_link(self) -> RawLink;
}

impl IntoRawLink for RawLink {
    fn into_link(self) -> RawLink {
        self
    }
}

impl IntoRawLink for RawCid {
    fn into_link(self) -> RawLink {
        RawLink(self)
    }
}

impl Deref for RawLink {
    type Target = RawCid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for RawLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Link({})", self.to_hex())
    }
}

impl std::fmt::Display for RawLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Link({})", self.to_hex())
    }
}

impl Serialize for RawLink {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut cid_bytes = self.0.bytes().to_vec();
        // Preprend the multihash 0x00 (base 256) to indicate that this is a raw CID, as per the IPLD specification for raw CIDs in Links.
        cid_bytes.insert(0, 0x00);
        let value = Value::Tag(42, Box::new(Value::Bytes(cid_bytes)));
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RawLink {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if let Value::Tag(42, boxed_value) = value {
            if let Value::Bytes(bytes) = *boxed_value {
                // Remove the leading 0x00 byte before creating the RawCid
                return Ok(RawLink(RawCid::new(bytes[1..].to_vec())));
            }
        }
        Err(D::Error::custom("Invalid CID format"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::RawCid;
    use super::*;

    #[test]
    fn test_link_serialization() {
        let link = RawLink(RawCid::new(vec![0x01, 0x55, 0x02, 0x03, 0x04]));

        let mut buf = Vec::new();
        ciborium::ser::into_writer(&link, &mut buf).unwrap();
        let expected = vec![0xD8, 0x2A, 0x46, 0x00, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 42 + prepended 0x00
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_link_deserialization() {
        let data = vec![0xD8, 0x2A, 0x46, 0x00, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 42 + prepended 0x0
        let link: RawLink = ciborium::de::from_reader(data.as_slice()).unwrap();
        let expected = RawLink(RawCid::new(vec![0x01, 0x55, 0x02, 0x03, 0x04]));
        assert_eq!(link, expected);
    }
}
