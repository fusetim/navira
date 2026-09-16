use std::{ops::{Deref, DerefMut}, str::FromStr};
use cid::{Cid, multibase::Base};
use serde::{Deserialize, Serialize};

/// Bytes representation in DagJson
/// 
/// Will serialize like this:
/// ```json 
/// {"/": { "bytes": String /* Base64 encoded binary */ }}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bytes {
    bytes: Vec<u8>,
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Self {
        bytes.bytes
    }
}

 #[derive(Serialize, Deserialize)]
struct OuterBytes {
    #[serde(rename = "/")]
    inner: InnerBytes,
}

#[derive(Serialize, Deserialize)]
struct InnerBytes {
    bytes: String,
}

impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded_bytes = base64::encode(&self.bytes);
        let inner = InnerBytes {
            bytes: encoded_bytes,
        };
        let outer = OuterBytes { inner };
        outer.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let outer = OuterBytes::deserialize(deserializer)?;
        let decoded_bytes = base64::decode(outer.inner.bytes).map_err(serde::de::Error::custom)?;
        Ok(Bytes {
            bytes: decoded_bytes,
        })
    }
}

/// Link representation in DagJson
/// 
/// Will serialize like this:
/// ```json
/// {"/": String /* Base58 encoded CIDv0 or Multibase Base32 encoded CIDv1 */}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    cid: Cid,
}

impl Deref for Link {
    type Target = Cid;

    fn deref(&self) -> &Self::Target {
        &self.cid
    }
}

impl DerefMut for Link {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cid
    }
}

impl From<Cid> for Link {
    fn from(cid: Cid) -> Self {
        Self { cid }
    }
}

impl From<Link> for Cid {
    fn from(link: Link) -> Self {
        link.cid
    }
}

#[derive(Serialize, Deserialize)]
struct OuterLink {
    #[serde(rename = "/")]
    inner: String,
}

impl Serialize for Link {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let cid_str = if self.cid.version() == cid::Version::V0 {
            self.cid.to_string_of_base(Base::Base58Btc).unwrap()
        } else {
            self.cid.to_string_of_base(Base::Base32Lower).unwrap()
        };
        let outer = OuterLink { inner: cid_str };
        outer.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Link {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let outer = OuterLink::deserialize(deserializer)?;
        let cid = Cid::from_str(&outer.inner).map_err(serde::de::Error::custom)?;
        Ok(Link { cid })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_json_bytes() {
        let bytes = Bytes::from(vec![1, 2, 3, 4]);
        let json = serde_json::to_string(&bytes).unwrap();
        assert_eq!(json, r#"{"/":{"bytes":"AQIDBA=="}}"#);
    }

    #[test]
    fn deserialize_json_bytes() {
        let json = r#"{"/":{"bytes":"AQIDBA=="}}"#;
        let bytes: Bytes = serde_json::from_str(json).unwrap();
        assert_eq!(&*bytes, &[1, 2, 3, 4]);
    }

    #[test]
    fn serialize_json_link() {
        let cid = Cid::try_from("bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy").unwrap();
        let link = Link::from(cid);
        let json = serde_json::to_string(&link).unwrap();
        assert_eq!(json, r#"{"/":"bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy"}"#);
    }

    #[test]
    fn deserialize_json_link() {
        let json = r#"{"/":"bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy"}"#;
        let link: Link = serde_json::from_str(json).unwrap();
        assert_eq!(link.cid.to_string(), "bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy");
    }

    // TODO: Test with CIDv0 links
}