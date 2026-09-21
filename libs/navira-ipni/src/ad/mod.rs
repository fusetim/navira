pub mod ipld;
pub mod metadata;
use cid::multihash::Multihash;
use ipld::{Bytes, Link};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SignedHead {
    pub head: Link,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub pubkey: Bytes,
    pub sig: Bytes,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Advertisement {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_id: Option<Link>,
    pub provider: String,
    pub addresses: Vec<String>,
    pub signature: Bytes,
    pub entries: Link,
    pub context_id: Bytes,
    pub metadata: Bytes,
    pub is_rm: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_provider: Option<ExtendedProvider>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExtendedProvider {
    pub providers: Vec<Provider>,
    pub r#override: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub id: String,
    pub addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Bytes>,
    pub signature: Bytes,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EntryChunk {
    // List of entries (multihash)
    pub entries: Vec<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<Link>,
}

impl EntryChunk {
    pub fn with_hashes(hashes: Vec<Multihash<64>>, next: Option<Link>) -> Self {
        Self {
            entries: hashes
                .into_iter()
                .map(|h| Bytes::from(h.to_bytes()))
                .collect(),
            next,
        }
    }

    pub fn to_hashes(&self) -> Vec<Multihash<64>> {
        self.entries
            .iter()
            .map(|b| Multihash::from_bytes(&b.as_ref()).unwrap())
            .collect()
    }
}

impl Advertisement {
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;

    #[test]
    fn serialize_json_entry_chunk_empty() {
        let chunk = EntryChunk {
            entries: vec![],
            next: None,
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert_eq!(json, r#"{"entries":[]}"#);
    }

    #[test]
    fn serialize_json_entry_chunk_with_next() {
        let next_link = Link::from(
            Cid::try_from("bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy").unwrap(),
        );
        let chunk = EntryChunk {
            entries: vec![],
            next: Some(next_link),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert_eq!(
            json,
            r#"{"entries":[],"next":{"/":"bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy"}}"#
        );
    }

    #[test]
    fn serialize_json_entry_chunk_with_entries() {
        let hash1 = Multihash::wrap(0x12, &[1, 2, 3, 4]).unwrap();
        let hash2 = Multihash::wrap(0x12, &[5, 6, 7, 8]).unwrap();
        let chunk = EntryChunk::with_hashes(vec![hash1.clone(), hash2.clone()], None);
        let json = serde_json::to_string(&chunk).unwrap();
        assert_eq!(
            json,
            r#"{"entries":[{"/":{"bytes":"EgQBAgME"}},{"/":{"bytes":"EgQFBgcI"}}]}"#
        );
    }

    #[test]
    fn serialize_json_entry_chunk_with_entries_and_next() {
        let hash1 = Multihash::wrap(0x12, &[1, 2, 3, 4]).unwrap();
        let hash2 = Multihash::wrap(0x12, &[5, 6, 7, 8]).unwrap();
        let next_link = Link::from(
            Cid::try_from("bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy").unwrap(),
        );
        let chunk = EntryChunk::with_hashes(vec![hash1.clone(), hash2.clone()], Some(next_link));
        let json = serde_json::to_string(&chunk).unwrap();
        assert_eq!(
            json,
            r#"{"entries":[{"/":{"bytes":"EgQBAgME"}},{"/":{"bytes":"EgQFBgcI"}}],"next":{"/":"bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy"}}"#
        );
    }

    #[test]
    fn serialize_json_advertisement_simple() {
        let ad = Advertisement {
            previous_id: None,
            provider: "provider1.peerId".to_string(),
            addresses: vec!["/ip4/127.0.0.1/tcp/8080".to_owned()],
            signature: Bytes::from(vec![1, 2, 3]),
            entries: Link::from(
                Cid::try_from("bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy")
                    .unwrap(),
            ),
            context_id: Bytes::from(vec![4, 5, 6]),
            metadata: Bytes::from(vec![7, 8, 9]),
            is_rm: false,
            extended_provider: None,
        };
        let json = serde_json::to_string(&ad).unwrap();
        assert_eq!(
            json,
            r#"{"provider":"provider1.peerId","addresses":["/ip4/127.0.0.1/tcp/8080"],"signature":{"/":{"bytes":"AQID"}},"entries":{"/":"bafybeibiah4lt72jetyjhdf26d2twq7gqcenxytsy2ifzdl2viq2m4icuy"},"context_id":{"/":{"bytes":"BAUG"}},"metadata":{"/":{"bytes":"BwgJ"}},"is_rm":false}"#
        );
    }

    // TODO: Add additional tests for advertisements more complex.

    // TODO: Test deserialization of advertisements.
}
