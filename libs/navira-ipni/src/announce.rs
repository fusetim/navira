use cid::Cid;
use multiaddr::Multiaddr;
use navira_ser::ipld::Link;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announce {
    cid: Cid,
    addresses: Vec<Multiaddr>,
    extra: Vec<u8>,
    orig_peer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EncodedAnnounce {
    #[serde(rename = "Cid")]
    cid: Link,
    #[serde(rename = "Addrs")]
    addresses: Vec<Vec<u8>>,
    #[serde(rename = "ExtraData", skip_serializing_if = "Vec::is_empty")]
    extra: Vec<u8>,
    #[serde(rename = "OrigPeer", skip_serializing_if = "Option::is_none")]
    orig_peer: Option<String>,
}

impl Announce {
    pub fn new(
        cid: Cid,
        addresses: Vec<Multiaddr>,
        extra: Vec<u8>,
        orig_peer: Option<String>,
    ) -> Self {
        Self {
            cid,
            addresses,
            extra,
            orig_peer,
        }
    }

    fn to_encoded(&self) -> EncodedAnnounce {
        EncodedAnnounce {
            cid: Link::from(self.cid),
            addresses: self.addresses.iter().map(|addr| addr.to_vec()).collect(),
            extra: self.extra.clone(),
            orig_peer: self.orig_peer.clone(),
        }
    }

    fn from_encoded(encoded: EncodedAnnounce) -> Result<Self, String> {
        let cid = (*encoded.cid).clone();
        let addresses = encoded
            .addresses
            .into_iter()
            .map(|addr_bytes| {
                Multiaddr::try_from(addr_bytes)
                    .map_err(|e| format!("Failed to convert bytes to Multiaddr: {}", e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            cid,
            addresses,
            extra: encoded.extra,
            orig_peer: encoded.orig_peer,
        })
    }
}

impl Serialize for Announce {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let encoded = self.to_encoded();
        encoded.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Announce {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = EncodedAnnounce::deserialize(deserializer)?;
        Announce::from_encoded(encoded).map_err(serde::de::Error::custom)
    }
}
