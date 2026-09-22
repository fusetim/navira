use std::{ops::Deref, unimplemented};

use cid::{Cid, multibase, multibase::Base, multihash::Multihash};

pub const MULTICODEC_LIBP2P_KEY: u64 = 0x72;
pub const MULTIHASH_CODE_IDENTITY: u64 = 0x00;
pub const MULTIHASH_CODE_SHA256: u64 = 0x12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerId(Cid);

impl Deref for PeerId {
    type Target = Cid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToString for PeerId {
    fn to_string(&self) -> String {
        self.to_legacy()
    }
}

impl PeerId {
    pub fn new(public_key: &[u8]) -> Self {
        let hash = if public_key.len() > 42 {
            // Public key is too long,
            // we need to hash it using sha256
            unimplemented!("Hashing public keys is not yet implemented")
        } else {
            Multihash::wrap(MULTIHASH_CODE_IDENTITY, public_key).unwrap()
        };
        Self(Cid::new_v1(MULTICODEC_LIBP2P_KEY, hash))
    }

    pub fn from_cid(cid: Cid) -> Result<Self, ()> {
        if cid.codec() != MULTICODEC_LIBP2P_KEY {
            return Err(());
        }
        Ok(Self(cid))
    }

    pub fn to_legacy(&self) -> String {
        // Encode using Base58Btc (and remove the multibase prefix `z`)
        multibase::encode(Base::Base58Btc, self.0.hash().to_bytes())[1..].to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    Ed25519 = 0,
    RSA = 1,
    Secp256k1 = 2,
    ECDSA = 3,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedPublicKey {
    key_type: KeyType,
    key_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedPrivateKey {
    key_type: KeyType,
    key_bytes: Vec<u8>,
}

impl EncodedPublicKey {
    pub fn new(key_type: KeyType, key_bytes: Vec<u8>) -> Self {
        Self {
            key_type,
            key_bytes,
        }
    }

    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    pub fn key_bytes(&self) -> &[u8] {
        &self.key_bytes
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_key(self.key_type, &self.key_bytes)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, ()> {
        let (key_type, key_bytes) = decode_key(encoded)?;
        Ok(Self {
            key_type,
            key_bytes,
        })
    }
}

impl EncodedPrivateKey {
    pub fn new(key_type: KeyType, key_bytes: Vec<u8>) -> Self {
        Self {
            key_type,
            key_bytes,
        }
    }

    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    pub fn key_bytes(&self) -> &[u8] {
        &self.key_bytes
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_key(self.key_type, &self.key_bytes)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, ()> {
        let (key_type, key_bytes) = decode_key(encoded)?;
        Ok(Self {
            key_type,
            key_bytes,
        })
    }
}

fn encode_key(key_type: KeyType, key_bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![];
    // First, encode the key tag, protobuf varint (0-3)
    encoded.push(0x08);
    encoded.push(key_type as u8);
    // Then, encode the key bytes as a length-delimited field
    encoded.push(0x12);
    encoded.push(key_bytes.len() as u8);
    encoded.extend_from_slice(key_bytes);
    encoded
}

fn decode_key(encoded: &[u8]) -> Result<(KeyType, Vec<u8>), ()> {
    // Decode the protobuf-encoded private key
    // Field order is fixed!
    if encoded.len() < 3 {
        return Err(());
    }
    if encoded[0] != 0x08 {
        return Err(());
    }
    let key_type = match encoded[1] {
        0 => KeyType::Ed25519,
        1 => KeyType::RSA,
        2 => KeyType::Secp256k1,
        3 => KeyType::ECDSA,
        _ => return Err(()),
    };
    if encoded[2] != 0x12 {
        return Err(());
    }
    let key_bytes_len = encoded[3] as usize;
    if encoded.len() < 4 + key_bytes_len {
        return Err(());
    }
    let key_bytes = encoded[4..4 + key_bytes_len].to_vec();
    Ok((key_type, key_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ED25519_PRIVATE_KEY_ENCODED_HEX: &'static str = "080112407e0830617c4a7de83925dfb2694556b12936c477a0e1feb2e148ec9da60fee7d1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e";
    const ED25519_PUBLIC_KEY_ENCODED_HEX: &'static str =
        "080112201ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e";

    #[test]
    fn test_peerid_decode_encode_private_key() {
        let private_key_bytes = hex::decode(ED25519_PRIVATE_KEY_ENCODED_HEX).unwrap();
        let encoded_private_key = EncodedPrivateKey::decode(&private_key_bytes).unwrap();
        assert_eq!(encoded_private_key.key_type(), KeyType::Ed25519);
        let re_encoded = encoded_private_key.encode();
        assert_eq!(re_encoded, private_key_bytes);
    }

    #[test]
    fn test_peerid_decode_encode_public_key() {
        let public_key_bytes = hex::decode(ED25519_PUBLIC_KEY_ENCODED_HEX).unwrap();
        let encoded_public_key = EncodedPublicKey::decode(&public_key_bytes).unwrap();
        assert_eq!(encoded_public_key.key_type(), KeyType::Ed25519);
        let re_encoded = encoded_public_key.encode();
        assert_eq!(re_encoded, public_key_bytes);
    }
}
