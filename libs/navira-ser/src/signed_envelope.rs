use navira_peer::identity::{
    SignCheckError,
    ed25519::{self, Ed25519Identity},
};

use crate::varint::UnsignedVarint;

// Envelope encloses a signed payload produced by a peer, along with the public
// key of the keypair it was signed with so that it can be statelessly validated
// by the receiver.
//
// The payload is prefixed with a byte string that determines the type, so it
// can be deserialized deterministically. Often, this byte string is a
// multicodec.
// message Envelope {
//   // public_key is the public key of the keypair the enclosed payload was
//   // signed with.
//   PublicKey public_key = 1;
//
//   // payload_type encodes the type of payload, so that it can be deserialized
//   // deterministically.
//   bytes payload_type = 2;
//
//   // payload is the actual payload carried inside this envelope.
//   bytes payload = 3;
//
//   // signature is the signature produced by the private key corresponding to
//   // the enclosed public key, over the payload, prefixing a domain string for
//   // additional security.
//   bytes signature = 5;
// }
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedEnvelope {
    key: Vec<u8>,
    payload_type: Vec<u8>,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

impl SignedEnvelope {
    pub fn new<D: AsRef<[u8]>, T: AsRef<[u8]>>(
        identity: &Ed25519Identity,
        domain: D,
        payload_type: T,
        payload: Vec<u8>,
    ) -> Result<Self, ()> {
        let sig_payload = sig_payload(domain.as_ref(), payload_type.as_ref(), &payload);
        let signature = identity.sign(&sig_payload);

        Ok(Self {
            key: identity.public_key().to_vec(),
            payload_type: payload_type.as_ref().to_vec(),
            payload,
            signature: signature.to_vec(),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        fn write_bytes(buf: &mut Vec<u8>, field: u8, value: &[u8]) {
            // All fields in Envelope are length-delimited (wire type 2).
            buf.push((field << 3) | 2);
            let mut length = value.len() as u64;
            while length >= 0x80 {
                buf.push((length as u8) | 0x80);
                length >>= 7;
            }
            buf.push(length as u8);
            buf.extend_from_slice(value);
        }

        write_bytes(&mut buf, 1, &self.key);
        write_bytes(&mut buf, 2, &self.payload_type);
        write_bytes(&mut buf, 3, &self.payload);
        write_bytes(&mut buf, 5, &self.signature);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
            let mut value = 0u64;
            for shift in (0..70).step_by(7) {
                let byte = *bytes.get(*pos)?;
                *pos += 1;
                if shift == 63 && byte > 1 {
                    return None;
                }
                value |= ((byte & 0x7f) as u64) << shift;
                if byte & 0x80 == 0 {
                    return Some(value);
                }
            }
            None
        }

        let mut pos = 0;
        let mut key = None;
        let mut payload_type = None;
        let mut payload = None;
        let mut signature = None;

        while pos < bytes.len() {
            let tag = read_varint(bytes, &mut pos).ok_or(DecodeError::InvalidFormat)?;
            let field = tag >> 3;
            let wire_type = tag & 7;
            if field == 0 {
                return Err(DecodeError::InvalidFormat);
            }

            match wire_type {
                0 => {
                    read_varint(bytes, &mut pos).ok_or(DecodeError::InvalidFormat)?;
                }
                1 => {
                    if pos
                        .checked_add(8)
                        .filter(|&end| end <= bytes.len())
                        .is_none()
                    {
                        return Err(DecodeError::InvalidFormat);
                    }
                    pos += 8;
                }
                2 => {
                    let length =
                        read_varint(bytes, &mut pos).ok_or(DecodeError::InvalidFormat)? as usize;
                    let end = pos.checked_add(length).ok_or(DecodeError::InvalidFormat)?;
                    let value = bytes
                        .get(pos..end)
                        .ok_or(DecodeError::InvalidFormat)?
                        .to_vec();
                    pos = end;
                    match field {
                        1 => key = Some(value),
                        2 => payload_type = Some(value),
                        3 => payload = Some(value),
                        5 => signature = Some(value),
                        _ => {}
                    }
                }
                5 => {
                    if pos
                        .checked_add(4)
                        .filter(|&end| end <= bytes.len())
                        .is_none()
                    {
                        return Err(DecodeError::InvalidFormat);
                    }
                    pos += 4;
                }
                // Groups are obsolete and cannot be skipped without parsing nesting.
                _ => return Err(DecodeError::InvalidFormat),
            }
        }

        Ok(Self {
            key: key.ok_or(DecodeError::InvalidFormat)?,
            payload_type: payload_type.ok_or(DecodeError::InvalidFormat)?,
            payload: payload.ok_or(DecodeError::InvalidFormat)?,
            signature: signature.ok_or(DecodeError::InvalidFormat)?,
        })
    }

    pub fn check_sig<R: AsRef<[u8]>>(&self, domain: R) -> Result<(), VerifyError> {
        let sig_payload = sig_payload(domain.as_ref(), &self.payload_type, &self.payload);
        ed25519::verify(&self.key, &sig_payload, &self.signature).map_err(|err| match err {
            SignCheckError::InvalidSignature => VerifyError::InvalidSignature,
            SignCheckError::InvalidPublicKey => VerifyError::InvalidKey,
            SignCheckError::InvalidKeyType => VerifyError::InvalidKey,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DecodeError {
    #[error("Invalid format")]
    InvalidFormat,
    #[error("Invalid signature")]
    InvalidSignature,
}

#[derive(thiserror::Error, Debug)]
pub enum VerifyError {
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Invalid key")]
    InvalidKey,
}

fn sig_payload(domain: &[u8], payload_type: &[u8], payload: &[u8]) -> Vec<u8> {
    let domain_length = UnsignedVarint(domain.len() as u64).encode();
    let payload_type_length = UnsignedVarint(payload_type.len() as u64).encode();
    let payload_length = UnsignedVarint(payload.len() as u64).encode();

    let mut buf = Vec::with_capacity(
        domain_length.len()
            + domain.len()
            + payload_type_length.len()
            + payload_type.len()
            + payload_length.len()
            + payload.len(),
    );

    buf.extend_from_slice(&domain_length);
    buf.extend_from_slice(domain);
    buf.extend_from_slice(&payload_type_length);
    buf.extend_from_slice(payload_type);
    buf.extend_from_slice(&payload_length);
    buf.extend_from_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signed_envelope_encode_decode() {
        let identity = Ed25519Identity::generate();
        let domain = b"test-domain";
        let payload_type = b"test-type".to_vec();
        let payload = b"test-payload".to_vec();

        let envelope =
            SignedEnvelope::new(&identity, domain, payload_type.clone(), payload.clone()).unwrap();
        let encoded = envelope.encode();
        println!("Encoded envelope: {}", hex::encode(&encoded));
        let decoded = SignedEnvelope::decode(&encoded).unwrap();

        assert_eq!(envelope.key, decoded.key);
        assert_eq!(envelope.payload_type, decoded.payload_type);
        assert_eq!(envelope.payload, decoded.payload);
        assert_eq!(envelope.signature, decoded.signature);

        // Check signature verification
        assert!(decoded.check_sig(domain).is_ok());
    }
}
