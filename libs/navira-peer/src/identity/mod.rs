pub mod ed25519;
pub mod peerid;

pub trait IntoPeerId {
    fn into_peerid(&self) -> peerid::PeerId;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignCheckError {
    InvalidPublicKey,
    InvalidKeyType,
    InvalidSignature,
}
