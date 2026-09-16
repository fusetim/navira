use ed25519_dalek::{SecretKey, Signer as _, SigningKey};
use getrandom::rand_core::UnwrapErr;

use crate::identity::{IntoPeerId, peerid::{EncodedPrivateKey, EncodedPublicKey, KeyType, PeerId}};


#[derive(Debug, Clone)]
pub struct Ed25519Identity {
    key: ed25519_dalek::SigningKey,
}

impl Ed25519Identity {
    /// Generates a new Ed25519 identity with a random keypair.
    pub fn generate() -> Self {
        let mut csprng = UnwrapErr(getrandom::SysRng);
        Self { key: SigningKey::generate(&mut csprng) }
    }

    pub fn from_secret_key(key: SecretKey) -> Self {
        Self { key: SigningKey::from(key) }
    }

    pub fn from_private_key(encoded: &[u8]) -> Result<Self, ()> {
        let encoded_private_key = EncodedPrivateKey::decode(encoded)?;
        if encoded_private_key.key_type() != KeyType::Ed25519 {
            return Err(());
        }
        let mut secret_key = [0u8;32];
        secret_key.copy_from_slice(&encoded_private_key.key_bytes()[0..32]);
        Ok(Self::from_secret_key(secret_key))
    }

    /// Get the encoded public key bytes for this identity.
    pub fn public_key(&self) -> Vec<u8> {
        let public_key = self.key.verifying_key().to_bytes();
        EncodedPublicKey::new(KeyType::Ed25519, public_key.to_vec()).encode()
    }

    /// Get the encoded private key bytes for this identity.
    pub fn private_key(&self) -> Vec<u8> {
        let private_key = self.key.to_bytes();
        EncodedPrivateKey::new(KeyType::Ed25519, private_key.to_vec()).encode()
    }

    /// Sign a message with this identity's private key.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        let signature = self.key.try_sign(message).unwrap();
        signature.to_bytes()
    }
}

impl IntoPeerId for Ed25519Identity {
    fn into_peerid(&self) -> crate::identity::peerid::PeerId {
        PeerId::new(&self.public_key())
    }
}

