use ed25519_dalek::{SecretKey, Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use getrandom::rand_core::UnwrapErr;

use crate::identity::{
    IntoPeerId, SignCheckError,
    peerid::{EncodedPrivateKey, EncodedPublicKey, KeyType, PeerId},
};

#[derive(Debug, Clone)]
pub struct Ed25519Identity {
    key: SigningKey,
}

impl Ed25519Identity {
    /// Generates a new Ed25519 identity with a random keypair.
    pub fn generate() -> Self {
        let mut csprng = UnwrapErr(getrandom::SysRng);
        Self {
            key: SigningKey::generate(&mut csprng),
        }
    }

    pub fn from_secret_key(key: SecretKey) -> Self {
        Self {
            key: SigningKey::from(key),
        }
    }

    pub fn from_private_key(encoded: &[u8]) -> Result<Self, ()> {
        let encoded_private_key = EncodedPrivateKey::decode(encoded)?;
        if encoded_private_key.key_type() != KeyType::Ed25519 {
            return Err(());
        }
        let mut secret_key = [0u8; 32];
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

/// Verify a signature for a given message and public key.
///
/// # Args
///
/// * `public_key`: The libp2p encoded public key bytes of the signer.
/// * `message`: The message that was signed.
/// * `signature`: The signature bytes to verify.
pub fn verify(public_key: &[u8], message: &[u8], signature: &[u8]) -> Result<(), SignCheckError> {
    let encoded_public_key = match EncodedPublicKey::decode(public_key) {
        Ok(key) => key,
        Err(_) => return Err(SignCheckError::InvalidPublicKey),
    };
    if encoded_public_key.key_type() != KeyType::Ed25519 {
        return Err(SignCheckError::InvalidKeyType);
    }
    let mut public_key_bytes = [0u8; 32];
    public_key_bytes.copy_from_slice(&encoded_public_key.key_bytes()[0..32]);

    let public_key = match VerifyingKey::from_bytes(&public_key_bytes) {
        Ok(key) => key,
        Err(_) => return Err(SignCheckError::InvalidPublicKey),
    };

    let signature =
        Signature::from_slice(signature).map_err(|_| SignCheckError::InvalidSignature)?;

    public_key
        .verify(message, &signature)
        .map_err(|_| SignCheckError::InvalidSignature)
}
