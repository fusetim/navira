pub mod cid;
pub mod v1;
pub mod v2;
pub mod varint;

#[derive(thiserror::Error, Debug)]
pub enum CarError {
    #[error("Serialization error occurred")]
    SerializationError,
    #[error("Deserialization error occurred")]
    DeserializationError(),
    #[error("Invalid format")]
    InvalidFormat,
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}
