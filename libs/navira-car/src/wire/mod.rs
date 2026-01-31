use cbor4ii::core::{dec::Read, enc::Write};

pub mod cid;
pub mod v1;
pub mod varint;

#[derive(thiserror::Error, Debug)]
pub enum CarError {
    #[error("Serialization error occurred")]
    SerializationError,
    #[error("Deserialization error occurred")]
    DeserializationError(#[from] CarDeserializationError),
    #[error("Invalid format")]
    InvalidFormat,
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum CarDeserializationError {
    #[error("Unexpected end of input")]
    UnexpectedEof,
    #[error("Invalid CBOR data")]
    InvalidCbor,
    #[error("Invalid CAR structure")]
    InvalidCarStructure,
}

pub type Result<T> = std::result::Result<T, CarError>;

pub trait CarSerializable {
    fn to_car_bytes<W: Write>(&self, writer: &mut W) -> Result<()>;
}

pub trait CarDeserializable: Sized {
    fn from_car_bytes<'a, R: Read<'a>>(reader: &mut R) -> Result<Self>;
}
