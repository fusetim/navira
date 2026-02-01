pub mod header;
pub mod index;

/// CAR v2 pragma bytes
///
/// These bytes are used to identify the CAR v2 format in a file header.
/// The pragma consists of a fixed sequence of bytes that includes
/// the version number of the CAR format.
pub const CAR_V2_PRAGMA: &[u8] = &[
    0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
];
