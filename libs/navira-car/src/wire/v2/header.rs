/// CAR v2 header structure
///
/// The CARv2 header is a fixed-size structure that contains metadata
/// about the CARv2 file, including characteristics, data offset,
/// data size, and index offset.
///
/// The header is 40 bytes in size and is represented as follows:
/// - Bytes 0-15: Characteristics bitfield (u128, Little Endian)
/// - Bytes 16-23: Data offset from the start of the CARv2 pragma (u64, Little Endian)
/// - Bytes 24-31: Data size in bytes (u64, Little Endian)
/// - Bytes 32-39: Index offset from the start of the CARv2 pragma (u64, Little Endian, 0 if no index)
#[derive(Clone, PartialEq, Eq)]
pub struct CarV2Header {
    /// Characteristics bitfield
    pub characteristics: Characteristics,
    /// Data offset from the start of the CARv2 pragma
    /// Little Endian u64
    pub data_offset: u64,
    /// Data size in bytes
    /// Little Endian u64
    pub data_size: u64,
    /// Index offset from the start of the CARv2 pragma (0 if no index)
    /// Little Endian u64
    pub index_offset: u64,
}

impl From<[u8; 40]> for CarV2Header {
    fn from(bytes: [u8; 40]) -> Self {
        let characteristics =
            Characteristics(u128::from_le_bytes(bytes[0..16].try_into().unwrap()));
        let data_offset = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let data_size = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let index_offset = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        CarV2Header {
            characteristics,
            data_offset,
            data_size,
            index_offset,
        }
    }
}

impl From<&CarV2Header> for [u8; 40] {
    fn from(header: &CarV2Header) -> Self {
        let mut bytes = [0u8; 40];
        bytes[0..16].copy_from_slice(&header.characteristics.0.to_le_bytes());
        bytes[16..24].copy_from_slice(&header.data_offset.to_le_bytes());
        bytes[24..32].copy_from_slice(&header.data_size.to_le_bytes());
        bytes[32..40].copy_from_slice(&header.index_offset.to_le_bytes());
        bytes
    }
}

bitfield::bitfield! {
    /// Characteristics bitfield for CARv2 header
    pub struct Characteristics(u128);
    /// Indicates if the CARv2 file is fully indexed
    pub has_full_index, set_has_full_index: 0;
}

impl core::fmt::Debug for Characteristics {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Characteristics({:#x})", self.0)
    }
}

impl Clone for Characteristics {
    fn clone(&self) -> Self {
        Characteristics(self.0)
    }
}
impl Copy for Characteristics {}
impl PartialEq for Characteristics {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Characteristics {}
