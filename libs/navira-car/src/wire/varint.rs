//! CAR archives make use of variable-length integers (varints) for efficient encoding of integer values.
//! 
//! This module provides utilities for encoding and decoding varints according to the CAR specification.
//!
//! Actually, CAR varints follow the [LEB128 encoding scheme](https://en.wikipedia.org/wiki/LEB128),
//! which is a common method for encoding integers in a variable number of bytes.

/// Unsigned variable-length integer (varint) as used in CAR files.
/// 
/// This struct represents an unsigned varint, which can be encoded and decoded using LEB128 encoding.  
/// To do so,
/// - Use `UnsignedVarint::encode()` to encode the varint into a vector of bytes.
/// - Use `UnsignedVarint::decode(bytes)` to decode a varint from a slice of bytes, which returns 
///   the decoded varint and the number of bytes read.
/// 
/// ## Examples
/// ```
/// use navira_car::wire::varint::UnsignedVarint;
/// 
/// let varint = UnsignedVarint(624485);
/// let encoded = varint.encode();
/// assert_eq!(encoded, vec![0xE5, 0x8E, 0x26]);
/// 
/// let (decoded, bytes_read) = UnsignedVarint::decode(&encoded).unwrap();
/// assert_eq!(decoded, UnsignedVarint(624485));
/// assert_eq!(bytes_read, encoded.len());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsignedVarint(
    /// The underlying unsigned integer value of the varint.
    pub u64
);

/// Signed variable-length integer (varint) as used in CAR files.
/// 
/// This struct represents a signed varint, which can be encoded and decoded using LEB128 encoding.
/// To do so,
/// - Use `SignedVarint::encode()` to encode the varint into a vector of bytes.
/// - Use `SignedVarint::decode(bytes)` to decode a varint from a slice of bytes, which returns 
///   the decoded varint and the number of bytes read.
/// 
/// ## Examples
/// ```
/// use navira_car::wire::varint::SignedVarint;
/// 
/// let varint = SignedVarint(-123456);
/// let encoded = varint.encode();
/// assert_eq!(encoded, vec![0xC0, 0xBB, 0x78]);
/// 
/// let (decoded, bytes_read) = SignedVarint::decode(&encoded).unwrap();
/// assert_eq!(decoded, SignedVarint(-123456));
/// assert_eq!(bytes_read, encoded.len());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedVarint(
    /// The underlying signed integer value of the varint.
    pub i64
);

impl UnsignedVarint {
    /// Encodes the UnsignedVarint into a vector of bytes using LEB128 encoding.
    pub fn encode(self) -> Vec<u8> {
        let mut value = self.0;
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // Set continuation bit
            }
            bytes.push(byte);
            if value == 0 {
                break;
            }
        }
        bytes
    }

    /// Decodes an UnsignedVarint from a slice of bytes.
    /// 
    /// ## Returns
    /// - `Some((UnsignedVarint, bytes_read))` if decoding is successful,
    ///   where `UnsignedVarint` is the decoded varint and `bytes_read` is the number of bytes consumed during decoding.
    /// - `None` if the input bytes do not represent a valid varint (e.g., incomplete varint or overflow).
    pub fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut result = 0u64;
        let mut shift = 0;
        for (i, &byte) in bytes.iter().enumerate() {
            let value = (byte & 0x7F) as u64;
            result |= value << shift;
            if (byte & 0x80) == 0 {
                return Some((UnsignedVarint(result), i + 1));
            }
            shift += 7;
            if shift >= 64 {
                return None; // Overflow
            }
        }
        None // Incomplete varint
    }
}

impl From<u64> for UnsignedVarint {
    fn from(value: u64) -> Self {
        UnsignedVarint(value)
    }
}

impl From<UnsignedVarint> for u64 {
    fn from(varint: UnsignedVarint) -> Self {
        varint.0
    }
}

impl SignedVarint {
    /// Encodes the SignedVarint into a vector of bytes using LEB128 encoding.
    pub fn encode(self) -> Vec<u8> {
        let mut value = self.0;
        let neg = value < 0;
        let mut bytes = Vec::new();
        let mut more = true;
        while more {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if neg {
                value |= -(1 << (64 - 7)); // Sign extend
            }
            // Determine if more bytes are needed
            if (value == 0 && (byte & 0x40) == 0) || (value == -1 && (byte & 0x40) != 0) {
                more = false;
            } else {
                byte |= 0x80; // Set continuation bit
            }
            bytes.push(byte);
        }
        bytes
    }

    /// Decodes an SignedVarint from a slice of bytes.
    /// 
    /// ## Returns
    /// - `Some((SignedVarint, bytes_read))` if decoding is successful,
    ///   where `SignedVarint` is the decoded varint and `bytes_read` is the number of bytes consumed during decoding.
    /// - `None` if the input bytes do not represent a valid varint (e.g., incomplete varint or overflow).
    pub fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let mut result = 0i64;
        let mut shift = 0;
        let mut byte: u8;
        for (i, &b) in bytes.iter().enumerate() {
            byte = b;
            let value = (byte & 0x7F) as i64;
            result |= value << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                // Sign bit of byte is second high order bit (0x40)
                if (shift < 64) && ((byte & 0x40) != 0) {
                    result |= -1i64 << shift; // Sign extend
                }
                return Some((SignedVarint(result), i + 1));
            }
            if shift >= 64 {
                return None; // Overflow
            }
        }
        None // Incomplete varint
    }
}

impl From<i64> for SignedVarint {
    fn from(value: i64) -> Self {
        SignedVarint(value)
    }
}

impl From<SignedVarint> for i64 {
    fn from(varint: SignedVarint) -> Self {
        varint.0
    }
}

#[cfg(test)]
mod tests {
    use super::{SignedVarint, UnsignedVarint};

    #[test]
    fn test_unsigned_varint_encoding() {
        let varint = UnsignedVarint(624485);
        let expected = vec![0xE5, 0x8E, 0x26];
        assert_eq!(varint.encode(), expected);
    }

    #[test]
    fn test_unsigned_varint_encoding_decoding() {
        let varint = vec![0xE5, 0x8E, 0x26];
        let (decoded, bytes_read) = UnsignedVarint::decode(&varint).unwrap();
        assert_eq!(decoded, UnsignedVarint(624485));
        assert_eq!(bytes_read, varint.len());
    }

    #[test]
    fn test_unsigned_varint_round_trip() {
        for i in 0..=65537 {
            let varint = UnsignedVarint(i);
            let encoded = varint.encode();
            let (decoded, bytes_read) = UnsignedVarint::decode(&encoded).unwrap();
            assert_eq!(varint, decoded);
            assert_eq!(bytes_read, encoded.len());
        }
    }

    #[test]
    fn test_signed_varint_encoding() {
        let varint = SignedVarint(-123456);
        let expected = vec![0xC0, 0xBB, 0x78];
        assert_eq!(varint.encode(), expected);
    }

    #[test]
    fn test_signed_varint_encoding_decoding() {
        let varint = vec![0xC0, 0xBB, 0x78];
        let (decoded, bytes_read) = SignedVarint::decode(&varint).unwrap();
        assert_eq!(decoded, SignedVarint(-123456));
        assert_eq!(bytes_read, varint.len());
    }

    #[test]
    fn test_signed_varint_round_trip() {
        let test_values = [-65537, -32768, -1, 0, 1, 32767, 65537];
        for &i in &test_values {
            let varint = SignedVarint(i);
            let encoded = varint.encode();
            let (decoded, bytes_read) = SignedVarint::decode(&encoded).unwrap();
            assert_eq!(varint, decoded);
            assert_eq!(bytes_read, encoded.len());
        }
    }

    #[test]
    fn test_unsigned_varint_decode_car_header_size() {
        const CAR_EXTRACT: [u8; 12] = [
            0x63, 0xA2, 0x65, 0x72, 0x6F, 0x6F, 0x74, 0x73, 0x82, 0xD8, 0x2A, 0x58,
        ];
        let (decoded, bytes_read) = UnsignedVarint::decode(&CAR_EXTRACT).unwrap();
        assert_eq!(decoded, UnsignedVarint(99));
        assert_eq!(bytes_read, 1);
    }
}
