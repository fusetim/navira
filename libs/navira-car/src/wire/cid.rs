use cbor4ii::core::{dec::Decode, enc::Encode, types::Tag};

use crate::wire::{CarDeserializable, CarSerializable};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RawCid(Vec<u8>);

impl RawCid {
    pub fn new(bytes: Vec<u8>) -> Self {
        RawCid(bytes)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for RawCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawCid({})", hex::encode(&self.0))
    }
}

impl std::fmt::Display for RawCid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RawCid({})", hex::encode(&self.0))
    }
}

impl CarDeserializable for RawCid {
    fn from_car_bytes<'a, R: cbor4ii::core::dec::Read<'a>>(reader: &mut R) -> super::Result<Self> {
        let tag: Tag<u64> = Tag::decode(reader).map_err(|_| {
            super::CarError::DeserializationError(super::CarDeserializationError::InvalidCbor)
        })?;
        if tag.0 != 42 {
            return Err(super::CarError::DeserializationError(
                super::CarDeserializationError::InvalidCarStructure,
            ));
        }
        let bytes: Vec<u8> = Decode::decode(reader).map_err(|_| {
            super::CarError::DeserializationError(super::CarDeserializationError::InvalidCbor)
        })?;
        Ok(RawCid::new(bytes))
    }
}

impl CarSerializable for RawCid {
    fn to_car_bytes<W: cbor4ii::core::enc::Write>(&self, writer: &mut W) -> super::Result<()> {
        let bytes = self.bytes().to_owned();
        let tag = Tag(42u64, bytes);
        tag.encode(writer)
            .map_err(|_| super::CarError::SerializationError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cbor4ii::core::utils::{BufWriter, SliceReader};

    use super::RawCid;
    use crate::wire::{CarDeserializable, CarSerializable};

    #[test]
    fn test_raw_cid_serialization() {
        let cid_bytes = vec![0x01, 0x55, 0x02, 0x03, 0x04];
        let raw_cid = RawCid::new(cid_bytes.clone());

        let buf = Vec::new();
        let mut writer = BufWriter::new(buf);
        raw_cid.to_car_bytes(&mut writer).unwrap();

        let mut reader = SliceReader::new(&mut writer.buffer());
        let deserialized_cid = RawCid::from_car_bytes(&mut reader).unwrap();

        assert_eq!(raw_cid, deserialized_cid);
    }

    #[test]
    fn test_raw_cid_deserialization_invalid_tag() {
        let invalid_cid_data = vec![0xC1, 0x01, 0x55, 0x02, 0x03, 0x04]; // Tag 1 instead of 42
        let mut reader = SliceReader::new(&invalid_cid_data);

        let result = RawCid::from_car_bytes(&mut reader);
        assert!(result.is_err());
    }
}
