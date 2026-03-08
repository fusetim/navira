//! CAR v2 related types and utilities
//!
//! This module contains types and utilities related to the CAR v2 format.
//! If you are looking for the main CAR reader/writer, you probably want to use the
//! [CarReader](crate::CarReader) types in the parent module instead, which can handle both CAR v1
//! and v2 formats transparently.
//!
//! However, if you only need to work with CAR v2 headers or sections, you can use the types in this module directly.

mod header;
mod index;
mod read;
mod write;

pub use crate::wire::v1::{Block, LocatableSection, Section, SectionFormatError, SectionLocation};
pub use header::{CarV2Header, Characteristics};
pub use index::*;
pub use read::{CarReader, CarReaderError};
pub use write::*;

/// CAR v2 pragma bytes
///
/// These bytes are used to identify the CAR v2 format in a file header.  
/// The pragma consists of a fixed sequence of bytes that includes
/// the version number of the CAR format.
pub const CAR_V2_PRAGMA: &[u8] = &[
    0x0a, 0xa1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x02,
];

#[cfg(test)]
mod tests {
    use crate::wire::cid::{IntoRawLink as _, RawCid};

    use super::*;

    const CAR_V2: [u8; 715] = [
        // Offset 0x00000000 to 0x000002CA
        0x0A, 0xA1, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x02, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF3, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0xA2, 0x65, 0x72, 0x6F, 0x6F, 0x74, 0x73, 0x81,
        0xD8, 0x2A, 0x58, 0x23, 0x00, 0x12, 0x20, 0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13,
        0x71, 0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39, 0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3,
        0xCD, 0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2, 0x32, 0xF8, 0x67, 0x76, 0x65, 0x72, 0x73, 0x69,
        0x6F, 0x6E, 0x01, 0x51, 0x12, 0x20, 0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13, 0x71,
        0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39, 0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3, 0xCD,
        0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2, 0x32, 0xF8, 0x12, 0x2D, 0x0A, 0x22, 0x12, 0x20, 0xD9,
        0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A, 0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC,
        0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E, 0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6,
        0x26, 0x12, 0x04, 0xF0, 0x9F, 0x8D, 0xA4, 0x18, 0xA4, 0x01, 0x85, 0x01, 0x12, 0x20, 0xD9,
        0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A, 0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC,
        0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E, 0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6,
        0x26, 0x12, 0x31, 0x0A, 0x22, 0x12, 0x20, 0xD7, 0x45, 0xB7, 0x75, 0x7F, 0x5B, 0x45, 0x93,
        0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E, 0xB4, 0x96, 0xA7, 0x41, 0x0A, 0x0D,
        0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1, 0x12, 0x09, 0x62, 0x61, 0x72, 0x72,
        0x65, 0x6C, 0x65, 0x79, 0x65, 0x18, 0x3A, 0x12, 0x2E, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20,
        0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C,
        0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C,
        0x29, 0x4D, 0x12, 0x04, 0xF0, 0x9F, 0x90, 0xA1, 0x18, 0x07, 0x58, 0x12, 0x20, 0xD7, 0x45,
        0xB7, 0x75, 0x7F, 0x5B, 0x45, 0x93, 0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E,
        0xB4, 0x96, 0xA7, 0x41, 0x0A, 0x0D, 0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1,
        0x12, 0x34, 0x0A, 0x24, 0x01, 0x55, 0x12, 0x20, 0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2,
        0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14, 0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9,
        0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5, 0x07, 0x2D, 0x12, 0x0A, 0x66, 0x69, 0x73,
        0x68, 0x6D, 0x6F, 0x6E, 0x67, 0x65, 0x72, 0x18, 0x04, 0x28, 0x01, 0x55, 0x12, 0x20, 0xB4,
        0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14, 0xEF,
        0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5, 0x07,
        0x2D, 0x66, 0x69, 0x73, 0x68, 0x2B, 0x01, 0x55, 0x12, 0x20, 0xA2, 0xE1, 0xC4, 0x0D, 0xA1,
        0xAE, 0x33, 0x5D, 0x4D, 0xFF, 0xE7, 0x29, 0xEB, 0x4D, 0x5C, 0xA2, 0x3B, 0x74, 0xB9, 0xE5,
        0x1F, 0xC5, 0x35, 0xF4, 0xA8, 0x04, 0xA2, 0x61, 0x08, 0x0C, 0x29, 0x4D, 0x6C, 0x6F, 0x62,
        0x73, 0x74, 0x65, 0x72, 0x01, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xA2, 0xE1, 0xC4, 0x0D, 0xA1, 0xAE, 0x33, 0x5D, 0x4D, 0xFF,
        0xE7, 0x29, 0xEB, 0x4D, 0x5C, 0xA2, 0x3B, 0x74, 0xB9, 0xE5, 0x1F, 0xC5, 0x35, 0xF4, 0xA8,
        0x04, 0xA2, 0x61, 0x08, 0x0C, 0x29, 0x4D, 0x94, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xB4, 0x74, 0xA9, 0x9A, 0x27, 0x05, 0xE2, 0x3C, 0xF9, 0x05, 0xA4, 0x84, 0xEC, 0x6D, 0x14,
        0xEF, 0x58, 0xB5, 0x6B, 0xBE, 0x62, 0xE9, 0x29, 0x27, 0x83, 0x46, 0x6E, 0xC3, 0x63, 0xB5,
        0x07, 0x2D, 0x6B, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xD7, 0x45, 0xB7, 0x75, 0x7F,
        0x5B, 0x45, 0x93, 0xEE, 0xAB, 0x78, 0x20, 0x30, 0x6C, 0x7B, 0xC6, 0x4E, 0xB4, 0x96, 0xA7,
        0x41, 0x0A, 0x0D, 0x07, 0xDF, 0x7A, 0x34, 0xFF, 0xEC, 0x4B, 0x97, 0xF1, 0x12, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xD9, 0xC0, 0xD5, 0x37, 0x6D, 0x26, 0xF1, 0x93, 0x1F, 0x7A,
        0xD5, 0x2D, 0x7A, 0xCC, 0x00, 0xFC, 0x10, 0x90, 0xD2, 0xED, 0xB0, 0x80, 0x8B, 0xF6, 0x1E,
        0xEB, 0x0A, 0x15, 0x28, 0x26, 0xF6, 0x26, 0x8B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xFB, 0x16, 0xF5, 0x08, 0x34, 0x12, 0xEF, 0x13, 0x71, 0xD0, 0x31, 0xED, 0x4A, 0xA2, 0x39,
        0x90, 0x3D, 0x84, 0xEF, 0xDA, 0xDF, 0x1B, 0xA3, 0xCD, 0x67, 0x8E, 0x64, 0x75, 0xB1, 0xA2,
        0x32, 0xF8, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn test_car_v2_header_deserialization() {
        let mut reader = CarReader::new();
        reader.receive_data(&CAR_V2, 0);
        reader.read_header().unwrap();
        let (v1h, v2h) = reader.header().unwrap();
        assert_eq!(v2h.characteristics.0, 0);
        assert_eq!(v2h.data_offset, 51);
        assert_eq!(v2h.data_size, 448);
        assert_eq!(v2h.index_offset, 499);
        assert_eq!(v1h.roots().len(), 1);
    }

    #[test]
    fn test_car_v2_header_deserialization_partial() {
        let mut reader = CarReader::new();

        let (v1h, v2h);
        loop {
            match reader.read_header() {
                Ok(_) => {
                    let headers = reader.header().unwrap();
                    v1h = headers.0.clone();
                    v2h = headers.1.clone();
                    break;
                }
                Err(CarReaderError::InsufficientData(offset, hint)) => {
                    let end = offset + hint;
                    let data = &CAR_V2[offset..end.min(CAR_V2.len())];
                    reader.receive_data(data, offset);
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        assert_eq!(v2h.characteristics.0, 0);
        assert_eq!(v2h.data_offset, 51);
        assert_eq!(v2h.data_size, 448);
        assert_eq!(v2h.index_offset, 499);
        assert_eq!(v1h.roots().len(), 1);
    }

    #[test]
    fn test_car_v2_header_count_blocks() {
        let mut reader = CarReader::new();
        reader.receive_data(&CAR_V2, 0);
        reader.read_header().unwrap();

        let mut block_count = 0;
        let mut block_bytes = 0;
        loop {
            match reader.read_section() {
                Ok(section) => {
                    println!("Read section: {:?}", section);
                    block_count += 1;
                    block_bytes += section.block().data().len();
                }
                Err(CarReaderError::InsufficientData(_, _)) => {
                    break;
                }
                Err(CarReaderError::EndOfSections) => {
                    break;
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
        assert_eq!(block_count, 5);
        assert_eq!(block_bytes, 211);
    }

    #[test]
    fn test_car_v2_writer_reader_compatibility() {
        let root_cid = RawCid::from_hex(
            "015512200000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let cid2 = RawCid::from_hex(
            "01551220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let cid3 = RawCid::from_hex(
            "01551220ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )
        .unwrap();
        let first_block = Block::new(vec![1, 2, 3, 4]);
        let second_block = Block::new(vec![5, 6, 7, 8]);
        let third_block = Block::new(vec![9, 10, 11, 12]);
        let section1 = Section::new(root_cid.clone(), first_block);
        let section2 = Section::new(cid2, second_block);
        let section3 = Section::new(cid3, third_block);

        let mut writer = CarWriter::new(vec![root_cid.clone()]);
        let mut sink = Vec::new();
        let mut buf = [0u8; 64];
        let mut section_to_write = vec![section1.clone(), section2.clone(), section3.clone()];

        // Helper function to flush data from the writer to the sink, handling the non-linear writing and extension of the sink as needed
        fn flush_writer_to_sink<W: CarWriteV2>(writer: &mut W, sink: &mut Vec<u8>, buf: &mut [u8]) {
            let (offset, written) = writer.send_data(buf);
            println!(
                "At offset: {}, written: {}, data: {}",
                offset,
                written,
                hex::encode(&buf[..written])
            );
            if written > 0 {
                // V2 Writer will write data non-linearly, so we need to insert it at the correct offset in the sink
                // 1. If necessary, resize the sink to accommodate the new data
                if offset + written > sink.len() {
                    sink.resize(offset + written, 0);
                }
                // 2. Copy the written data into the sink at the correct offset
                sink[offset..offset + written].copy_from_slice(&buf[..written]);
            }
        }

        // 1. Write the sections
        loop {
            // Write bytes to sink until there are no more sections to write and no more data to flush
            flush_writer_to_sink(&mut writer, &mut sink, &mut buf);

            // If there are still sections to write, try to write the next one
            if let Some(section) = section_to_write.pop() {
                match writer.write_section(&section) {
                    Ok(location) => println!("Section written at location: {:?}", location),
                    Err(CarWriterError::BufferFull) => {
                        // Buffer is full, we need to flush before writing the next section
                        section_to_write.push(section); // Put the section back to try writing it again after flushing
                        continue;
                    }
                }
            } else {
                // No more sections to write, we just need to flush any remaining data
                if !writer.has_data_to_send() {
                    break; // All done
                }
            }
        }
        // 2. Finalize sections and index
        while writer.has_data_to_send() {
            flush_writer_to_sink(&mut writer, &mut sink, &mut buf);
        }
        let mut writer = match writer.finalize_all() {
            Ok(w) => w,
            Err(_) => {
                panic!(
                    "Unexpected error, writer has no more data to send but cannot be finalized..."
                );
            }
        };

        // 3. Finalize the header and write it to the sink
        while writer.has_data_to_send() {
            flush_writer_to_sink(&mut writer, &mut sink, &mut buf);
        }

        println!("Final CAR data: {:?}", hex::encode(&sink));

        // Now, read the data back with a reader and check that the header and sections are the same
        let mut reader = CarReader::new();
        // 1. Read the header
        loop {
            match reader.read_header() {
                Ok(()) => break, // Header read successfully
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + buf.len(), sink.len());
                    if read_from >= end {
                        panic!("Test data exhausted before header could be read");
                    }
                    reader.receive_data(&sink[read_from..end], read_from);
                }
                Err(err) => {
                    panic!("Unexpected error while reading header: {:?}", err);
                }
            }
        }
        // 2. Check the header matches what we wrote
        let (header, v2_header) = reader.header().unwrap();
        assert_eq!(header.version(), 1);
        assert_eq!(header.roots().len(), 1);
        assert_eq!(header.roots()[0], root_cid.into_link());
        // 3. Read sections and check they match what we wrote
        let mut seen = [false; 3];
        loop {
            match reader.read_section() {
                Ok(section) => {
                    println!("Read section with CID: {:?}", section.cid());
                    if section.cid() == section1.cid() {
                        assert_eq!(section.block().data(), section1.block().data());
                        seen[0] = true;
                    } else if section.cid() == section2.cid() {
                        assert_eq!(section.block().data(), section2.block().data());
                        seen[1] = true;
                    } else if section.cid() == section3.cid() {
                        assert_eq!(section.block().data(), section3.block().data());
                        seen[2] = true;
                    } else {
                        panic!("Unexpected CID in section: {:?}", section.cid());
                    }
                }
                Err(CarReaderError::InsufficientData(read_from, _)) => {
                    let end = std::cmp::min(read_from + buf.len(), sink.len());
                    if read_from >= end {
                        // No more data to read
                        break;
                    }
                    reader.receive_data(&sink[read_from..end], read_from);
                }
                Err(CarReaderError::EndOfSections) => {
                    break; // All sections read successfully
                }
                Err(err) => {
                    panic!("Unexpected error while reading section: {:?}", err);
                }
            }
        }
        assert!(
            seen.iter().all(|&s| s),
            "Not all sections were read correctly"
        );
    }
}
