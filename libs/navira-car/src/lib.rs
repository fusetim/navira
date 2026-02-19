//! navira-car is a Rust library for reading and writing CAR (Content Addressable aRchive) files, 
//! which are used in the IPFS ecosystem to store collections of content-addressed data.
//! 
//! The library provides functionality for working with both CAR v1 and CAR v2 formats,
//! including reading headers, sections, blocks, and indexes.  
//! ***TODO:** Write support, Index support, and more utilities for working with CAR files.*
//! 
//! The main philosophy of the library is to provide a simple and efficient API for 
//! working with CAR files, while staying close to the underlying specifications and formats. In 
//! particular, the library provides low-level access to the "wire" format of CAR files, and expose 
//! a sans-IO API for reading CAR files that can be used in a variety of contexts (e.g., reading from files, network streams, etc).
//! 
//! The main entry point for reading CAR files is the [CarReader] type, 
//! which can handle both CAR v1 and v2 formats transparently.
//! 
//! ## Usages
//! 
//! ### Consume an entire CAR file and print all the CIDs of the blocks it contains
//! ```rust 
//! let car_bytes = include_bytes!("res/carv1-basic.car");
//! 
//! // Create a CarReader and feed it the CAR file bytes
//! let mut reader = navira_car::CarReader::new();
//! reader.receive_data(car_bytes, 0);
//! 
//! // Try to read the header (it should succeed since we have the full CAR file in memory)
//! reader.read_header().unwrap();
//! assert_eq!(reader.get_format(), Some(navira_car::CarFormat::V1));
//! 
//! // Print all the CIDs of the blocks in the CAR file
//! while let Ok(sect) = reader.read_section() {
//!     println!("Block raw/binary CID: {}", sect.cid().to_hex());
//! }
//! assert!(reader.read_section().is_err()); // We should have reached the end of the CAR file
//! 
//! //>> Output:
//! // Block raw/binary CID: 01711220f88bc853804cf294fe417e4fa83028689fcdb1b1592c5102e1474dbc200fab8b
//! // Block raw/binary CID: 122002acecc5de2438ea4126a3010ecb1f8a599c8eff22fff1a1dcffe999b27fd3de
//! // Block raw/binary CID: 01551220b6fbd675f98e2abd22d4ed29fdc83150fedc48597e92dd1a7a24381d44a27451
//! // Block raw/binary CID: 122079a982de3c9907953d4d323cee1d0fb1ed8f45f8ef02870c0cb9e09246bd530a
//! // Block raw/binary CID: 0155122081cc5b17018674b401b42f35ba07bb79e211239c23bffe658da1577e3e646877
//! // Block raw/binary CID: 1220e7dc486e97e6ebe5cdabab3e392bdad128b6e09acc94bb4e2aa2af7b986d24d0
//! // Block raw/binary CID: 0155122061be55a8e2f6b4e172338bddf184d6dbee29c98853e0a0485ecee7f27b9af0b4
//! // Block raw/binary CID: 0171122069ea0740f9807a28f4d932c62e7c1c83be055e55072c90266ab3e79df63a365b
//! ```
//! 
//! 
//! 
//! ## Alternatives
//! 
//! Alternatives to this library include:  
//! - [rs-car](https://crates.io/crates/rs-car)
//! - [rust-car](https://crates.io/crates/rust-car)
//! - [blockless-car](https://crates.io/crates/blockless-car)

pub mod wire;
pub mod read;

pub use read::{CarReader, CarReaderError, CarFormat};