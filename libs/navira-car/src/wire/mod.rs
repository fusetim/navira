//! "Wire" format for CAR files
//!
//! This module contains all the structures, serialization/deserialization logic, and utilities related to
//! the "wire" format of CAR files. The "wire" format refers to the actual byte-level representation of CAR files,
//! including headers, sections, and blocks.

pub mod cid;
pub mod v1;
pub mod v2;
pub mod varint;