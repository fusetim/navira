//! Standard I/O implementations for CAR archive handling.
//!
//! This module provides utilities and method to read and write easily CAR files using
//! the standard [Read](std::io::Read), [Write](std::io::Write), [Seek](std::io::Seek) traits.

mod read;
mod write;

use std::{fs::File, path::Path};

pub use read::*;
pub use write::*;

/// Open a CAR file from the given path and return a [CarReader] for it.
///
/// # Arguments
///
/// * `path` - Path to the CAR file to open
///
/// # Returns
///
/// * `Ok(CarReader<File>)`, if the CAR file can be successfully opened (meaning at least the header could be decoded).
/// * `Err(CarReaderError)`, otherwise, indicating the CAR file is corrupted, invalid or just unsupported.
pub fn open_file<P: AsRef<Path>>(path: P) -> Result<CarReader<File>, CarReaderError> {
    let file = std::fs::File::open(&path).map_err(CarReaderError::Io)?;
    CarReader::open(file)
}
