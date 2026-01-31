//! Data-store related functionality for navira-store
//!
//! This module provides functions and types for managing the data store used by navira-store
//! to serve static content over /ipfs/bitswap.
//!
//! The data store is responsible for maintaining an index of CID to CAR file and finally the block
//! data itself (offset + length in the CAR file).
//! To achieve this, the data store scans at startup all the CAR files in a given directory,
//! pre-indexes them if necessary (CARv2 file have an embedded index) and then build the overall block to car
//! file index in memory for fast lookup.
//!
//! Additional caches are also implemented (as LRU caches) to speed up repeated access to the same blocks or CAR files.
//! Therefore a small number of frequently accessed blocks is kept in memory to avoid repeated disk access. Moreover, recently
//! accessed CAR files are kept open, and their index is cached in memory to avoid re-reading it from disk.
//!
//! The main type provided by this module is `DataStore` which exposes methods to lookup blocks by CID and retrieve their data.
//!
//! TODO: Example usage of DataStore

use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub type Result<T> = std::result::Result<T, DataStoreError>;
/// Errors related to DataStore operations
#[derive(thiserror::Error, Debug)]
pub enum DataStoreError {
    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// CID not found in the datastore
    #[error("CID not found: {0}")]
    NotFound(String),
}

/// DataStore for navira-store
pub struct DataStore {
    // Tracked CAR files
    tracked_car: Vec<PathBuf>,
    // CAR file handles
    car_handles: Vec<CarHandle>,

    // TODO: Block caches
    // TODO: CAR index caches
    max_open_cars: usize,
}

impl DataStore {
    /// Create a new DataStore
    pub fn new() -> Self {
        Self::with_limits(16)
    }

    /// Create a DataStore with custom limits
    pub fn with_limits(max_open_cars: usize) -> Self {
        Self {
            tracked_car: Vec::new(),
            car_handles: Vec::new(),
            max_open_cars,
        }
    }

    /// Scan a directory for CAR files and track them
    ///
    /// # Arguments
    ///
    /// * `dir` - Path to the directory to scan
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of CAR files found and tracked
    /// * `Err(DataStoreError)` - Error occurred during scanning
    pub fn scan_directory<P: AsRef<Path>>(&mut self, dir: P) -> Result<usize> {
        // Scan the directory for .car files
        let mut discovered = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("car") {
                let abs_path = std::fs::canonicalize(&path)?;
                discovered.push(abs_path);
            }
        }

        // Insert the discovered CAR files into tracked_car if not already present
        let mut count = 0;
        for car_path in discovered {
            if !self.tracked_car.contains(&car_path) {
                self.tracked_car.push(car_path);
                count += 1;
            }
        }

        Ok(count)
    }

    /// Preforms the block indexing of the tracked CAR files
    /// ///
    /// # Returns
    /// * `Ok(())` - Indexing completed successfully
    /// * `Err(DataStoreError)` - Error occurred during indexing
    pub fn index(&mut self) -> Result<()> {
        Ok(())
    }

    /// Carefully shutdown the DataStore, closing any open CAR files
    pub fn shutdown(&mut self) -> Result<()> {
        self.car_handles.clear();
        Ok(())
    }

    /// Open a CAR file and return its handle
    fn open_car(&mut self, idx: usize) -> Result<&mut CarHandle> {
        // Check if the CAR file is already open
        if !self.car_handles.iter().any(|h| h.idx == idx) {
            // If we reached the max open CAR files, close the least recently used one
            if self.car_handles.len() >= self.max_open_cars {
                self.car_handles.remove(0);
            }

            // Open the CAR file
            let car_path = &self.tracked_car[idx];
            let file = File::open(car_path)?;
            let handle = CarHandle { idx, file };
            self.car_handles.push(handle);
        }
        // Return the handle
        Ok(self.car_handles.iter_mut().find(|h| h.idx == idx).unwrap())
    }
}

/// Handle to an open CAR file
pub struct CarHandle {
    idx: usize,
    file: File,
}
