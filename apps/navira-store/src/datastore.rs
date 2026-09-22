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

use std::path::{Path, PathBuf};

use cid::multibase::Base;
use cid::multihash::Multihash;
use cid::{Cid, Version};
use navira_car::tokio::{CarBlockLocation, CarReader};
use navira_car::wire::cid::RawCid;
use navira_car::wire::v1::CarBlock;
use tokio_stream::StreamExt;

use tracing::debug;

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
    // TODO: Block caches
    // TODO: CAR index caches
}

pub struct LocationHint {
    car_index: usize,
    car_file: PathBuf,
    metadata: CarBlockLocation,
}

impl LocationHint {
    fn new(car_index: usize, car_file: PathBuf, metadata: CarBlockLocation) -> Self {
        Self {
            car_index,
            car_file,
            metadata,
        }
    }

    pub fn car_file(&self) -> &PathBuf {
        &self.car_file
    }

    pub fn metadata(&self) -> &CarBlockLocation {
        &self.metadata
    }
}

impl DataStore {
    /// Create a new DataStore
    pub fn new() -> Self {
        Self {
            tracked_car: Vec::new(),
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
    pub async fn scan_directory<P: AsRef<Path>>(&mut self, dir: P) -> Result<usize> {
        // Scan the directory for .car files
        let mut discovered = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
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
    ///
    /// # Returns
    /// * `Ok(Vec<RawCid>)` - Indexing completed successfully, returning a list of all the CIDs found
    /// * `Err(DataStoreError)` - Error occurred during indexing
    pub async fn index(&mut self) -> Result<Vec<RawCid>> {
        let cnt = self.tracked_car.len();
        let mut cids: Vec<_> = Vec::new();
        for idx in 0..cnt {
            let path = self.tracked_car[idx].clone();
            let mut file = tokio::fs::OpenOptions::new().read(true).open(&path).await?;

            debug!("Indexing CAR file {} at path {:?}", idx, path);

            let (header, mut reader) = CarReader::open(&mut file)
                .await
                .expect("Failed to open CAR file for indexing");
            // Print the header information for debugging
            debug!("CAR file {} header: {:?}", idx, header);

            // Count the sections in file
            let mut count = 0;
            let mut blocks = reader.block_locations();
            while let Some(Ok(loc)) = blocks.next().await {
                count += 1;
                cids.push(loc.cid().clone());
                debug!(
                    "CAR file {} -> block@{:X} : {} ({} bytes)",
                    idx,
                    loc.section_start(),
                    pretty_print_cid(
                        &Cid::try_from(loc.cid()).expect("Failed to convert RawCid to Cid")
                    ),
                    loc.block_length()
                );
            }

            debug!("Finished indexing CAR file {}, found {} blocks", idx, count);
        }
        Ok(cids)
    }

    /// Check if the DataStore contains a block with the given multihash
    pub async fn contains(&self, hash: &Multihash<64>) -> Option<LocationHint> {
        // Stupid lookup, just look in each CAR file for the block, until found
        for idx in 0..self.tracked_car.len() {
            let path = self.tracked_car[idx].clone();
            let mut file = tokio::fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .await
                .ok()?;

            let (_, mut reader) = CarReader::open(&mut file).await.ok()?;
            let mut block_loc = None;
            {
                let mut blocks = reader.block_locations();
                while let Some(Ok(loc)) = blocks.next().await {
                    let Some(cid) = Cid::try_from(loc.cid()).ok() else {
                        continue;
                    };
                    if cid.hash() == hash {
                        block_loc = Some(loc);
                        break;
                    }
                }
            }
            if let Some(loc) = block_loc {
                return Some(LocationHint::new(idx, self.tracked_car[idx].clone(), loc));
            }
        }
        None
    }

    pub async fn contains_cid(&self, cid: &Cid) -> Option<LocationHint> {
        self.contains(cid.hash()).await
    }

    pub async fn get_block_by_hint(&self, hint: &LocationHint) -> Option<CarBlock> {
        // Check that the hint's car_index is valid
        if hint.car_index >= self.tracked_car.len() {
            return None;
        }
        // Check that the car_file in the hint matches the tracked_car at car_index
        if hint.car_file != self.tracked_car[hint.car_index] {
            return None;
        }

        // Open the CAR file and read the block at the given location
        let path = self.tracked_car[hint.car_index].clone();
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .ok()?;
        let (_, mut reader) = CarReader::open(&mut file).await.ok()?;
        let (section, _) = reader
            .read_section_at(hint.metadata().section_start())
            .await
            .ok()?;
        Some(section.block)
    }

    /// Get the block for a given multihash
    pub async fn get_block(&self, hash: &Multihash<64>) -> Option<CarBlock> {
        let hint = self.contains(hash).await?;
        self.get_block_by_hint(&hint).await
    }

    /// Get the block for a given CID
    pub async fn get_block_by_cid(&self, cid: Cid) -> Option<CarBlock> {
        self.get_block(&cid.hash()).await
    }

    /// Carefully shutdown the DataStore, closing any open CAR files
    pub fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

fn pretty_print_cid(cid: &Cid) -> String {
    match cid.version() {
        Version::V0 => format!("CIDv0: {}", cid.to_string_of_base(Base::Base58Btc).unwrap()),
        Version::V1 => format!(
            "CIDv1: {}",
            cid.to_string_of_base(Base::Base32Lower).unwrap()
        ),
    }
}
