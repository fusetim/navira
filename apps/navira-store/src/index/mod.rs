use cid::{Cid, multihash::Multihash};
use navira_car::tokio::{CarOpenError, CarReader, CarSectionReadError};
use navira_ipni::{ad::Advertisement, serde_json};
use std::path::PathBuf;
use tokio::{
    fs::OpenOptions,
    io::{AsyncRead, AsyncSeek},
};
use tokio_stream::StreamExt;

const MULTICODEC_DAG_JSON: u64 = 0x71;

#[derive(Debug)]
pub struct IndexProvider {
    adcar: PathBuf,
    head: Option<Cid>,
    advertised_hashes: Option<Vec<Multihash<64>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CAR opening file error: {0}")]
    CarOpen(CarOpenError),
    #[error("CAR reading error: {0}")]
    CarRead(CarSectionReadError),
    #[error("Too many roots / heads in the adcar file")]
    TooManyRoots,
    #[error("Invalid root CID in the adcar file")]
    InvalidRootCid,
    #[error("Missing root advertisement in the adcar file")]
    MissingRootAdvertisement,
    #[error("Failed to decode advertisement: {0}")]
    AdvertisementDecode(serde_json::Error),
}

impl IndexProvider {
    async fn load(&mut self) -> Result<(), LoadError> {
        // Load the head from the adcar file
        let file = OpenOptions::new().read(true).open(&self.adcar).await?;
        let (header, mut reader) = CarReader::open(file).await.map_err(LoadError::CarOpen)?;
        let root_cids = header.roots();
        // Expect at most 1 root CID in the adcar file
        if root_cids.len() > 1 {
            return Err(LoadError::TooManyRoots);
        }
        if root_cids.is_empty() {
            self.head = None;
            self.advertised_hashes = Some(Vec::new());
            return Ok(());
        }
        let Ok(head_cid) = Cid::try_from(root_cids[0].to_raw_cid()) else {
            return Err(LoadError::InvalidRootCid);
        };
        self.head = Some(head_cid);
        self.load_advertised_hashes(&mut reader, &head_cid.hash())
            .await?;
        Ok(())
    }

    async fn read_advertisement<R>(
        reader: &mut CarReader<R>,
        hash: &Multihash<64>,
    ) -> Result<Advertisement, LoadError>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        // Locate the block with the given hash in the CAR file
        let mut found = None;
        {
            let mut blocks = reader.block_locations();
            while let Some(Ok(loc)) = blocks.next().await {
                let Ok(cid) = Cid::try_from(loc.cid()) else {
                    continue;
                };
                if cid.hash() == hash {
                    found = Some(loc);
                    break;
                }
            }
        }
        if let Some(loc) = found {
            // Read the block data
            let (section, _) = reader
                .read_section_at(loc.section_start())
                .await
                .map_err(LoadError::CarRead)?;
            let block_data = section.block.0;
            // Here you can process the block_data as needed
            Advertisement::decode(&block_data).map_err(LoadError::AdvertisementDecode)
        } else {
            Err(LoadError::MissingRootAdvertisement)
        }
    }

    async fn load_advertised_hashes<R>(
        &mut self,
        reader: &mut CarReader<R>,
        root: &Multihash<64>,
    ) -> Result<(), LoadError>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        let advertisement = Self::read_advertisement(reader, root).await?;
        self.advertised_hashes = Some(Vec::new()); // TODO: Actually parse the advertisements
        Ok(())
    }
}

impl IndexProvider {
    pub async fn create(adcar: PathBuf) -> Self {
        Self {
            adcar,
            head: None,
            advertised_hashes: None,
        }
    }

    pub async fn open(adcar: PathBuf) -> Result<Self, LoadError> {
        let mut provider = Self {
            adcar,
            head: None,
            advertised_hashes: None,
        };
        provider.load().await?;
        Ok(provider)
    }

    pub async fn save(&mut self) -> Result<(), LoadError> {
        // TODO: Implement saving logic
        Ok(())
    }
}
