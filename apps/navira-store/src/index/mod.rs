mod http;

use axum::{Router, routing::get};
use cid::{Cid, multihash::Multihash};
use navira_car::tokio::{CarSectionSerializationError, CarV1HeaderSerializationError};
use navira_car::wire::cid::RawCid;
use navira_car::wire::v1::{CarSection, CarSectionMetadata};
use navira_car::{
    tokio::{CarOpenError, CarReader, CarSectionReadError, CarWriter},
    wire::v1::{CarBlock, CarHeader as CarV1Header},
};
use navira_ipni::{
    ad::{
        Advertisement, EntryChunk, SignedHead,
        ipld::{Bytes, Link},
    },
    serde_json,
};
use navira_peer::identity::{IntoPeerId as _, ed25519::Ed25519Identity};
use sha2::{Digest, digest::Update};
use std::{collections::HashSet, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::{
    fs::OpenOptions,
    io::{AsyncRead, AsyncSeek},
    sync::RwLock,
};
use tokio_stream::StreamExt;

const MULTICODEC_DAG_JSON: u64 = 0x71;

#[derive(Debug)]
pub struct IndexProvider {
    adcar: PathBuf,
    head: Option<Cid>,
    advertised_hashes: Option<HashSet<Multihash<64>>>,
    signed_head: Option<SignedHead>,
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
            self.advertised_hashes = Some(HashSet::new());
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
        let _advertisement = Self::read_advertisement(reader, root).await?;
        self.advertised_hashes = Some(HashSet::new()); // TODO: Actually parse the advertisements
        Ok(())
    }

    async fn init_car(&mut self) -> Result<(), ChainUpdateError> {
        // Create a new CAR file with an empty header
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.adcar)
            .await
            .map_err(ChainUpdateError::AdcarIo)?;
        let header = CarV1Header::new(vec![]);
        let mut writer = CarWriter::new(file);
        writer
            .write_header(&header)
            .await
            .map_err(ChainUpdateError::WriteHeader)?;
        writer
            .finish()
            .await
            .flush()
            .await
            .map_err(ChainUpdateError::AdcarIo)?;
        Ok(())
    }

    async fn get_block_from_car(&self, hash: &Multihash<64>) -> Result<Option<CarSection>, ()> {
        let file = OpenOptions::new()
            .read(true)
            .open(&self.adcar)
            .await
            .map_err(|_| ())?;
        let (_, mut reader) = CarReader::open(file).await.map_err(|_| ())?;
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
            let (section, _) = reader
                .read_section_at(loc.section_start())
                .await
                .map_err(|_| ())?;
            return Ok(Some(section));
        }
        Ok(None)
    }

    async fn append_car(
        &mut self,
        blocks: Vec<CarSection>,
        head: Option<Cid>,
    ) -> Result<(), ChainUpdateError> {
        let new = tempfile::NamedTempFile::new().map_err(ChainUpdateError::TempFileIo)?;

        // Copy and append the blocks from the old CAR file to the new CAR file
        {
            let old = OpenOptions::new()
                .read(true)
                .open(&self.adcar)
                .await
                .map_err(ChainUpdateError::AdcarIo)?;
            let (_, mut reader) = CarReader::open(old)
                .await
                .map_err(ChainUpdateError::AdcarOpen)?;

            let new_file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(new.path())
                .await
                .map_err(ChainUpdateError::TempFileIo)?;

            let header = CarV1Header::new(head.map(|c| vec![RawCid::from(c)]).unwrap_or_default());
            let mut writer = CarWriter::new(new_file);

            // Write the header and the new blocks to the new CAR file
            writer
                .write_header(&header)
                .await
                .map_err(ChainUpdateError::WriteHeader)?;
            for block in blocks {
                writer
                    .write_section(&block)
                    .await
                    .map_err(ChainUpdateError::WriteSection)?;
            }

            // Write the existing blocks from the old CAR file to the new CAR file
            let mut section_stream = reader.sections();
            while let Some(Ok(section)) = section_stream.next().await {
                writer
                    .write_section(&section)
                    .await
                    .map_err(ChainUpdateError::WriteSection)?;
            }

            let mut writer = writer.finish().await;
            writer.flush().await.map_err(ChainUpdateError::TempFileIo)?;
            writer
                .shutdown()
                .await
                .map_err(ChainUpdateError::TempFileIo)?;
        }

        // Replace the old CAR file with the new CAR file
        tokio::fs::copy(new.path(), &self.adcar)
            .await
            .map_err(ChainUpdateError::Replace)?;
        let _ = new.close(); // Try to close / delete the temporary file, ignoring any errors

        Ok(())
    }
}

impl IndexProvider {
    pub async fn create(adcar: PathBuf) -> Result<Self, LoadError> {
        let mut this = Self {
            adcar,
            head: None,
            advertised_hashes: None,
            signed_head: None,
        };
        this.init_car().await.map_err(|_| {
            LoadError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to initialize CAR file",
            ))
        })?;
        Ok(this)
    }

    pub async fn open(adcar: PathBuf) -> Result<Self, LoadError> {
        let mut provider = Self {
            adcar,
            head: None,
            advertised_hashes: None,
            signed_head: None,
        };
        provider.load().await?;
        Ok(provider)
    }

    pub fn head(&self) -> Option<Cid> {
        self.head
    }

    pub async fn announce_hashes(
        &mut self,
        identity: &Ed25519Identity,
        hashes: &[Multihash<64>],
    ) -> Result<usize, AnnounceError> {
        // Remove the hashes that are already advertised
        let empty = HashSet::new();
        let advertised = self.advertised_hashes.as_ref().unwrap_or(&empty);
        let filtered = hashes
            .iter()
            .filter(|h| !advertised.contains(h))
            .cloned()
            .collect::<Vec<_>>();

        let entry_count = filtered.len();
        if entry_count == 0 {
            return Ok(0);
        }

        let entry = EntryChunk::with_hashes(filtered.clone(), None);
        let entry = serde_json::to_string(&entry).map_err(AnnounceError::EntrySerialization)?;
        if entry.len() > (1 << 21) {
            // 2 MiB
            return Err(AnnounceError::EntryChunkTooBig);
        }
        let entry = CarBlock(entry.into());
        let entry_cid = cidify(&entry, MULTICODEC_DAG_JSON);
        let entry = CarSection::new(
            CarSectionMetadata::new(entry.len() as u64, entry_cid.into()),
            entry,
        );

        let provider_peerid = identity.into_peerid().to_base32();
        let mut announcement = Advertisement {
            previous_id: self.head().map(Link::from),
            provider: provider_peerid,
            addresses: vec!["/dns4/navira.fusetim.space/tcp/443/https".to_string()],
            signature: Bytes::from(vec![]),
            entries: Link::from(entry_cid),
            context_id: Bytes::from(b"navira-store-alpha01".to_vec()),
            metadata: Bytes::from(vec![0x09, 0x20]),
            is_rm: false,
            extended_provider: None,
        };
        announcement.signature = {
            let announcement = serde_json::to_string(&announcement)
                .map_err(AnnounceError::AdvertisementSerialization)?;
            Bytes::from(identity.sign(&announcement.as_bytes()).to_vec())
        };
        let announcement = serde_json::to_string(&announcement)
            .map_err(AnnounceError::AdvertisementSerialization)?;
        let announcement = CarBlock(announcement.into());
        let announcement_cid = cidify(&announcement, MULTICODEC_DAG_JSON);
        let announcement = CarSection::new(
            CarSectionMetadata::new(announcement.len() as u64, announcement_cid.into()),
            announcement,
        );

        self.append_car(vec![announcement, entry], Some(announcement_cid))
            .await
            .map_err(AnnounceError::AdChainUpdate)?;

        self.head = Some(announcement_cid);
        self.advertised_hashes
            .get_or_insert_with(HashSet::new)
            .extend(filtered.into_iter());

        let sig = Bytes::from(identity.sign(&announcement_cid.to_bytes()).to_vec());
        self.signed_head = Some(SignedHead {
            head: Link::from(announcement_cid),
            topic: None,
            pubkey: Bytes::from(identity.public_key()),
            sig,
        });
        Ok(entry_count)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum AnnounceError {
    #[error("WIP: EntryChunk is too big to be announced")]
    EntryChunkTooBig,
    #[error("Entry serialization failed: {0}")]
    EntrySerialization(serde_json::Error),
    #[error("Advertisement serialization failed: {0}")]
    AdvertisementSerialization(serde_json::Error),
    #[error("Ad chain update error: {0}")]
    AdChainUpdate(ChainUpdateError),
}

#[derive(thiserror::Error, Debug)]
pub enum ChainUpdateError {
    #[error("Failed to write header: {0}")]
    WriteHeader(CarV1HeaderSerializationError),
    #[error("Failed to write section: {0}")]
    WriteSection(CarSectionSerializationError),
    #[error("Failed to perform temp file I/O: {0}")]
    TempFileIo(std::io::Error),
    #[error("Failed to replace temp file: {0}")]
    Replace(std::io::Error),
    #[error("Failed to create adcar: {0}")]
    AdcarIo(std::io::Error),
    #[error("Failed to open adcar: {0}")]
    AdcarOpen(CarOpenError),
}

fn cidify(block: &CarBlock, codec: u64) -> Cid {
    const SHA256_CODE: u64 = 0x12; // Multicodec code for SHA-256

    // Use SHA256 as the hash function for CIDv1
    let digest = sha2::Sha256::new().chain(block.as_ref()).finalize();
    let hash = Multihash::wrap(SHA256_CODE, &digest.as_slice()).unwrap();
    Cid::new_v1(codec, hash)
}

pub fn new_ipni_service() -> Router<Arc<RwLock<IndexProvider>>> {
    Router::new()
        .route("/ipni/v1/ad/head", get(http::get_head))
        .route("/ipni/v1/ad/{cid}", get(http::get_cid))
}
