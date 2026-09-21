use super::{MAX_HEADER_SIZE, MAX_SECTION_METADATA_SIZE, resize_buf_hint};
use crate::sio::{self, MAX_BLOCK_SIZE};
use crate::wire::v1::{CarHeader as CarV1Header, CarSection};
use std::num::NonZeroUsize;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt as _};

pub struct CarWriter<W>
where
    W: AsyncWrite + AsyncSeek + Unpin,
{
    writer: W,
    header_start: usize,
    section_start: Option<NonZeroUsize>,
}

impl<W> CarWriter<W>
where
    W: AsyncWrite + AsyncSeek + Unpin,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            header_start: 0,
            section_start: None,
        }
    }

    pub fn new_with_offset(writer: W, offset: usize) -> Self {
        Self {
            writer,
            header_start: offset,
            section_start: None,
        }
    }

    pub async fn write_header(
        &mut self,
        header: &CarV1Header,
    ) -> Result<usize, CarV1HeaderSerializationError> {
        let mut buf: Vec<u8> = vec![0u8; 1024];
        match sio::write_v1_header(&mut buf, header) {
            Ok(bytes_written) => {
                self.writer
                    .seek(std::io::SeekFrom::Start(self.header_start as u64))
                    .await?;
                self.writer.write_all(&buf[..bytes_written]).await?;
                self.section_start =
                    Some(NonZeroUsize::new(self.header_start + bytes_written).unwrap());
                Ok(bytes_written)
            }
            Err(sio::CarV1HeaderSerializationError::BufferTooSmall { needed }) => {
                if !resize_buf_hint(&mut buf, 0, needed, NonZeroUsize::new(MAX_HEADER_SIZE)) {
                    return Err(CarV1HeaderSerializationError::HeaderTooLarge {
                        needed: needed.unwrap_or_else(|| NonZeroUsize::new(0).unwrap()),
                        max: NonZeroUsize::new(MAX_HEADER_SIZE).unwrap(),
                    });
                }
                let bytes_written =
                    sio::write_v1_header(&mut buf, header).map_err(|e| match e {
                        sio::CarV1HeaderSerializationError::CborSerialization(e) => {
                            CarV1HeaderSerializationError::CborSerialization(e)
                        }
                        sio::CarV1HeaderSerializationError::BufferTooSmall { needed } => {
                            CarV1HeaderSerializationError::HeaderTooLarge {
                                needed: needed.unwrap_or_else(|| NonZeroUsize::new(0).unwrap()),
                                max: NonZeroUsize::new(MAX_HEADER_SIZE).unwrap(),
                            }
                        }
                    })?;
                self.writer
                    .seek(std::io::SeekFrom::Start(self.header_start as u64))
                    .await?;
                self.writer.write_all(&buf[..bytes_written]).await?;
                self.section_start =
                    Some(NonZeroUsize::new(self.header_start + bytes_written).unwrap());
                Ok(bytes_written)
            }
            Err(sio::CarV1HeaderSerializationError::CborSerialization(e)) => {
                Err(CarV1HeaderSerializationError::CborSerialization(e))
            }
        }
    }

    pub async fn write_section(
        &mut self,
        section: &CarSection,
    ) -> Result<usize, CarSectionSerializationError> {
        if section.length() > (MAX_BLOCK_SIZE as u64) {
            return Err(CarSectionSerializationError::BlockTooLarge {
                needed: NonZeroUsize::new(section.length() as usize).unwrap(),
                max: NonZeroUsize::new(MAX_BLOCK_SIZE).unwrap(),
            });
        }

        let mut buf: Vec<u8> = vec![0u8; 1024];
        let mut bytes_written = 0;
        loop {
            match sio::write_section(&mut buf, section) {
                Ok(n) => {
                    bytes_written = n;
                    break;
                }
                Err(sio::CarSectionSerializationError::BufferTooSmall { needed }) => {
                    if !resize_buf_hint(
                        &mut buf,
                        0,
                        needed,
                        NonZeroUsize::new(MAX_SECTION_METADATA_SIZE + MAX_BLOCK_SIZE),
                    ) {
                        return Err(CarSectionSerializationError::SectionTooLarge {
                            needed: needed.unwrap_or_else(|| NonZeroUsize::new(0).unwrap()),
                            max: NonZeroUsize::new(MAX_SECTION_METADATA_SIZE + MAX_BLOCK_SIZE)
                                .unwrap(),
                        });
                    }
                }
                Err(sio::CarSectionSerializationError::CborSerialization(e)) => {
                    return Err(CarSectionSerializationError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("CBOR serialization error: {}", e),
                    )));
                }
            }
        }

        self.writer.write_all(&buf[..bytes_written]).await?;
        Ok(bytes_written)
    }

    pub async fn finish(self) -> W {
        self.writer
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CarV1HeaderSerializationError {
    #[error("CBOR serialization error: {0}")]
    CborSerialization(ciborium::ser::Error<std::io::Error>),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error(
        "CAR Header is too large, needed at least {needed} bytes, maximum allowed: {max} bytes"
    )]
    HeaderTooLarge {
        needed: NonZeroUsize,
        max: NonZeroUsize,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum CarSectionSerializationError {
    #[error("Block is too large, needed at least {needed} bytes, maximum allowed: {max} bytes")]
    BlockTooLarge {
        needed: NonZeroUsize,
        max: NonZeroUsize,
    },
    #[error(
        "CAR Section too large, metadata (CID) must be too large. Got {needed} bytes, maximum allowed: {max} bytes"
    )]
    SectionTooLarge {
        needed: NonZeroUsize,
        max: NonZeroUsize,
    },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
