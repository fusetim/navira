use std::num::NonZeroUsize;

use crate::sio::{
    self, CarSectionDeserialiationError, CarV1HeaderDeserializationError, MAX_BLOCK_SIZE,
    ReadCursor,
};
use crate::wire::v1::{self, CarBlock, CarSection, CarSectionMetadata};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};
use tokio_stream::Stream;
//use log::{debug};

pub const MAX_HEADER_SIZE: usize = 1 << 14; // 16 KiB
pub const MAX_SECTION_METADATA_SIZE: usize = 1 << 10; // 1 KiB (for the section metadata, which is typically small)

/// A reader for CAR files that implements `AsyncRead` from the `tokio` crate.
pub struct CarReader<R: AsyncRead + AsyncSeek + Unpin> {
    reader: R,
    data_start: usize,
}

impl<R: AsyncRead + AsyncSeek + Unpin> CarReader<R> {
    /// Opens a new `CarReader` from an `AsyncRead` instance.
    pub async fn open(mut reader: R) -> Result<(v1::CarHeader, Self), CarOpenError> {
        // Try to read the header first
        let mut buf: Vec<u8> = vec![0u8; 1024]; // Start with a buffer of 1KB
        let mut bytes_read = reader.read(&mut buf).await?;
        loop {
            let cursor = ReadCursor::new(&buf, 0);
            match sio::read_header(&cursor) {
                Ok((header, header_length)) => {
                    return Ok((
                        header,
                        Self {
                            reader,
                            data_start: header_length,
                        },
                    ));
                }
                Err(CarV1HeaderDeserializationError::InsufficientData { hint }) => {
                    // If the buffer is full or the hint suggests the buffer is too small,
                    // resize it
                    if !resize_buf_hint(
                        &mut buf,
                        bytes_read,
                        hint,
                        NonZeroUsize::new(MAX_HEADER_SIZE),
                    ) {
                        return Err(CarOpenError::HeaderTooLarge(MAX_HEADER_SIZE, buf.len()));
                    }
                    // Read more data into the buffer
                    let min_length = hint.map_or(1, |h| h.get()) + bytes_read;
                    // While we haven't read enough bytes to satisfy the hint, keep reading
                    while bytes_read < min_length {
                        let read = reader.read(&mut buf[bytes_read..]).await?;
                        if read == 0 {
                            return Err(CarOpenError::Truncated(min_length, bytes_read));
                        }
                        bytes_read += read;
                    }
                }
                Err(e) => return Err(CarOpenError::BadArchive(e)),
            }
        }
    }

    pub async fn seek_data_start(&mut self) -> Result<(), std::io::Error> {
        self.reader
            .seek(std::io::SeekFrom::Start(self.data_start as u64))
            .await?;
        Ok(())
    }

    pub fn close(self) -> R {
        self.reader
    }

    pub fn sections(&mut self) -> CarSectionStream<'_, R> {
        let section_start = self.data_start;
        CarSectionStream {
            reader: Some(self),
            section_start,
            read_fut: None,
            end_of_file: false,
        }
    }

    async fn read_section_metadata(
        &mut self,
    ) -> Result<(CarSectionMetadata, usize), CarSectionReadError> {
        let mut buf: Vec<u8> = vec![0u8; 1024]; // Start with a buffer of 1KB
        let mut bytes_read = 0;
        loop {
            let read = self.reader.read(&mut buf).await?;
            bytes_read += read;
            //debug!("read_section_metadata > section metadata read {} bytes, total bytes read: {}", read, bytes_read);
            if read == 0 {
                if bytes_read == 0 {
                    return Err(CarSectionReadError::EndOfFile);
                } else {
                    return Err(CarSectionReadError::UnexpectedEof);
                }
            }
            let cursor = ReadCursor::new(&buf[..bytes_read], 0);
            match sio::read_section_metadata(&cursor) {
                Ok((metadata, metadata_length)) => {
                    // Seek back to the position after the metadata for the next read
                    self.reader
                        .seek(std::io::SeekFrom::Current(
                            -(bytes_read as i64) + (metadata_length as i64),
                        ))
                        .await?;
                    return Ok((metadata, metadata_length));
                }
                Err(CarSectionDeserialiationError::InsufficientData { hint }) => {
                    // If the buffer is full or the hint suggests the buffer is too small,
                    // resize it
                    if !resize_buf_hint(
                        &mut buf,
                        bytes_read,
                        hint,
                        NonZeroUsize::new(MAX_SECTION_METADATA_SIZE),
                    ) {
                        return Err(CarSectionReadError::SectionMetadataTooLarge(
                            MAX_SECTION_METADATA_SIZE,
                            bytes_read,
                        ));
                    }
                }
                Err(CarSectionDeserialiationError::TooLargeBlockSize(size)) => {
                    return Err(CarSectionReadError::BlockTooLarge(MAX_BLOCK_SIZE, size));
                }
                Err(CarSectionDeserialiationError::UnsupportedCid) => {
                    return Err(CarSectionReadError::UnsupportedCid);
                }
            }
        }
    }

    async fn read_section(&mut self) -> Result<(CarSection, usize), CarSectionReadError> {
        // Read the section metadata first
        let (metadata, metadata_length) = self.read_section_metadata().await?;
        //debug!("section_metadata_len: {} bytes", metadata_length);

        // Ensure that the block size does not exceed the maximum allowed size
        let block_length = metadata.length as usize;
        //debug!("section_block_len: {} bytes", block_length);
        if block_length > MAX_BLOCK_SIZE {
            return Err(CarSectionReadError::BlockTooLarge(
                MAX_BLOCK_SIZE,
                block_length,
            ));
        }

        let mut block_buf: Vec<u8> = vec![0u8; block_length];
        let res = self.reader.read_exact(&mut block_buf).await;
        match res {
            Ok(_) => {
                let section = CarSection {
                    metadata,
                    block: CarBlock(block_buf),
                };
                Ok((section, metadata_length + block_length))
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(CarSectionReadError::UnexpectedEof);
                } else {
                    return Err(CarSectionReadError::Io(e));
                }
            }
        }
    }

    async fn seek_to(&mut self, position: usize) -> Result<(), std::io::Error> {
        self.reader
            .seek(std::io::SeekFrom::Start(position as u64))
            .await?;
        Ok(())
    }

    async fn read_section_metadata_at(
        &mut self,
        position: usize,
    ) -> Result<(CarSectionMetadata, usize), CarSectionReadError> {
        //debug!("read_section_metadata_at > seeking to position: {}", position);
        self.seek_to(position).await?;
        self.read_section_metadata().await
    }

    async fn read_section_at(
        &mut self,
        position: usize,
    ) -> Result<(CarSection, usize), CarSectionReadError> {
        //debug!("read_section_at > seeking to position: {}", position);
        self.seek_to(position).await?;
        self.read_section().await
    }
}

pub struct CarSectionStream<'a, R: AsyncRead + AsyncSeek + Unpin> {
    reader: Option<&'a mut CarReader<R>>,
    section_start: usize,
    read_fut: Option<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = (
                            &'a mut CarReader<R>,
                            Result<(CarSection, usize), CarSectionReadError>,
                        ),
                    > + Send
                    + 'a,
            >,
        >,
    >,
    end_of_file: bool,
}

impl<'a, R> Stream for CarSectionStream<'a, R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send,
{
    type Item = Result<CarSection, CarSectionReadError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // Init: If we don't have a read future, create one to read the next section
        let section_start = this.section_start;
        if this.read_fut.is_none() {
            let reader = this
                .reader
                .take()
                .expect("section stream reader unavailable");
            let fut = async move {
                let result = reader.read_section_at(section_start).await;
                (reader, result)
            };
            this.read_fut = Some(Box::pin(fut));
        }

        // Poll the current read future to see if the section has been read
        let mut fut = this
            .read_fut
            .take()
            .expect("section read future unavailable");
        let result = fut.as_mut().poll(cx);
        match result {
            std::task::Poll::Ready((reader, Ok((section, consumed)))) => {
                this.reader = Some(reader);
                this.section_start += consumed;
                std::task::Poll::Ready(Some(Ok(section)))
            }
            std::task::Poll::Ready((reader, Err(e))) => {
                this.reader = Some(reader);
                if let CarSectionReadError::EndOfFile = e {
                    this.end_of_file = true;
                    std::task::Poll::Ready(None)
                } else {
                    std::task::Poll::Ready(Some(Err(e)))
                }
            }
            std::task::Poll::Pending => {
                this.read_fut = Some(fut);
                std::task::Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.end_of_file {
            (0, Some(0))
        } else {
            (0, None)
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CarOpenError {
    #[error("Failed to open CAR file: {0}")]
    BadArchive(#[from] CarV1HeaderDeserializationError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Truncated CAR file: expected at least {0} bytes, got {1} bytes")]
    Truncated(usize, usize),
    #[error("Header too large: expected at most {0} bytes, got {1} bytes")]
    HeaderTooLarge(usize, usize),
}

#[derive(thiserror::Error, Debug)]
pub enum CarSectionReadError {
    #[error("Unsupported block CID")]
    UnsupportedCid,
    #[error("Block too large: expected at most {0} bytes, got {1} bytes")]
    BlockTooLarge(usize, usize),
    #[error("Section metadata too large: expected at most {0} bytes, got {1} bytes")]
    SectionMetadataTooLarge(usize, usize),
    #[error("Unexpected end of file while reading section")]
    UnexpectedEof,
    #[error("End of file")]
    EndOfFile,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Resize the buffer based on the hint provided.
///
/// This function will only try to resize the buffer, if the hint suggests that the
/// buffer is too small or that the buffer is already full.
/// It will double the size of the buffer, unless it is already at or above the maximum size.
///
/// Returns false if the buffer needed to be resized but is already at or above the maximum size, true otherwise.
fn resize_buf_hint(
    buf: &mut Vec<u8>,
    bytes_read: usize,
    hint: Option<NonZeroUsize>,
    max_size: Option<NonZeroUsize>,
) -> bool {
    if bytes_read == buf.len() || hint.map_or(false, |h| (bytes_read + h.get()) > buf.len()) {
        // If the buffer is already at or above the maximum size, return an error
        if let Some(max) = max_size {
            if buf.len() >= max.get() {
                return false;
            }
        }
        buf.resize(buf.len() * 2, 0); // Double the buffer size
    }
    true
}
