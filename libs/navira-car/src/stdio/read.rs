use crate::{
    CarFormat, CarReader as SansIoCarReader, CarReaderError as SansIoCarReaderError,
    wire::{cid::RawLink, v1::SectionFormatError},
};
use std::{io, iter::FusedIterator};

/// Errors related to CarReader operations
#[derive(thiserror::Error, Debug)]
pub enum CarReaderError {
    /// Invalid data format
    #[error("Invalid data format")]
    InvalidFormat,
    #[error("Invalid header format")]
    InvalidHeader(ciborium::de::Error<std::io::Error>),
    #[error("Invalid CAR version, expected 2")]
    InvalidVersion,
    #[error("Invalid section format")]
    InvalidSectionFormat(SectionFormatError),
    /// No more sections available in the CAR file
    ///
    /// This error is returned when attempting to read a section but there are no more sections available in the CAR file.  
    /// For instance, when you reached the end of the inner CARv1 data in a CARv2 file and try to read another section, you will get this error.
    #[error("No more sections available in the CAR file")]
    EndOfSections,
    /// I/O error occurred during reading
    #[error("I/O error occurred during reading: {0}")]
    Io(#[from] std::io::Error),
}

/// A std-io wrapper to read CAR archives from any type that implements [std::io::Read] and [std::io::Seek].
///
/// # Examples
///
/// ## Reading in-memory all sections/blocks of a CAR archive.
///
/// ```
/// use std::io::Cursor;
/// use navira_car::CarFormat;
/// use navira_car::stdio::CarReader;
///
/// let car_bytes = include_bytes!("../res/carv2-basic.car");
/// let mut reader = CarReader::open(Cursor::new(car_bytes.as_ref())).unwrap();
/// assert_eq!(reader.get_format(), CarFormat::V2);
/// assert_eq!(reader.get_roots().len(), 1);
/// let sections: Vec<_> = reader.sections().map(|item| dbg!(item)).collect();
/// assert_eq!(sections.len(), 5);
/// ```
pub struct CarReader<R: std::io::Read + io::Seek> {
    inner: SansIoCarReader,
    reader: R,
}

/// An iterator over the sections of a CAR archive.
///
/// This iterator will decode every section present in the archive.
/// It might return errors if the underlying archive is incorrectly formatted,
/// or if there is an I/O error while reading the underlying reader.
pub struct CarSectionIterator<'a, R: std::io::Read + io::Seek> {
    car_reader: &'a mut CarReader<R>,
}

impl<R: io::Read + io::Seek> CarReader<R> {
    /// Handle the underlying error, if it is an IO error, it will try to read/seek where it needs to.
    /// Otherwise, this function will just map to the proper error.
    fn handle_underlying_error(&mut self, err: SansIoCarReaderError) -> Result<(), CarReaderError> {
        match err {
            SansIoCarReaderError::InvalidHeader(e) => Err(CarReaderError::InvalidHeader(e)),
            SansIoCarReaderError::InvalidVersion => Err(CarReaderError::InvalidVersion),
            SansIoCarReaderError::InvalidSectionFormat(e) => {
                Err(CarReaderError::InvalidSectionFormat(e))
            }
            SansIoCarReaderError::EndOfSections => Err(CarReaderError::EndOfSections),
            SansIoCarReaderError::InvalidFormat => Err(CarReaderError::InvalidFormat),
            SansIoCarReaderError::InsufficientData(offset, _) => {
                // We need to read more data from the underlying reader and feed it to the inner CarReader
                let mut buffer = vec![0u8; 1024];
                self.reader.seek(io::SeekFrom::Start(offset as u64))?;
                let bytes_read = self.reader.read(&mut buffer)?;
                if bytes_read == 0 {
                    return Err(CarReaderError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected end of file while reading CAR data",
                    )));
                }
                self.inner.receive_data(&buffer[..bytes_read], offset);
                // After feeding the new data, we can try to read again
                Ok(())
            }
            SansIoCarReaderError::PreconditionNotMet => {
                panic!(
                    "Precondition not met error should never be returned by the inner CarReader since we are not exposing any method that can cause it. This is a bug in the inner CarReader implementation."
                );
            }
        }
    }

    /// Reads the CAR header from the underlying reader and feeds it to the inner CarReader
    fn read_header(&mut self) -> Result<(), CarReaderError> {
        loop {
            match self.inner.read_header() {
                Ok(()) => return Ok(()),
                Err(e) => self.handle_underlying_error(e)?,
            }
        }
    }
}

impl<R: io::Read + io::Seek> CarReader<R> {
    /// Open a CAR archive.
    ///
    /// # Args
    /// * reader: The underlying byte reader which supports [std::io::Read] and [std::io::Seek].
    ///
    /// # Returns
    /// * `Ok(Self)`, if the CAR archive can be successfully opened (meaning at least the header could be decoded).
    /// * `Err(CarReaderError)`, otherwise, indicating the CAR archive is corrupted, invalid or just unsupported.
    pub fn open(reader: R) -> Result<Self, CarReaderError> {
        let mut car_reader = Self {
            inner: SansIoCarReader::new(),
            reader,
        };
        car_reader.read_header()?;
        Ok(car_reader)
    }

    /// Get the root CIDs of the archive as [RawLink].
    pub fn get_roots(&self) -> &[RawLink] {
        self.inner.header().unwrap().0.roots()
    }

    /// Get the CAR archive format
    pub fn get_format(&self) -> CarFormat {
        self.inner.get_format().unwrap()
    }

    /// Rewind the archive to its beggining.
    ///
    /// You probably do not need to use this function.
    pub fn rewind(&mut self) {
        self.inner.seek_first_section().unwrap();
    }

    /// Get an iterator over all the sections of the archive.
    pub fn sections(&mut self) -> CarSectionIterator<'_, R> {
        self.rewind();
        CarSectionIterator { car_reader: self }
    }
}

impl<R: io::Read + io::Seek> Iterator for CarSectionIterator<'_, R> {
    type Item = Result<crate::wire::v1::LocatableSection, CarReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.car_reader.inner.read_section() {
                Ok(section) => return Some(Ok(section)),
                Err(e) => match self.car_reader.handle_underlying_error(e) {
                    Ok(()) => continue, // We handled the error by reading more data, try to read the section again
                    Err(CarReaderError::Io(err))
                        if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        return None; // We reached the end of the underlying reader, return None to indicate that there are no more sections
                    }
                    Err(CarReaderError::EndOfSections) => return None, // We reached the end of the sections in the CAR file, return None to indicate that there are no more sections
                    Err(err) => return Some(Err(err)), // An unrecoverable error occurred, return it
                },
            }
        }
    }
}

impl<R: io::Read + io::Seek> FusedIterator for CarSectionIterator<'_, R> {}

#[cfg(test)]
mod tests {
    use crate::wire::cid::RawCid;

    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_car_reader_v1() {
        let car_bytes = include_bytes!("../res/carv1-basic.car");
        let mut reader = CarReader::open(Cursor::new(car_bytes.as_ref())).unwrap();
        let expected_roots = [
            RawLink::new(
                RawCid::from_hex(
                    "01711220f88bc853804cf294fe417e4fa83028689fcdb1b1592c5102e1474dbc200fab8b",
                )
                .unwrap(),
            ),
            RawLink::new(
                RawCid::from_hex(
                    "0171122069ea0740f9807a28f4d932c62e7c1c83be055e55072c90266ab3e79df63a365b",
                )
                .unwrap(),
            ),
        ];
        assert_eq!(reader.get_format(), CarFormat::V1);
        assert_eq!(reader.get_roots(), &expected_roots);
        let sections: Vec<_> = reader.sections().map(|item| dbg!(item)).collect();
        assert_eq!(sections.len(), 8);
        assert!(sections.iter().all(|s| s.is_ok()));
    }

    #[test]
    fn test_car_reader_v2() {
        let car_bytes = include_bytes!("../res/carv2-basic.car");
        let mut reader = CarReader::open(Cursor::new(car_bytes.as_ref())).unwrap();
        let expected_roots = [RawLink::new(
            RawCid::from_hex(
                "1220fb16f5083412ef1371d031ed4aa239903d84efdadf1ba3cd678e6475b1a232f8",
            )
            .unwrap(),
        )];
        assert_eq!(reader.get_format(), CarFormat::V2);
        assert_eq!(reader.get_roots(), &expected_roots);
        let sections: Vec<_> = reader.sections().map(|item| dbg!(item)).collect();
        assert_eq!(sections.len(), 5);
        assert!(sections.iter().all(|s| s.is_ok()));
    }
}
