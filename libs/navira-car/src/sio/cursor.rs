use std::ops::Deref;

pub struct ReadCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ReadCursor<'a> {
    pub fn new(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }

    /// Get current position in the overall buffer
    pub fn position(&self) -> usize {
        self.pos
    }
}

impl Deref for ReadCursor<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}
