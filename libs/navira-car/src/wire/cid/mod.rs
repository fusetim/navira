//! CID (Content Identifier) handling for CAR files.
//!
//! A CAR archive contains CIDs that identify the content of the blocks in the archive.
//! However, in most contexts (outside of validation), there is no need to actually parse the
//! CIDs, but just to treat them as opaque byte sequences.
//!
//! This module provides the [RawCid] struct, which is a simple wrapper around a byte vector
//! that represents a CID in its raw binary form.
//!
//! However, it also provides a method to try to parse a CID from a byte stream, which can be useful
//! for validating that the bytes conform to the expected structure of a CID (e.g., CIDv0 or CIDv1)
//! without needing to fully understand the internal structure of the CID (e.g., multihash coherence).
//!
//! ***TODO:** In the future, we will add the conversion fuctions to convert between RawCid and a
//! more structured CID type (e.g., using the [cid crate](https://crates.io/crates/cid)) to make CAR operations easier.*
mod cid;
mod link;
pub use cid::{CidFormatError, RawCid};
pub use link::{IntoRawLink, RawLink};
