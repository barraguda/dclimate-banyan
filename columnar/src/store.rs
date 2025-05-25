//! Banyan store trait alias and helper functions.

use banyan::{
    store::{BlockWriter, BranchCache, MemStore as BanyanMemStore, ReadOnlyStore},
    Secrets,
};
use banyan_utils::tags::Sha256Digest;

/// A trait alias for the required capabilities of a Banyan store used by this library.
///
/// This simplifies type signatures for `DatastreamRowSeq`. The store must be readable,
/// writable (for transactions), cloneable, thread-safe, and have a static lifetime.
pub trait BanyanRowSeqStore:
    ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
{
}

// Blanket implementation: Any type satisfying the bounds implements the alias.
impl<S> BanyanRowSeqStore for S where
    S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
{
}

/// A concrete type alias for Banyan's in-memory store using `Sha256Digest`.
pub type MemStore = BanyanMemStore<Sha256Digest>;

/// Creates a new Banyan in-memory store with a specified maximum size.
///
/// # Arguments
///
/// * `max_size` - The maximum memory capacity of the store in bytes.
///
/// # Returns
///
/// A `MemStore` instance configured for use with this library.
pub fn memory_store(max_size: usize) -> MemStore {
    MemStore::new(max_size, Sha256Digest::digest)
}

// Potential future addition:
// pub fn ipfs_store(...) -> impl BanyanRowSeqStore { ... }
