//! A time-series storage library using Banyan for efficient persistence.
//!
//! This library stores time-series data, defined by a user-provided schema
//! (`DataDefinition`), in a single Banyan tree. Data is chunked and stored
//! columnarly within `RowSeqData` leaves, allowing for type-specific compression.
//!
//! Features:
//! - Schema definition (`DataDefinition`, `ColumnDefinition`, `ColumnType`).
//! - Efficient columnar storage and compression (`RowSeqData`).
//! - Banyan tree integration for scalable indexing based on row offset and timestamp.
//! - Append-only data extension (`DatastreamRowSeq::extend`).
//! - Flexible querying with filtering by:
//!   - Absolute row offset (`query` with `offset_range`).
//!   - Timestamp (`query` with `time_range_micros`).
//!   - Column values (`query` with `filters`).
//! - Optional state persistence (`load_or_initialize`, `save_state`).
//! - Pluggable storage backend (`BanyanRowSeqStore` trait, `memory_store` helper).
//!
//! # Quick Start
//!
//! ```no_run
//! use banyan_row_seq_storage::{
//!     memory_store, BanyanRowSeqError, ColumnDefinition, ColumnType,
//!     DataDefinition, DatastreamRowSeq, Record, UserFilter, UserFilterOp, Value,
//! };
//! use banyan::{Config, Secrets};
//! use std::{collections::BTreeMap, ops::Bound, sync::Arc};
//! use anyhow::Result; // Use anyhow Result for example simplicity
//!
//! fn main() -> Result<()> {
//!     // 1. Define Schema
//!     let schema = DataDefinition::new(vec![
//!         ColumnDefinition { name: "timestamp".to_string(), column_type: ColumnType::Timestamp },
//!         ColumnDefinition { name: "temperature".to_string(), column_type: ColumnType::Float },
//!         ColumnDefinition { name: "device_id".to_string(), column_type: ColumnType::String },
//!     ]);
//!
//!     // 2. Setup Store and Datastream
//!     let store = memory_store(1024 * 1024); // 1MB in-memory store
//!     let config = Config::default(); // Default Banyan config
//!     let secrets = Secrets::default(); // Default secrets (no encryption)
//!
//!     let mut ds = DatastreamRowSeq::load_or_initialize(
//!         store,
//!         config,
//!         secrets,
//!         None, // No persistence file path
//!         schema,
//!     )?;
//!
//!     // 3. Extend with Data
//!     let mut record1 = Record::new();
//!     record1.insert("timestamp".to_string(), Some(Value::Timestamp(1_000_000))); // 1 second epoch
//!     record1.insert("temperature".to_string(), Some(Value::Float(23.5)));
//!     record1.insert("device_id".to_string(), Some(Value::String("dev-01".to_string())));
//!
//!     let mut record2 = Record::new();
//!     record2.insert("timestamp".to_string(), Some(Value::Timestamp(2_000_000))); // 2 seconds epoch
//!     record2.insert("temperature".to_string(), Some(Value::Float(24.1)));
//!     record2.insert("device_id".to_string(), Some(Value::String("dev-02".to_string())));
//!
//!     ds.extend(&[record1, record2])?;
//!     println!("Extended datastream. Total rows: {}", ds.total_rows());
//!
//!     // 4. Query Data
//!     let results_iter = ds.query(
//!         vec!["timestamp".to_string(), "temperature".to_string()], // Select columns
//!         (Bound::Unbounded, Bound::Unbounded), // All offsets
//!         vec![], // No value filters
//!         (Bound::Included(1_500_000), Bound::Unbounded), // Time >= 1.5s
//!     )?;
//!
//!     println!("Query Results:");
//!     for result in results_iter {
//!         match result {
//!             Ok(record) => println!("  {:?}", record),
//!             Err(e) => eprintln!("  Error retrieving record: {}", e),
//!         }
//!     }
//!
//!     // 5. (Optional) Save State if path was provided
//!     // ds.save_state()?;
//!
//!     Ok(())
//! }
//! ```

// Declare modules, making their public items accessible within the crate
mod compression;
mod datastream;
mod error;
mod iterator;
mod query;
// mod store;
mod types;

// #[cfg(test)]
// mod test;

// --- Public Re-exports ---
// Re-export the most commonly used types and functions for easier access.

// Core data model and schema types
pub use crate::types::{
    ChunkKey, ChunkSummary, ColumnDefinition, ColumnType, DataDefinition, Record, UserFilter,
    UserFilterOp, Value,
};

// Main datastream entry point
pub use crate::datastream::DatastreamRowSeq;

// Error type
pub use crate::error::BanyanRowSeqError;

use banyan::store::{BlockWriter, ReadOnlyStore};
// Re-export key Banyan configuration types
pub use banyan::{Config, Secrets};

// Re-export key external types needed for interaction
pub use anyhow::Result;
use banyan_utils::tags::Sha256Digest;
// Often useful for application-level error handling
pub use libipld::Cid;

// Example: Re-exporting IPFS store if it were commonly used directly
// pub use banyan_utils::ipfs::IpfsStore;

// Example: Re-exporting digest type if needed externally
// pub use banyan_utils::tags::Sha256Digest;

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
