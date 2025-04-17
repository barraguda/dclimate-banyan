//! # Banyan Columnar Storage Library
//!
//! This crate provides columnar storage capabilities built on top of the
//! [Banyan](https://docs.rs/banyan/latest/banyan/) persistence layer.
//!
//! It allows storing structured data efficiently in columns, leveraging Banyan's
//! content-addressed storage and Merkle B+tree structures for indexing and querying.
//! This approach is particularly useful for analytical workloads where queries often
//! access only a subset of columns for a large number of rows.
//!
//! ## Key Features
//!
//! *   **Columnar Layout:** Data is stored column by column, improving compression
//!     and I/O efficiency for column-subset queries.
//! *   **Banyan Integration:** Built directly on Banyan's storage primitives, inheriting
//!     its properties like content addressing, deduplication (where applicable), and
//!     verifiability.
//! *   **Querying:** Supports filtering data based on values within columns using
//!     Banyan's range query capabilities, adapted for columnar access.
//! *   **Data Management:** Provides abstractions (`ColumnarDatastreamManager`,
//!     `ColumnarBanyanStore`) for managing streams of columnar data.
//!
//! ## Core Concepts
//!
//! *   **`ColumnarDatastreamManager`:** The main entry point for managing different
//!     named streams of columnar data.
//! *   **`ColumnarBanyanStore`:** Represents a single stream of columnar data,
//!     handling writes, reads, and indexing.
//! *   **`ColumnDefinition`, `DataDefinition`:** Define the schema (column names and types)
//!     of the stored data.
//! *   **`Record`, `Value`:** Represent rows and individual data cell values.
//! *   **`ValueFilter`, `Comparison`:** Used to specify query conditions.
//!
//! ## Example Usage (Conceptual)
//!
//! ```rust,ignore
//! // This is a conceptual example and might not compile directly
//! use banyan_columnar::{
//!     ColumnarDatastreamManager, ColumnDefinition, ColumnType, DataDefinition,
//!     Record, Value, Config, Secrets, Sha256Digest // Assuming necessary imports
//! };
//! use banyan::store::mem::MemStore; // Example using an in-memory store
//!
//! async fn run() -> anyhow::Result<()> {
//!     let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
//!     let secrets = Secrets::default(); // Or load your actual secrets
//!     let config = Config::default(); // Or configure as needed
//!
//!     let manager = ColumnarDatastreamManager::new(config, secrets, store);
//!
//!     // Define the schema
//!     let schema = DataDefinition::new(vec![
//!         ColumnDefinition::new("timestamp".into(), ColumnType::Timestamp),
//!         ColumnDefinition::new("value".into(), ColumnType::Float64),
//!         ColumnDefinition::new("sensor_id".into(), ColumnType::Text),
//!     ])?;
//!
//!     // Get or create a columnar store for a specific stream name
//!     let mut columnar_store = manager.get_or_create_store("sensor_data".into(), schema).await?;
//!
//!     // Prepare some data
//!     let record1 = Record::new(vec![
//!         Value::Timestamp(1678886400_000_000_000), // Example timestamp
//!         Value::Float64(23.5),
//!         Value::Text("sensor_A".into()),
//!     ]);
//!     // ... add more records
//!
//!     // Append data
//!     columnar_store.append(vec![record1]).await?;
//!
//!     // Querying (details depend on specific query interface)
//!     // let results = columnar_store.query(...).await?;
//!
//!     Ok(())
//! }
//! ```

// --- Modules ---

/// Compression algorithms and utilities. (Potentially internal)
mod compression;
/// Iterators for traversing columnar data. (Potentially internal)
mod iterator;
/// Core data management traits and implementations.
pub mod manager;
/// Query representation and execution logic.
pub mod query;
/// Core data types, schema definitions, and Banyan tree type definitions.
pub mod types;

/// Tests module (only compiled when testing)
#[cfg(test)]
mod test;

// --- Main Library Exports ---
// Re-export core Banyan configuration and secrets types for convenience.
pub use banyan::{Config, Secrets};
// Re-export the digest type used for links/hashes.
pub use banyan_utils::tags::Sha256Digest; // Or your specific Digest type if different

/// Exports related to data management and storage access.
pub mod store {
    pub use crate::manager::{ColumnarBanyanStore, ColumnarDatastreamManager};
}

/// Exports related to querying and filtering data.
pub mod querying {
    pub use crate::query::{Comparison, ValueFilter};
    // Potentially add Query structure or QueryBuilder here if they exist.
}

/// Exports related to data structures, schema, and types used by the library.
pub mod data {
    pub use crate::types::{
        ColumnChunk, ColumnDefinition, ColumnType, ColumnarTreeTypes, DataDefinition, Record,
        RichRangeKey, RichRangeSummary, Value,
    };
    // Consider if ColumnChunk, RichRangeKey, RichRangeSummary are truly needed
    // in the public API or if they are implementation details. Start minimal.
    // Keep: ColumnDefinition, ColumnType, DataDefinition, Record, Value as these define data.
    // Keep: ColumnarTreeTypes maybe, if users need to interact with Banyan's specifics.
}

// --- Optional: Prelude ---
// If the library grows and certain types are used very frequently,
// you might consider adding a prelude module.
// pub mod prelude {
//     pub use crate::manager::{ColumnarBanyanStore, ColumnarDatastreamManager};
//     pub use crate::query::{Comparison, ValueFilter};
//     pub use crate::types::{ColumnDefinition, ColumnType, DataDefinition, Record, Value};
//     pub use banyan::{Config, Secrets};
//     pub use banyan_utils::tags::Sha256Digest;
// }
