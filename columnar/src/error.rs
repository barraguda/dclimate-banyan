//! Error types for the banyan-row-seq storage library.
use crate::types::Value;
use thiserror::Error;

/// The primary error type for operations within this library.
#[derive(Error, Debug)]
pub enum BanyanRowSeqError {
    /// Indicates that a column name specified in an operation (e.g., query, filter)
    /// does not exist in the `DataDefinition` schema.
    #[error("Column '{0}' not found in data definition")]
    ColumnNotFound(String),

    /// Occurs when a value provided for a column does not match the `ColumnType`
    /// defined in the schema during insertion or filtering.
    #[error("Type mismatch for column '{column}': expected {expected}, got value {actual:?}")]
    TypeError {
        column: String,
        expected: String, // Consider using `crate::types::ColumnType` display here
        actual: Option<Value>, // Value that caused the error
    },

    /// An error occurred during the compression of column data.
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// An error occurred during the decompression of column data.
    #[error("Decompression error: {0}")]
    DecompressionError(String),

    /// Indicates an inconsistency found during decompression where the number of
    /// non-null values indicated by the presence bitmap does not match the
    /// actual number of values decoded from the data stream.
    #[error("Bitmap/Value count mismatch for column '{column}': bitmap {bitmap_count}, values {value_count}")]
    BitmapValueMismatch {
        column: String,
        bitmap_count: u64,
        value_count: usize,
    },

    /// An error occurred within the chunk processing logic of the result iterator.
    #[error("Chunk Iterator error: {0}")]
    ChunkIteratorError(String),

    /// An error occurred while reconstructing a specific row from decompressed columns.
    #[error("Row reconstruction error at relative index {index} for column {column}: {reason}")]
    ReconstructionError {
        index: u32,
        column: String,
        reason: String,
    },

    /// Indicates a mismatch between the expected data schema and the actual schema
    /// encountered (e.g., when loading persisted state).
    #[error("Data Definition mismatch")]
    SchemaMismatch,

    /// Represents an error in the user-provided query parameters (e.g., invalid range,
    /// incompatible filter types).
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Wraps an underlying error from the Banyan crate or the storage backend.
    #[error("Underlying Banyan/Store error: {0}")]
    StoreError(#[from] anyhow::Error),
}

/// A convenience type alias for `Result<T, BanyanRowSeqError>`.
pub type Result<T, E = BanyanRowSeqError> = std::result::Result<T, E>;
