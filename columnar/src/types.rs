use anyhow::{anyhow, Result};
use banyan::{
    index::{CompactSeq, Summarizable, VecSeq}, // Keep VecSeq for now
    TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    DagCbor,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Read, Seek, Write},
    ops::Range,
};
use thiserror::Error;
use tracing::error;

// --- Basic Data Types ---
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DagCbor)]
pub enum Value {
    Timestamp(i64),
    Integer(i64),
    Float(f64),
    String(String),
    Enum(u32),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Timestamp,
    Integer,
    Float,
    String,
    Enum(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataDefinition {
    pub columns: Vec<ColumnDefinition>,
    #[serde(skip)]
    name_to_index: BTreeMap<String, usize>,
}

impl DataDefinition {
    pub fn new(columns: Vec<ColumnDefinition>) -> Self {
        let name_to_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            columns,
            name_to_index,
        }
    }

    pub fn get_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).cloned()
    }

    pub fn get_col_def(&self, name: &str) -> Option<&ColumnDefinition> {
        self.get_index(name).map(|i| &self.columns[i])
    }

    // Helper to rebuild the index after deserialization
    pub fn rebuild_index(&mut self) {
        self.name_to_index = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
    }
}

pub type Record = BTreeMap<String, Option<Value>>;

// --- Banyan Tree Types Implementation ---

// Key: Represents a range within the logical stream
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
pub struct RichRangeKey {
    pub start_offset: u64,         // First absolute row index in this chunk
    pub count: u64,                // Number of rows logically covered by this chunk
    pub min_timestamp_micros: i64, // Min timestamp within this chunk
    pub max_timestamp_micros: i64, // Max timestamp within this chunk
}

// Summary: Aggregates information from underlying Keys or Summaries
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
pub struct RichRangeSummary {
    pub start_offset: u64,         // Minimum start_offset of all children
    pub total_count: u64,          // Total number of rows covered by children
    pub min_timestamp_micros: i64, // Minimum timestamp across all children
    pub max_timestamp_micros: i64, // Maximum timestamp across all children
}

// Wrapper for RoaringBitmap for CBOR/IPLD serialization
#[derive(Clone, Debug, PartialEq)] // Removed Serialize/Deserialize derive, use manual impls
pub struct CborRoaringBitmap {
    // Store the bitmap directly. Serialization handles bytes.
    bitmap: RoaringBitmap,
}

impl CborRoaringBitmap {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        // No need to pre-serialize bytes here
        Self { bitmap }
    }

    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    pub fn bitmap_mut(&mut self) -> &mut RoaringBitmap {
        // Note: If mutable access is provided, the user must be aware
        // that serialization captures the state *at the time of serialization*.
        &mut self.bitmap
    }

    // Added helper methods
    pub fn insert(&mut self, index: u32) {
        self.bitmap.insert(index);
    }

    pub fn contains(&self, index: u32) -> bool {
        self.bitmap.contains(index)
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

impl Default for CborRoaringBitmap {
    fn default() -> Self {
        Self::new()
    }
}

// Manual DagCbor implementation for CborRoaringBitmap
impl Encode<DagCborCodec> for CborRoaringBitmap {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> Result<()> {
        let mut bytes = Vec::new();
        // Serialize bitmap to bytes *during* the encode call
        self.bitmap
            .serialize_into(&mut bytes)
            .map_err(|e| anyhow!("Failed to serialize RoaringBitmap: {}", e))?;
        // Encode the bytes using DagCbor for byte arrays
        bytes.encode(c, w)
    }
}

impl Decode<DagCborCodec> for CborRoaringBitmap {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
        // Decode the DagCbor encoded byte array
        let bytes: Vec<u8> = Decode::decode(c, r)?;
        // Deserialize the RoaringBitmap from the bytes
        let bitmap = RoaringBitmap::deserialize_from(&bytes[..])
            .map_err(|e| anyhow!("Failed to deserialize RoaringBitmap: {}", e))?;
        Ok(Self { bitmap })
    }
}

// Value stored in Banyan leaves (Columnar Data Chunk)
#[derive(Clone, Debug, PartialEq, DagCbor)]
// #[ipld(repr = "kinded")]
pub enum ColumnChunk {
    Timestamp {
        present: CborRoaringBitmap, // Bitmap indicating non-null rows
        // #[ipld(repr = "value")]
        data: Vec<u8>, // Compressed i64 data (e.g., delta + zstd)
    },
    Integer {
        present: CborRoaringBitmap,
        // #[ipld(repr = "value")]
        data: Vec<u8>, // Compressed i64 data (e.g., delta + zstd)
    },
    Float {
        present: CborRoaringBitmap,
        // #[ipld(repr = "value")]
        data: Vec<u8>, // Compressed f64 data (e.g., Gorilla + zstd)
    },
    String {
        present: CborRoaringBitmap,
        // #[ipld(repr = "value")]
        data: Vec<u8>, // Compressed string data (e.g., dictionary + zstd)
    },
    Enum {
        present: CborRoaringBitmap,
        // #[ipld(repr = "value")]
        data: Vec<u8>, // Compressed u32 indices (e.g., varint + zstd)
    },
}

// Define the Banyan TreeTypes
#[derive(Clone, Debug)]
pub struct ColumnarTreeTypes;

impl TreeTypes for ColumnarTreeTypes {
    type Key = RichRangeKey;
    type Summary = RichRangeSummary;
    // Use VecSeq for simplicity. Requires Key/Summary to impl DagCbor.
    type KeySeq = VecSeq<Self::Key>;
    type SummarySeq = VecSeq<Self::Summary>;
    // Use Sha256Digest as Link type (convertible to/from Cid)
    type Link = Sha256Digest;
}

// --- Summarizable Implementations ---

// How to summarize a sequence of Keys (leaf data ranges) into a Summary
impl Summarizable<RichRangeSummary> for VecSeq<RichRangeKey> {
    fn summarize(&self) -> RichRangeSummary {
        if self.is_empty() {
            // Define an empty summary (important for Banyan)
            return RichRangeSummary {
                start_offset: 0, // Or perhaps u64::MAX to indicate emptiness? Check Banyan conventions. Using 0 for now.
                total_count: 0,
                min_timestamp_micros: i64::MAX, // Use MAX/MIN bounds for empty time range
                max_timestamp_micros: i64::MIN,
            };
        }

        // Assumes keys are ordered by start_offset, which Banyan ensures within a sequence.
        let first = self.first();
        let last = self.last(); // Need last key to calculate total count based on offsets
        let mut min_ts = first.min_timestamp_micros;
        let mut max_ts = first.max_timestamp_micros;
        let mut total_count = 0; // Calculate sum of counts

        for key in self.as_ref().iter() {
            total_count += key.count;
            min_ts = min_ts.min(key.min_timestamp_micros);
            max_ts = max_ts.max(key.max_timestamp_micros);
        }

        RichRangeSummary {
            start_offset: first.start_offset, // Min start offset
            total_count,                      // Sum of counts in the keys
            min_timestamp_micros: min_ts,
            max_timestamp_micros: max_ts,
        }
    }
}

// How to summarize a sequence of Summaries (branch node data) into a parent Summary
impl Summarizable<RichRangeSummary> for VecSeq<RichRangeSummary> {
    fn summarize(&self) -> RichRangeSummary {
        if self.is_empty() {
            return RichRangeSummary {
                start_offset: 0,
                total_count: 0,
                min_timestamp_micros: i64::MAX,
                max_timestamp_micros: i64::MIN,
            };
        }

        let first = self.first();
        let mut combined = RichRangeSummary {
            start_offset: first.start_offset, // Initialize with the first summary's start offset
            total_count: 0,
            min_timestamp_micros: first.min_timestamp_micros,
            max_timestamp_micros: first.max_timestamp_micros,
        };

        for child_summary in self.as_ref().iter() {
            combined.total_count += child_summary.total_count;
            combined.min_timestamp_micros = combined
                .min_timestamp_micros
                .min(child_summary.min_timestamp_micros);
            combined.max_timestamp_micros = combined
                .max_timestamp_micros
                .max(child_summary.max_timestamp_micros);
            // The start_offset of the parent is the minimum start_offset of its children
            combined.start_offset = combined.start_offset.min(child_summary.start_offset);
        }
        combined
    }
}

// --- Error Handling ---

#[derive(Error, Debug)]
pub enum ColumnarError {
    #[error("Column '{0}' not found in data definition")]
    ColumnNotFound(String),
    #[error("Data definition mismatch")]
    SchemaMismatch,
    #[error("Compression error: {0}")]
    CompressionError(String),
    #[error("Decompression error: {0}")]
    DecompressionError(String),
    #[error("Type error during compression/decompression: expected {expected}, got {actual}")]
    TypeError { expected: String, actual: String },
    #[error("Roaring Bitmap error: {0}")]
    RoaringError(#[from] std::io::Error), // Roaring uses std::io::Error
    // Wrap generic anyhow errors from Banyan or other sources
    #[error("Banyan/Storage error: {0}")]
    BanyanError(#[from] anyhow::Error),
    #[error("Iterator stopped unexpectedly for column {0}")]
    IteratorStopped(String),
    #[error(
        "Inconsistent chunk data: expected length {expected}, got {actual} for column {column}"
    )]
    InconsistentChunkLength {
        expected: usize,
        actual: usize,
        column: String,
    },
    #[error("Empty chunk data received from Banyan for column {column} at offset {offset}")]
    EmptyChunkData { column: String, offset: u64 },
    #[error("Inconsistent chunk range: Banyan range {banyan_range:?} does not match key range {key_range:?} for column {column}")]
    InconsistentChunkRange {
        banyan_range: Range<u64>,
        key_range: Range<u64>,
        column: String,
    },
    #[error("Deserialize into Cid error: {0}")]
    DeserializeCidError(#[from] libipld::cid::Error),
    #[error("Persistence error: {0}")]
    PersistenceError(String),
    #[error("Column '{0}' has no data (CID is None), cannot query.")]
    ColumnNotInitialized(String),
    #[error("Decompression mismatch: Bitmap indicates value at index {index}, but dense values iterator exhausted for column {column}.")]
    BitmapValueMismatch { index: u32, column: String },
    #[error("Decompression mismatch: Extra dense values found after processing bitmap for column {column}.")]
    ExtraDenseValues { column: String },
    #[error("Decompression consistency error: Bitmap index {index} out of bounds for expected chunk length {len} for column {column}.")]
    BitmapIndexOutOfBounds {
        index: u32,
        len: usize,
        column: String,
    },
    #[error("Reconstruction Error: Cache index {index} out of bounds for cached Vec length {len} for column {column}.")]
    CacheIndexOutOfBounds {
        index: u32,
        len: usize,
        column: String,
    },
    #[error("InconsistentChunkData: expected {expected}, got {actual} for column {column}.")]
    InconsistentChunkData {
        expected: usize,
        actual: usize,
        column: String,
    },
}
