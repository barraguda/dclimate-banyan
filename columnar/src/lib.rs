use anyhow::{anyhow, Result};
use banyan::{
    index::{CompactSeq, Summarizable, VecSeq}, // Keep VecSeq for now
    store::{BlockWriter, BranchCache, ReadOnlyStore}, // Added BlockWriter, ReadOnlyStore
    Config,
    FilteredChunk,
    Forest,
    Secrets,
    StreamBuilder,
    Transaction,
    Tree,
    TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    prelude::Codec,
    Cid, DagCbor,
};
use roaring::RoaringBitmap;
use serde::{de::Error as SerdeDeError, Deserialize, Deserializer, Serialize, Serializer}; // Alias Serde error
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Read, Seek, Write},
    iter::Peekable,
    marker::PhantomData,
    ops::{Bound, Range, RangeBounds},
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, error, info, trace, warn};

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
}

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
pub mod types {
    use super::*;

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
    #[ipld(repr = "kinded")]
    pub enum ColumnChunk {
        Timestamp {
            present: CborRoaringBitmap, // Bitmap indicating non-null rows
            #[ipld(repr = "value")]
            data: Vec<u8>, // Compressed i64 data (e.g., delta + zstd)
        },
        Integer {
            present: CborRoaringBitmap,
            #[ipld(repr = "value")]
            data: Vec<u8>, // Compressed i64 data (e.g., delta + zstd)
        },
        Float {
            present: CborRoaringBitmap,
            #[ipld(repr = "value")]
            data: Vec<u8>, // Compressed f64 data (e.g., Gorilla + zstd)
        },
        String {
            present: CborRoaringBitmap,
            #[ipld(repr = "value")]
            data: Vec<u8>, // Compressed string data (e.g., dictionary + zstd)
        },
        Enum {
            present: CborRoaringBitmap,
            #[ipld(repr = "value")]
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
}

// --- Compression Logic ---
// NOTE: These are still placeholder implementations using basic CBOR + Zstd.
// Replace with proper columnar compression algorithms (delta, dictionary, RLE, Gorilla, etc.)
// for performance and efficiency in a real library.
pub mod compression {
    use super::types::{CborRoaringBitmap, ColumnChunk};
    use super::*;
    use zstd::stream::{copy_encode, decode_all};

    // Placeholder: CBOR + Zstd for i64
    fn compress_i64_zstd(values: &[i64]) -> Result<Vec<u8>> {
        let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_bytes[..], &mut compressed, 3)?; // Level 3 compression
        Ok(compressed)
    }

    fn decompress_i64_zstd(data: &[u8]) -> Result<Vec<i64>> {
        let cbor_bytes = decode_all(data)?;
        let values: Vec<i64> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
        Ok(values)
    }

    // Placeholder: CBOR + Zstd for f64
    fn compress_f64_zstd(values: &[f64]) -> Result<Vec<u8>> {
        // DagCborCodec needs Vec<f64>, not &[f64] directly
        let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_f64_zstd(data: &[u8]) -> Result<Vec<f64>> {
        let cbor_bytes = decode_all(data)?;
        let values: Vec<f64> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
        Ok(values)
    }

    // Placeholder: CBOR + Zstd for String
    fn compress_string_zstd(values: &[String]) -> Result<Vec<u8>> {
        // DagCborCodec needs Vec<String>, not &[String] directly
        let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_string_zstd(data: &[u8]) -> Result<Vec<String>> {
        let cbor_bytes = decode_all(data)?;
        let values: Vec<String> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
        Ok(values)
    }

    // Placeholder: CBOR + Zstd for u32 (Enum indices)
    fn compress_enum_zstd(values: &[u32]) -> Result<Vec<u8>> {
        // DagCborCodec needs Vec<u32>, not &[u32] directly
        let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_enum_zstd(data: &[u8]) -> Result<Vec<u32>> {
        let cbor_bytes = decode_all(data)?;
        let values: Vec<u32> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
        Ok(values)
    }

    // Compresses a slice of Option<Value> for a given column type
    pub fn compress_column(
        values: &[Option<Value>],
        col_type: &ColumnType,
    ) -> Result<(CborRoaringBitmap, Vec<u8>)> {
        let mut present_bitmap = RoaringBitmap::new();
        let mut dense_values: Vec<Value> = Vec::with_capacity(values.len());

        for (i, val_opt) in values.iter().enumerate() {
            if let Some(val) = val_opt {
                present_bitmap.insert(i as u32);
                dense_values.push(val.clone()); // Clone only non-null values
            }
        }
        trace!(
            "Compressed column: bitmap len {}, dense values len {}",
            present_bitmap.len(),
            dense_values.len()
        );

        let compressed_data = match col_type {
            ColumnType::Timestamp => {
                let dense_i64s: Vec<i64> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::Timestamp(ts) => Ok(ts),
                        _ => Err(ColumnarError::TypeError {
                            expected: "Timestamp".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<_, _>>()?;
                // *** Replace with delta + zstd or similar ***
                compress_i64_zstd(&dense_i64s)?
            }
            ColumnType::Integer => {
                let dense_i64s: Vec<i64> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::Integer(i) => Ok(i),
                        _ => Err(ColumnarError::TypeError {
                            expected: "Integer".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<_, _>>()?;
                // *** Replace with delta + zstd or similar ***
                compress_i64_zstd(&dense_i64s)?
            }
            ColumnType::Float => {
                let dense_f64s: Vec<f64> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::Float(f) => Ok(f),
                        _ => Err(ColumnarError::TypeError {
                            expected: "Float".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<_, _>>()?;
                // *** Replace with Gorilla + zstd or similar ***
                compress_f64_zstd(&dense_f64s)?
            }
            ColumnType::String => {
                let dense_strings: Vec<String> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::String(s) => Ok(s),
                        _ => Err(ColumnarError::TypeError {
                            expected: "String".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<_, _>>()?;
                // *** Replace with dictionary + zstd or similar ***
                compress_string_zstd(&dense_strings)?
            }
            ColumnType::Enum(_) => {
                let dense_enums: Vec<u32> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::Enum(idx) => Ok(idx),
                        _ => Err(ColumnarError::TypeError {
                            expected: "Enum".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<_, _>>()?;
                // *** Replace with varint + zstd or similar ***
                compress_enum_zstd(&dense_enums)?
            }
        };

        // Wrap the bitmap here
        Ok((
            CborRoaringBitmap::from_bitmap(present_bitmap),
            compressed_data,
        ))
    }

    // Decompresses a ColumnChunk back into Vec<Option<Value>>
    pub fn decompress_column(chunk: &ColumnChunk) -> Result<Vec<Option<Value>>> {
        trace!("Decompressing column chunk: {:?}", chunk);
        let (present_cbor_bitmap, data, value_constructor): (
            &CborRoaringBitmap,
            &[u8],
            Box<dyn Fn(Value) -> Value>, // Placeholder type constructor
        ) = match chunk {
            ColumnChunk::Timestamp { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Timestamp
            ColumnChunk::Integer { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Integer
            ColumnChunk::Float { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Float
            ColumnChunk::String { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::String
            ColumnChunk::Enum { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Enum
        };

        let present_bitmap = present_cbor_bitmap.bitmap(); // Get the actual bitmap

        let dense_values: Vec<Value> = match chunk {
            ColumnChunk::Timestamp { .. } => decompress_i64_zstd(data)?
                .into_iter()
                .map(Value::Timestamp)
                .collect(),
            ColumnChunk::Integer { .. } => decompress_i64_zstd(data)?
                .into_iter()
                .map(Value::Integer)
                .collect(),
            ColumnChunk::Float { .. } => decompress_f64_zstd(data)?
                .into_iter()
                .map(Value::Float)
                .collect(),
            ColumnChunk::String { .. } => decompress_string_zstd(data)?
                .into_iter()
                .map(Value::String)
                .collect(),
            ColumnChunk::Enum { .. } => decompress_enum_zstd(data)?
                .into_iter()
                .map(Value::Enum)
                .collect(),
        };

        // Determine the logical size of the chunk (number of rows it represents)
        // This is crucial. The size isn't just the number of present values.
        // It should correspond to the `count` field in the `RichRangeKey` that
        // this chunk was stored with. Since we don't have the key here directly,
        // we rely on the bitmap's perspective. The highest set bit's index + 1
        // gives the minimum length needed to contain all present values.
        // If the bitmap is empty, the chunk logically represented 0 rows *that were present*.
        // A better approach might be to store the original `count` alongside the chunk data,
        // but for now, we use the bitmap max.
        let chunk_len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
        trace!(
            "  Bitmap max: {:?}, inferred chunk len: {}",
            present_bitmap.max(),
            chunk_len
        );

        let mut result = vec![None; chunk_len]; // Initialize with Nones
        let mut dense_iter = dense_values.into_iter();

        // Iterate through the bitmap's set bits (indices of present values)
        for present_index in present_bitmap.iter() {
            if (present_index as usize) < chunk_len {
                // Fetch the next corresponding value from the dense iterator
                if let Some(value) = dense_iter.next() {
                    // Apply the correct type constructor (though currently placeholders)
                    result[present_index as usize] = Some(value_constructor(value));
                } else {
                    // This indicates a mismatch: more bits set in bitmap than values found
                    error!("Decompression mismatch: Bitmap indicates value at index {}, but dense iterator exhausted.", present_index);
                    return Err(ColumnarError::DecompressionError(format!(
                        "Mismatch between bitmap presence (index {}) and decompressed value count.",
                        present_index
                    ))
                    .into());
                }
            } else {
                // This should ideally not happen if chunk_len is derived correctly
                error!("Decompression consistency error: Bitmap contains index {} which is >= inferred chunk length {}.", present_index, chunk_len);
                return Err(ColumnarError::DecompressionError(format!(
                    "Bitmap index {} out of bounds for inferred chunk length {}",
                    present_index, chunk_len
                ))
                .into());
            }
        }

        // After iterating through all present bits, the dense iterator should be empty
        if dense_iter.next().is_some() {
            error!("Decompression mismatch: Dense iterator not exhausted after processing all bitmap indices.");
            return Err(ColumnarError::DecompressionError(
                "Mismatch between bitmap count and decompressed value count (extra values found)"
                    .to_string(),
            )
            .into());
        }

        trace!("Decompressed into {} Option<Value>s", result.len());
        Ok(result)
    }
}

// --- Banyan Query Logic ---
pub mod query {
    use super::types::{ColumnarTreeTypes, RichRangeKey, RichRangeSummary};
    use super::*;
    use crate::Value;
    use banyan::index::{BranchIndex, LeafIndex};
    use banyan::query::Query;

    // Banyan Query struct - only contains offset and time ranges for Banyan tree traversal
    #[derive(Debug, Clone)]
    pub struct ColumnarBanyanQuery {
        pub offset_range: (Bound<u64>, Bound<u64>),
        pub time_range_micros: (Bound<i64>, Bound<i64>),
    }

    // Helper function to check if two potentially unbounded ranges intersect.
    // Ranges are inclusive start, exclusive end for comparison logic.
    fn ranges_intersect<T: PartialOrd>(
        r1_start: Bound<T>,
        r1_end: Bound<T>,
        r2_start: Bound<T>,
        r2_end: Bound<T>,
    ) -> bool {
        // Check if r1 is entirely before r2
        let r1_before_r2 = match (r1_end, r2_start) {
            (Bound::Included(e1), Bound::Included(s2)) => e1 < s2,
            (Bound::Excluded(e1), Bound::Included(s2)) => e1 <= s2, // if end is excluded, need <=
            (Bound::Included(e1), Bound::Excluded(s2)) => e1 <= s2, // if start is excluded, need <=
            (Bound::Excluded(e1), Bound::Excluded(s2)) => e1 <= s2,
            (_, Bound::Unbounded) => false, // r1 cannot be before unbounded start
            (Bound::Unbounded, _) => false, // unbounded end cannot be before anything
            _ => false,                     // Should handle all cases, but default false
        };

        // Check if r2 is entirely before r1
        let r2_before_r1 = match (r2_end, r1_start) {
            (Bound::Included(e2), Bound::Included(s1)) => e2 < s1,
            (Bound::Excluded(e2), Bound::Included(s1)) => e2 <= s1,
            (Bound::Included(e2), Bound::Excluded(s1)) => e2 <= s1,
            (Bound::Excluded(e2), Bound::Excluded(s1)) => e2 <= s1,
            (_, Bound::Unbounded) => false,
            (Bound::Unbounded, _) => false,
            _ => false,
        };

        // They intersect if neither is entirely before the other
        !r1_before_r2 && !r2_before_r1
    }

    impl Query<ColumnarTreeTypes> for ColumnarBanyanQuery {
        /// Determine which elements within a leaf *might* be relevant.
        /// This is called *after* intersecting for the branch containing the leaf.
        /// We simplify this: if the leaf's overall key range intersects the query,
        /// mark all elements provided in `res` as potentially relevant. The iterator
        /// will perform exact offset filtering later.
        fn containing(
            &self,
            _leaf_start_offset: u64,
            index: &LeafIndex<ColumnarTreeTypes>,
            res: &mut [bool],
        ) {
            // Check if the KeySeq is empty. This shouldn't happen for a valid leaf
            // node generated by our extend logic, but handle defensively.
            if index.keys.is_empty() {
                warn!("Query::containing called on leaf with empty keys sequence!");
                for r in res.iter_mut() {
                    *r = false;
                }
                return;
            }

            // Since keys is not empty, .first() is guaranteed to succeed and return RichRangeKey.
            // No `if let Some` needed here.
            let key: RichRangeKey = index.keys.first(); // Directly get the first key

            let key_offset_start = Bound::Included(key.start_offset);
            let key_offset_end = Bound::Excluded(key.start_offset.saturating_add(key.count));
            let key_time_start = Bound::Included(key.min_timestamp_micros);
            let key_time_end = Bound::Included(key.max_timestamp_micros);

            trace!(
                "Query::containing - Key: {:?}, Query Offset: {:?}, Query Time: {:?}",
                key,
                self.offset_range,
                self.time_range_micros
            );

            let offset_intersects = ranges_intersect(
                key_offset_start,
                key_offset_end,
                self.offset_range.0,
                self.offset_range.1,
            );
            let time_intersects = ranges_intersect(
                key_time_start,
                key_time_end,
                self.time_range_micros.0,
                self.time_range_micros.1,
            );

            if offset_intersects && time_intersects {
                trace!(
                    "  -> Key intersects query. Maintaining res state (len {}).",
                    res.len()
                );
                // If the key intersects, we don't need to change `res`. Elements marked true
                // by `intersecting` remain true. The iterator handles fine-grained filtering.
            } else {
                trace!(
                    "  -> Key does NOT intersect query. Marking res (len {}) as false.",
                    res.len()
                );
                // If the key doesn't intersect, prune all elements in this leaf represented by `res`.
                for r in res.iter_mut() {
                    *r = false;
                }
            }
        }

        /// Determine which child branches *might* contain relevant data based on their summaries.
        fn intersecting(
            &self,
            _offset: u64, // Base offset of the branch within the stream (unused)
            index: &BranchIndex<ColumnarTreeTypes>,
            res: &mut [bool], // Input/Output: indicates which children are currently considered relevant
        ) {
            for (i, summary) in index.summaries.as_ref().iter().enumerate() {
                // Only check summaries that are currently considered relevant
                if res[i] {
                    // Define the summary's ranges (inclusive start, exclusive end for offset)
                    let summary_offset_start = Bound::Included(summary.start_offset);
                    let summary_offset_end =
                        Bound::Excluded(summary.start_offset.saturating_add(summary.total_count));
                    let summary_time_start = Bound::Included(summary.min_timestamp_micros);
                    let summary_time_end = Bound::Included(summary.max_timestamp_micros); // Inclusive

                    // Check for intersection
                    let offset_intersects = ranges_intersect(
                        summary_offset_start,
                        summary_offset_end,
                        self.offset_range.0,
                        self.offset_range.1,
                    );
                    let time_intersects = ranges_intersect(
                        summary_time_start,
                        summary_time_end,
                        self.time_range_micros.0,
                        self.time_range_micros.1,
                    );

                    // Update res[i]: if it doesn't intersect, mark as false
                    if !(offset_intersects && time_intersects) {
                        res[i] = false;
                        trace!(
                            "Query::intersecting - Pruning child {} with summary {:?}",
                            i,
                            summary
                        );
                    } else {
                        trace!(
                            "Query::intersecting - Keeping child {} with summary {:?}",
                            i,
                            summary
                        );
                    }
                }
            }
        }
    }

    // --- Value Filters (Applied *after* Banyan retrieval) ---

    #[derive(Debug, Clone, PartialEq)] // Added PartialEq
    pub enum Comparison {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        // Add In, NotIn, Contains, etc. if needed
    }

    #[derive(Debug, Clone)]
    pub struct ValueFilter {
        pub column_name: String,
        pub operator: Comparison,
        pub value: Value, // The value to compare against
    }

    // Helper to apply filters to a reconstructed row map (String -> Option<Value>)
    pub fn apply_value_filters(
        row: &BTreeMap<String, Option<Value>>,
        filters: &[ValueFilter],
    ) -> bool {
        if filters.is_empty() {
            return true; // No filters means the row passes
        }
        // All filters must pass
        filters.iter().all(|filter| {
            match row.get(&filter.column_name) {
                Some(Some(row_value)) => {
                    // Value exists and is not None, perform comparison
                    compare_values(row_value, &filter.value, &filter.operator)
                }
                Some(None) => {
                    // Value exists but is None (NULL)
                    // How filters handle NULL is important. Often, comparisons with NULL yield false.
                    // Exception: IS NULL / IS NOT NULL filters (if added).
                    // Current behavior: NULL comparison fails unless it's `NotEquals NULL`? Let's make it always false for now.
                    // Or maybe Equals with Null? Let's define: comparison ops always false with NULL.
                    false // Filter fails if row value is NULL for standard comparisons
                }
                None => {
                    // Column not present in the (potentially partial) row map.
                    // This can happen if the column wasn't requested or during processing.
                    // Treat as filter failure.
                    error!(
                        "Column '{}' required by filter not found in row data: {:?}",
                        filter.column_name, row
                    );
                    false
                }
            }
        })
    }

    // Comparison logic (Handles type matching for basic numeric/timestamp comparisons)
    fn compare_values(left: &Value, right: &Value, op: &Comparison) -> bool {
        match op {
            Comparison::Equals => left == right, // Relies on Value::PartialEq
            Comparison::NotEquals => left != right, // Relies on Value::PartialEq
            Comparison::GreaterThan => match (left, right) {
                (Value::Integer(l), Value::Integer(r)) => l > r,
                (Value::Float(l), Value::Float(r)) => l > r,
                (Value::Timestamp(l), Value::Timestamp(r)) => l > r,
                // Attempt float/int comparison by promoting int to float
                (Value::Float(l), Value::Integer(r)) => *l > (*r as f64),
                (Value::Integer(l), Value::Float(r)) => (*l as f64) > *r,
                // Other type combinations are not comparable for GT
                _ => false,
            },
            Comparison::GreaterThanOrEqual => match (left, right) {
                (Value::Integer(l), Value::Integer(r)) => l >= r,
                (Value::Float(l), Value::Float(r)) => l >= r,
                (Value::Timestamp(l), Value::Timestamp(r)) => l >= r,
                (Value::Float(l), Value::Integer(r)) => *l >= (*r as f64),
                (Value::Integer(l), Value::Float(r)) => (*l as f64) >= *r,
                _ => false,
            },
            Comparison::LessThan => match (left, right) {
                (Value::Integer(l), Value::Integer(r)) => l < r,
                (Value::Float(l), Value::Float(r)) => l < r,
                (Value::Timestamp(l), Value::Timestamp(r)) => l < r,
                (Value::Float(l), Value::Integer(r)) => *l < (*r as f64),
                (Value::Integer(l), Value::Float(r)) => (*l as f64) < *r,
                _ => false,
            },
            Comparison::LessThanOrEqual => match (left, right) {
                (Value::Integer(l), Value::Integer(r)) => l <= r,
                (Value::Float(l), Value::Float(r)) => l <= r,
                (Value::Timestamp(l), Value::Timestamp(r)) => l <= r,
                (Value::Float(l), Value::Integer(r)) => *l <= (*r as f64),
                (Value::Integer(l), Value::Float(r)) => (*l as f64) <= *r,
                _ => false,
            },
        }
    }
}

// --- High-Level Manager ---
pub mod manager {
    use super::compression;
    use super::iterator::ColumnarResultIterator;
    use super::query::{ColumnarBanyanQuery, ValueFilter};
    use super::types::{ColumnChunk, ColumnarTreeTypes, RichRangeKey, RichRangeSummary}; // Use types module explicitly
    use super::*; // Import top-level things like DataDefinition, Record etc.
    use banyan::store::{BlockWriter, ReadOnlyStore};
    use banyan_utils::tags::Sha256Digest;
    use serde_json;
    use std::path::PathBuf; // Added PathBuf // For state persistence example

    // Define a concrete store trait alias for convenience
    pub trait ColumnarBanyanStore:
        ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
    {
    }
    impl<S> ColumnarBanyanStore for S where
        S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
    {
    }

    // --- Persistence Helper ---
    // Wrapper for Option<Sha256Digest> to handle CBOR/JSON serialization via CID string
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(transparent)] // Serialize/Deserialize as the inner type
    struct SerializableLinkOption(#[serde(with = "serde_link_option")] Option<Sha256Digest>);
    mod serde_link_option {
        use super::*;
        use serde::{Deserialize, Deserializer, Serializer};

        pub fn serialize<S>(
            link_opt: &Option<Sha256Digest>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match link_opt {
                Some(link) => {
                    let cid = Cid::from(*link);
                    serializer.serialize_some(&cid.to_string())
                }
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Sha256Digest>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt_s: Option<String> = Option::deserialize(deserializer)?;
            match opt_s {
                Some(s) => {
                    let cid = Cid::try_from(s).map_err(SerdeDeError::custom)?;
                    // Ensure the CID uses the expected hash algorithm (Sha256)
                    // TODO: FIX THIS
                    // if cid.hash().code() != multihash::Sha2_256.into() {
                    //     return Err(SerdeDeError::custom(format!(
                    //         "Invalid CID hash algorithm: expected Sha256, got {}",
                    //         cid.hash().code()
                    //     )));
                    // }
                    let digest = Sha256Digest::try_from(cid).map_err(SerdeDeError::custom)?;
                    Ok(Some(digest))
                }
                None => Ok(None),
            }
        }
    }

    // State to be persisted
    #[derive(Serialize, Deserialize)]
    struct ManagerState {
        // Store DataDefinition directly
        data_definition: DataDefinition,
        // Map column name to optional Link (CID)
        column_cids: BTreeMap<String, SerializableLinkOption>,
        total_rows: u64,
    }

    pub struct ColumnarDatastreamManager<S: ColumnarBanyanStore> {
        store: S,
        forest: Forest<ColumnarTreeTypes, S>,
        pub data_definition: Arc<DataDefinition>, // Keep Arc for internal sharing
        // Builders are transient, recreated on load or extension
        // TODO: #[serde(skip)]
        // column_builders: BTreeMap<String, StreamBuilder<ColumnarTreeTypes, ColumnChunk>>,
        // CIDs are the persistent state of the trees
        pub column_cids: BTreeMap<String, Option<Sha256Digest>>,
        pub total_rows: u64,
        config: Config,
        secrets: Secrets,
        // Optional path for saving/loading state file
        persistence_path: Option<PathBuf>,
    }

    impl<S: ColumnarBanyanStore> ColumnarDatastreamManager<S> {
        /// Creates a new, empty datastream manager.
        pub fn new(
            store: S,
            data_definition: DataDefinition,
            config: Config,
            secrets: Secrets,
            persistence_path: Option<&Path>,
        ) -> Result<Self> {
            let forest = Forest::new(store.clone(), BranchCache::default());
            let data_definition_arc = Arc::new(data_definition);
            let column_names = data_definition_arc
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();

            let mut column_cids = BTreeMap::new();
            // Initialize None CIDs for all defined columns
            for name in column_names {
                column_cids.insert(name, None);
            }

            info!("Created new ColumnarDatastreamManager");
            Ok(Self {
                store,
                forest,
                data_definition: data_definition_arc,
                column_cids,
                total_rows: 0,
                config,
                secrets,
                persistence_path: persistence_path.map(|p| p.to_path_buf()),
            })
        }

        /// Loads manager state from the persistence path, if configured.
        pub fn load_or_initialize(
            store: S,
            config: Config,
            secrets: Secrets,
            persistence_path: Option<&Path>,
            // If path doesn't exist or load fails, use this definition
            default_data_definition: Option<DataDefinition>,
        ) -> Result<Self> {
            let path_buf = persistence_path.map(|p| p.to_path_buf());

            if let Some(path) = &path_buf {
                info!("Attempting to load manager state from: {:?}", path);
                if path.exists() {
                    match Self::load_from_path(store.clone(), config.clone(), secrets.clone(), path)
                    {
                        Ok(manager) => {
                            info!("Successfully loaded manager state.");
                            return Ok(manager);
                        }
                        Err(e) => {
                            warn!("Failed to load state from {:?}: {}. Proceeding with initialization.", path, e);
                            // Fall through to initialization
                        }
                    }
                } else {
                    info!(
                        "Persistence file not found at {:?}. Initializing new manager.",
                        path
                    );
                }
            } else {
                info!("No persistence path provided. Initializing new manager.");
            }

            // Initialization path
            match default_data_definition {
                 Some(def) => Self::new(store, def, config, secrets, persistence_path),
                 None => Err(ColumnarError::PersistenceError(
                     "Persistence file not found or failed to load, and no default data definition provided.".to_string()
                 ).into())
             }
        }

        /// Internal load function
        fn load_from_path(store: S, config: Config, secrets: Secrets, path: &Path) -> Result<Self> {
            let file = std::fs::File::open(path).map_err(|e| {
                ColumnarError::PersistenceError(format!(
                    "Failed to open state file {:?}: {}",
                    path, e
                ))
            })?;
            let mut state: ManagerState = serde_json::from_reader(file).map_err(|e| {
                ColumnarError::PersistenceError(format!(
                    "Failed to deserialize state from {:?}: {}",
                    path, e
                ))
            })?;

            // Rebuild the DataDefinition index as it's skipped during serialization
            state.data_definition.rebuild_index();

            let data_definition_arc = Arc::new(state.data_definition);

            // Convert SerializableLinkOption back to Option<Sha256Digest>
            let column_cids: BTreeMap<String, Option<Sha256Digest>> = state
                .column_cids
                .into_iter()
                .map(|(name, serializable_link_opt)| (name, serializable_link_opt.0))
                .collect();

            let forest = Forest::new(store.clone(), BranchCache::default());

            debug!(
                "Loaded state: total_rows={}, columns={:?}",
                state.total_rows,
                column_cids.keys().collect::<Vec<_>>()
            );

            Ok(Self {
                store,
                forest,
                data_definition: data_definition_arc,
                column_cids,
                total_rows: state.total_rows,
                config,
                secrets,
                persistence_path: Some(path.to_path_buf()),
            })
        }

        /// Saves the current state (CIDs, total rows, definition) to the persistence path.
        pub fn save_state(&self) -> Result<()> {
            if let Some(path) = &self.persistence_path {
                info!("Saving manager state to: {:?}", path);

                // Ensure builders are flushed (optional, extend should handle it)
                // Might want to force pack here if builders aren't always packed in extend
                // self.flush_builders()?; // Example: requires implementing flush_builders

                // Convert CIDs to serializable format
                let serializable_cids = self
                    .column_cids
                    .iter()
                    .map(|(name, link_opt)| (name.clone(), SerializableLinkOption(*link_opt)))
                    .collect();

                let state = ManagerState {
                    // Clone Arc inner value for serialization
                    data_definition: (*self.data_definition).clone(),
                    column_cids: serializable_cids,
                    total_rows: self.total_rows,
                };

                let file = std::fs::File::create(path).map_err(|e| {
                    ColumnarError::PersistenceError(format!(
                        "Failed to create state file {:?}: {}",
                        path, e
                    ))
                })?;
                serde_json::to_writer_pretty(file, &state).map_err(|e| {
                    ColumnarError::PersistenceError(format!(
                        "Failed to serialize state to {:?}: {}",
                        path, e
                    ))
                })?;

                debug!(
                    "Saved state: total_rows={}, columns={:?}",
                    state.total_rows,
                    state.column_cids.keys().collect::<Vec<_>>()
                );
                Ok(())
            } else {
                // No persistence path configured, nothing to save.
                warn!("Attempted to save state, but no persistence path is configured.");
                Ok(()) // Or return an error if saving is mandatory? Ok for now.
            }
        }

        /// Appends new records to the columnar datastream.
        pub fn extend(&mut self, records: &[Record]) -> Result<()> {
            if records.is_empty() {
                debug!("Extend called with empty records, doing nothing.");
                return Ok(());
            }

            let count = records.len() as u64;
            let start_offset = self.total_rows;
            info!(
                "Extending datastream with {} records, starting at offset {}",
                count, start_offset
            );

            // --- Prepare Key --- (Same as before)
            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;
            let ts_col_name = "timestamp";
            for record in records {
                if let Some(Some(Value::Timestamp(ts))) = record.get(ts_col_name) {
                    min_ts = min_ts.min(*ts);
                    max_ts = max_ts.max(*ts);
                }
            }
            if min_ts == i64::MAX {
                warn!("No '{}' column found or all values were null in the batch. Using default timestamps (0) for key range.", ts_col_name);
                min_ts = 0;
                max_ts = 0;
            }
            let key = RichRangeKey {
                start_offset,
                count,
                min_timestamp_micros: min_ts,
                max_timestamp_micros: max_ts,
            };
            debug!("Created key for batch: {:?}", key);

            // --- Banyan Transaction ---
            // Create a transaction context holding the store for reading and writing.
            let mut txn = Transaction::new(self.forest.clone(), self.store.clone());
            // Store the CIDs resulting from this operation temporarily.
            let mut latest_cids_this_batch = BTreeMap::new();

            for col_def in &self.data_definition.columns {
                let column_name = &col_def.name;
                trace!("Processing column: {}", column_name);

                // --- Prepare Column Chunk --- (Same as before)
                let column_values: Vec<Option<Value>> = records
                    .iter()
                    .map(|record| record.get(column_name).cloned().flatten())
                    .collect();
                let (bitmap, compressed_data) =
                    compression::compress_column(&column_values, &col_def.column_type)?;
                trace!(
                    "  Compressed data size: {}, bitmap len: {}",
                    compressed_data.len(),
                    bitmap.len()
                );

                // --- Explicit chunk creation with tracing ---
                let chunk: ColumnChunk;
                match col_def.column_type {
                    ColumnType::Timestamp => {
                        trace!("  Creating Timestamp chunk");
                        chunk = ColumnChunk::Timestamp {
                            present: bitmap,
                            data: compressed_data,
                        };
                    }
                    ColumnType::Integer => {
                        trace!("  Creating Integer chunk");
                        chunk = ColumnChunk::Integer {
                            present: bitmap,
                            data: compressed_data,
                        };
                    }
                    ColumnType::Float => {
                        trace!("  Creating Float chunk"); // <<< Check this log for humidity/temp
                        chunk = ColumnChunk::Float {
                            present: bitmap,
                            data: compressed_data,
                        };
                    }
                    ColumnType::String => {
                        trace!("  Creating String chunk");
                        chunk = ColumnChunk::String {
                            present: bitmap,
                            data: compressed_data,
                        };
                    }
                    ColumnType::Enum(_) => {
                        trace!("  Creating Enum chunk");
                        chunk = ColumnChunk::Enum {
                            present: bitmap,
                            data: compressed_data,
                        };
                    }
                };

                // Add trace to see the created chunk variant BEFORE passing to banyan
                // Use a simple format that clearly shows the variant name
                let chunk_variant_name = match &chunk {
                    ColumnChunk::Timestamp { .. } => "Timestamp",
                    ColumnChunk::Integer { .. } => "Integer",
                    ColumnChunk::Float { .. } => "Float",
                    ColumnChunk::String { .. } => "String",
                    ColumnChunk::Enum { .. } => "Enum",
                };
                trace!(
                    "  Created chunk variant (before extend): {}",
                    chunk_variant_name
                );

                // --- Load or Create Builder within Transaction ---
                // Get the *current* CID for this column before modification.
                let current_cid = self.column_cids.get(column_name).cloned().flatten();
                trace!(
                    "  Loading builder for column '{}', current CID: {:?}",
                    column_name,
                    current_cid
                );

                // Load the existing stream builder state *into the transaction* or create a new one.
                let mut builder = match current_cid {
                    Some(cid) => {
                        txn.load_stream_builder(self.secrets.clone(), self.config.clone(), cid)?
                    }
                    None => StreamBuilder::new(self.config.clone(), self.secrets.clone()),
                };

                // --- Extend Builder ---
                // This modifies the builder *in place* and uses the transaction's writer (`txn.writer`)
                // to store the new blocks generated during this operation.
                txn.extend_unpacked(&mut builder, vec![(key.clone(), chunk)])?;
                trace!("  Extended builder in transaction.");

                // --- Get Updated Link ---
                // *After* extend_unpacked, the builder reflects the new state. Get its link (CID).
                let new_cid = builder.link();
                trace!(
                    "  New CID for column '{}' after extend: {:?}",
                    column_name,
                    new_cid
                );
                // Store this new CID temporarily. We update the main manager state *after* the transaction scope.
                latest_cids_this_batch.insert(column_name.clone(), new_cid);

                // No need to store the builder itself back into the manager state.
                // Its state is captured by the new_cid.
            } // End column loop

            // --- Finalize Transaction (Implicit) ---
            // The transaction (`txn`) now goes out of scope. When it's dropped,
            // nothing special happens to the transaction object itself. The key is that
            // all write operations (`extend_unpacked` -> `builder` -> `writer`) have already
            // occurred using the `store` (`self.store`) instance held by the transaction's writer component.
            // If the store needs explicit flushing, that would be handled by the store implementation
            // itself (e.g., if `S` had a `flush` method, we might call `self.store.flush()`).
            // For MemStore, writes are immediate. For other stores, we rely on their behavior.
            info!("Transaction scope ended. Writes assumed complete via transaction's writer.");
            // Drop `txn` explicitly for clarity if desired, though scope drop is sufficient.
            drop(txn);

            // --- Update Manager State ---
            // Now that the transaction is conceptually finished and writes are done,
            // update the manager's CIDs with the latest ones obtained *during* the transaction.
            for (col_name, new_cid) in latest_cids_this_batch {
                if new_cid.is_some()
                    || self
                        .column_cids
                        .get(&col_name)
                        .map_or(false, |opt| opt.is_some())
                {
                    // Update if the new CID is Some, OR if the old CID was Some (to correctly reflect becoming None)
                    trace!(
                        "  Updating manager CID for column '{}' to {:?}",
                        col_name,
                        new_cid
                    );
                    self.column_cids.insert(col_name.clone(), new_cid);
                } else {
                    // Old CID was None and new CID is None, no change needed.
                    trace!(
                        "  No CID update needed for column '{}' (remains None).",
                        col_name
                    );
                }
            }

            // Update total rows
            self.total_rows += count;
            debug!("Updated total_rows to: {}", self.total_rows);

            // --- Persist State ---
            if let Err(e) = self.save_state() {
                error!("Failed to save manager state after extend: {}", e);
                // Decide handling: hard error or warning?
            }

            Ok(())
        }

        /// Queries the columnar datastream.
        pub fn query(
            &self,
            requested_columns: Vec<String>,
            offset_range: (Bound<u64>, Bound<u64>),
            filters: Vec<ValueFilter>, // Value-based filters (applied post-retrieval)
            time_range_micros: (Bound<i64>, Bound<i64>), // Time-based filter (used for Banyan pruning)
        ) -> Result<ColumnarResultIterator<S>> {
            info!(
                "Initiating query: requested_cols={:?}, offset={:?}, time={:?}, filters={}",
                requested_columns,
                offset_range,
                time_range_micros,
                filters.len()
            );

            // Determine all columns needed: requested + those used in filters
            let mut needed_columns = requested_columns.clone();
            for filter in &filters {
                if !needed_columns.contains(&filter.column_name) {
                    if self
                        .data_definition
                        .get_col_def(&filter.column_name)
                        .is_none()
                    {
                        error!(
                            "Filter references non-existent column: {}",
                            filter.column_name
                        );
                        return Err(super::ColumnarError::ColumnNotFound(
                            filter.column_name.clone(),
                        )
                        .into()); // Qualify
                    }
                    needed_columns.push(filter.column_name.clone());
                }
            }
            needed_columns.sort();
            needed_columns.dedup();
            debug!("Query needs columns: {:?}", needed_columns);

            let banyan_query = super::query::ColumnarBanyanQuery {
                // Qualify
                offset_range: offset_range.clone(),
                time_range_micros: time_range_micros.clone(), // Clone time range for banyan query
            };

            let mut column_chunk_iters = BTreeMap::new();
            let txn = Transaction::new(self.forest.clone(), self.store.clone());

            for col_name in &needed_columns {
                trace!("Setting up iterator for column: {}", col_name);
                let cid = self.column_cids
                   .get(col_name)
                   .ok_or_else(|| {
                       error!("Internal state error: CID mapping missing for column defined in schema: {}", col_name);
                        super::ColumnarError::ColumnNotFound(col_name.clone()) // Qualify
                   })?
                   .ok_or_else(|| {
                       warn!("Query involves column '{}' which has no data yet (CID is None).", col_name);
                        super::ColumnarError::ColumnNotInitialized(col_name.clone()) // Qualify
                   })?;

                trace!("  Loading tree for CID: {}", cid);
                // Qualify types::* and ColumnChunk
                let tree: Tree<super::types::ColumnarTreeTypes, super::types::ColumnChunk> =
                    txn.load_tree(self.secrets.clone(), cid)?;

                let iter = self
                    .forest
                    .iter_filtered_chunked(&tree, banyan_query.clone(), &|_| ());

                column_chunk_iters.insert(col_name.clone(), iter.peekable());
            }

            // Determine the absolute start and end offsets for the Row Iterator
            let query_start_offset = match offset_range.0 {
                Bound::Included(s) => s,
                Bound::Excluded(s) => s.saturating_add(1),
                Bound::Unbounded => 0,
            };
            let query_end_offset = match offset_range.1 {
                Bound::Included(e) => e.saturating_add(1), // Iterator end is exclusive
                Bound::Excluded(e) => e,
                Bound::Unbounded => self.total_rows, // Query up to the current known total rows
            };

            debug!(
                "Creating ColumnarResultIterator: start_offset={}, end_offset={}",
                query_start_offset, query_end_offset
            );

            // Create and return the result iterator
            ColumnarResultIterator::new(
                self.data_definition.clone(),
                needed_columns,
                requested_columns,
                filters,
                time_range_micros,
                column_chunk_iters,
                query_start_offset,
                query_end_offset,
            )
        }
    }
}

// --- Result Iterator ---
pub mod iterator {
    use super::compression;
    use super::query::{apply_value_filters, ValueFilter};
    use super::types::{ColumnChunk, RichRangeKey};
    use super::*;
    use banyan::FilteredChunk; // Correct import

    // Stores the currently loaded and decompressed chunk data for one column
    #[derive(Debug, Clone)]
    struct DecompressedColumnData {
        bitmap: RoaringBitmap,           // Use RoaringBitmap directly
        values: Arc<Vec<Option<Value>>>, // Arc avoids cloning Vec for every row
        chunk_key: RichRangeKey,         // Keep key for range info
    }

    // Helper trait to box iterators generically
    trait BoxedIteratorExt: Iterator + Sized + Send + 'static {
        fn boxed(self) -> Box<dyn Iterator<Item = Self::Item> + Send> {
            Box::new(self)
        }
    }
    impl<I: Iterator + Sized + Send + 'static> BoxedIteratorExt for I {}

    /// Iterator that yields rows by fetching, decompressing, and aligning chunks
    /// from multiple column Banyan trees.
    pub struct ColumnarResultIterator<S: manager::ColumnarBanyanStore> {
        data_definition: Arc<DataDefinition>,
        needed_columns: Vec<String>, // Columns needed for result + filtering
        requested_columns: Vec<String>, // Columns to include in the final output map
        filters: Vec<ValueFilter>,   // Value filters applied after row reconstruction
        query_time_range_micros: (Bound<i64>, Bound<i64>), // Time range for Banyan pruning
        // Input iterators producing compressed chunks for each needed column
        // Banyan's iter_filtered_chunked yields Result<FilteredChunk<(u64, K, V), MappedData = ()>>
        compressed_chunk_iters: BTreeMap<
            String,
            Peekable<
                Box<
                    dyn Iterator<
                            Item = Result<
                                FilteredChunk<(u64, types::RichRangeKey, types::ColumnChunk), ()>,
                            >,
                        > + Send,
                >,
            >,
        >,

        // State for the current row being processed
        current_absolute_offset: u64, // The next row offset to yield
        query_end_offset: u64,        // The exclusive end offset for the query

        // Cache for the currently loaded and decompressed chunk data for each needed column
        // Keyed by column name.
        current_decompressed_chunk_cache: BTreeMap<String, DecompressedColumnData>,
        // Range covered by the *currently cached* and aligned chunks
        current_chunk_range: Range<u64>,
        _store_phantom: PhantomData<S>, // Keep track of store type if needed
    }

    impl<S: manager::ColumnarBanyanStore> ColumnarResultIterator<S> {
        #[allow(clippy::too_many_arguments)] // Necessary arguments for initialization
        pub(super) fn new(
            data_definition: Arc<DataDefinition>,
            needed_columns: Vec<String>,
            requested_columns: Vec<String>,
            filters: Vec<ValueFilter>,
            query_time_range_micros: (Bound<i64>, Bound<i64>),
            compressed_chunk_iters: BTreeMap<
                String,
                Peekable<
                    impl Iterator<
                            Item = Result<
                                FilteredChunk<(u64, types::RichRangeKey, types::ColumnChunk), ()>,
                            >,
                        > + Send
                        + 'static, // Ensure the iterator is Send + 'static
                >,
            >,
            query_start_offset: u64,
            query_end_offset: u64,
        ) -> Result<Self> {
            // Box the iterators for dynamic dispatch storage in the struct
            let boxed_iters = compressed_chunk_iters
                .into_iter()
                .map(|(k, v)| (k, v.boxed().peekable())) // Assuming boxed() helper is defined
                .collect();

            Ok(Self {
                data_definition,
                needed_columns,
                requested_columns,
                filters,
                query_time_range_micros,
                compressed_chunk_iters: boxed_iters,
                current_absolute_offset: query_start_offset,
                query_end_offset,
                current_decompressed_chunk_cache: BTreeMap::new(),
                current_chunk_range: 0..0,
                _store_phantom: PhantomData,
            })
        }

        /// Loads and decompresses the next set of aligned chunks covering the `target_offset`.
        /// Returns `Ok(true)` if a chunk was loaded, `Ok(false)` if the target offset cannot be reached
        /// (end of iteration or skipped gap), or `Err` on failure.
        /// Updates `self.current_absolute_offset` if a gap is skipped.
        fn load_next_chunk(&mut self, target_offset: u64) -> Result<bool> {
            debug!(
                "load_next_chunk(target_offset={}) - Current state: offset={}, cache_range={:?}",
                target_offset, self.current_absolute_offset, self.current_chunk_range
            );

            self.current_decompressed_chunk_cache.clear(); // Clear previous chunk cache
            let mut anchor_range: Option<Range<u64>> = None; // Range determined by the first covering chunk found
            let mut anchor_key: Option<RichRangeKey> = None; // Key of the anchor chunk
                                                             // Track the start of the *next* available chunk across all columns, if no anchor is found
            let mut next_candidate_start = u64::MAX;

            // --- Phase 1: Probe Iterators ---
            // Peek each column's iterator to find the chunk covering target_offset
            // or the next available chunk if target_offset falls in a gap.
            trace!(" -> Phase 1: Probing columns...");
            for col_name in &self.needed_columns {
                let iter = self
                    .compressed_chunk_iters
                    .get_mut(col_name)
                    .ok_or_else(|| ColumnarError::ColumnNotFound(col_name.clone()))?; // Should not happen

                trace!("  --> Probing column: {}", col_name);

                // Advance iterator past chunks that end *before* the target offset
                loop {
                    match iter.peek() {
                        Some(Ok(peeked_filtered_chunk)) => {
                            // Banyan yields FilteredChunk. We need the *key* from its data.
                            // If data is empty, the chunk was fully pruned by Banyan's query.containing.
                            if peeked_filtered_chunk.data.is_empty() {
                                trace!("      Skipping empty FilteredChunk (range {:?}) during pre-skip", peeked_filtered_chunk.range);
                                iter.next(); // Consume the empty chunk
                                continue;
                            }

                            // Extract the key from the first element in the data vec
                            // We assume our `extend` logic puts only one key/value per Banyan leaf.
                            let key = &peeked_filtered_chunk.data[0].1; // Peeked key: (offset, key, value).1
                            let chunk_range = key.start_offset..(key.start_offset + key.count);

                            if chunk_range.end <= target_offset {
                                trace!(
                                    "      Skipping chunk range {:?} (ends before target {})",
                                    chunk_range,
                                    target_offset
                                );
                                iter.next(); // Consume this chunk
                            } else {
                                // Found a chunk that potentially covers or starts after the target
                                trace!(
                                    "      Found candidate chunk range {:?} with key {:?}",
                                    chunk_range,
                                    key
                                );
                                break;
                            }
                        }
                        Some(Err(_)) => {
                            // Consume the error and propagate it
                            error!(
                                "      Error peeking iterator during pre-skip for column {}",
                                col_name
                            );
                            return Err(iter.next().unwrap().err().unwrap().into());
                            // Convert banyan::Error to anyhow::Error
                        }
                        None => {
                            // Iterator for this column is exhausted
                            trace!(
                                "      Iterator exhausted during pre-skip for column {}",
                                col_name
                            );
                            break; // Stop trying to skip for this column
                        }
                    }
                } // End skip loop

                // Now, examine the chunk at the current peek position
                match iter.peek() {
                    Some(Ok(chunk)) => {
                        if chunk.data.is_empty() {
                            trace!("      Peeked chunk is empty FilteredChunk (range {:?}), ignoring for anchoring.", chunk.range);
                            // This column doesn't have data for this range according to the query.
                            // It might become the `next_candidate_start` if its range.start is relevant.
                            next_candidate_start = next_candidate_start.min(chunk.range.start);
                            continue; // Move to the next column's probe
                        }

                        let key = chunk.data[0].1.clone(); // Clone key from peek
                        let chunk_range = key.start_offset..(key.start_offset + key.count);
                        trace!(
                            "      Candidate chunk range {:?} with key {:?}",
                            chunk_range,
                            key
                        );

                        // Update the earliest start offset found among all columns
                        next_candidate_start = next_candidate_start.min(chunk_range.start);
                        trace!(
                            "         (next_candidate_start updated to {})",
                            next_candidate_start
                        );

                        // Does this chunk cover the target offset?
                        if chunk_range.contains(&target_offset) {
                            trace!("      -> Chunk covers target_offset {}", target_offset);
                            if anchor_range.is_none() {
                                // This is the first column found covering the target. Set anchor.
                                trace!(
                                    "         Setting anchor_range={:?}, anchor_key={:?}",
                                    chunk_range,
                                    key
                                );
                                anchor_range = Some(chunk_range);
                                anchor_key = Some(key);
                            } else {
                                // Another column also covers the target. Verify consistency.
                                if Some(&chunk_range) != anchor_range.as_ref()
                                    || Some(&key) != anchor_key.as_ref()
                                {
                                    // Inconsistency detected! Chunks covering the same target offset
                                    // should have originated from the same `extend` batch and thus
                                    // have the identical RichRangeKey.
                                    error!(
                                         "Inconsistent chunk keys/ranges found during probe! Column '{}' has {:?}/{:?}, but anchor is {:?}/{:?}. This indicates a data corruption or logic error in 'extend'.",
                                         col_name, chunk_range, key, anchor_range, anchor_key
                                     );
                                    // We cannot proceed reliably. Advancing might hide the issue. Error out.
                                    return Err(ColumnarError::InconsistentChunkRange {
                                        banyan_range: chunk_range, // Placeholder, banyan range might differ slightly
                                        key_range: key.start_offset..(key.start_offset + key.count),
                                        column: col_name.clone(),
                                    }
                                    .into());
                                    // // Alternative: Try to recover by advancing past the inconsistent anchor
                                    // let advance_to = anchor_range.as_ref().unwrap().end;
                                    // warn!("Advancing offset to {} due to inconsistency.", advance_to);
                                    // self.current_absolute_offset = advance_to;
                                    // return Ok(false);
                                } else {
                                    trace!("         Chunk matches existing anchor. OK.");
                                }
                            }
                        } else {
                            // Chunk starts *after* the target offset
                            trace!(
                                "      -> Chunk range {:?} starts after target_offset {}",
                                chunk_range,
                                target_offset
                            );
                        }
                    }
                    Some(Err(_)) => {
                        error!("      Error peeking iterator for column {}", col_name);
                        return Err(iter.next().unwrap().err().unwrap().into());
                    }
                    None => {
                        // Iterator exhausted for this column
                        trace!("      Iterator exhausted for column {}", col_name);
                        // If we needed this column and it's exhausted before finding the target,
                        // we might not be able to satisfy the query further. Handled below.
                    }
                } // End match iter.peek() after skip
            } // End Phase 1 Probe Loop
            trace!(" -> Phase 1 Complete.");

            // --- Check Probe Results & Determine Action ---
            let (expected_range, expected_key) = match (anchor_range, anchor_key) {
                (Some(r), Some(k)) => {
                    // Found a consistent anchor covering the target offset. Proceed to load.
                    trace!(
                        " -> Found anchor covering target {}: range={:?}, key={:?}",
                        target_offset,
                        r,
                        k
                    );
                    (r, k) // These are the range and key we expect *all* needed columns to have
                }
                (None, None) => {
                    // No chunk covering the target offset was found in *any* column.
                    // This means the target offset falls into a gap or is beyond the end of data.
                    // Advance the iterator's current offset to the start of the next available chunk,
                    // or to the query end if no more chunks exist.
                    let advance_to = if next_candidate_start > target_offset
                        && next_candidate_start != u64::MAX
                    {
                        // Found chunks starting *after* the target offset. Jump to the earliest one.
                        next_candidate_start
                    } else {
                        // No chunks found at or after the target offset in any column. We're done.
                        self.query_end_offset
                    };
                    trace!(
                         " -> No anchor found covering target {}. Advancing iterator offset to {} (next_candidate_start={}, query_end={}).",
                         target_offset, advance_to, next_candidate_start, self.query_end_offset
                     );
                    self.current_absolute_offset = advance_to;
                    return Ok(false); // Indicate no chunk loaded, iterator offset advanced
                }
                _ => unreachable!(
                    "Logic error: anchor_range and anchor_key should be Some or None together"
                ),
            };

            // --- Phase 2: Verify and Load All Columns ---
            // We have an expected key/range. Verify all needed columns have this exact chunk
            // and load/decompress their data.
            trace!(
                " -> Phase 2: Verifying and loading range {:?} with key {:?}",
                expected_range,
                expected_key
            );
            let mut loaded_data_cache = BTreeMap::new(); // Build the new cache
            for col_name in &self.needed_columns {
                trace!("  --> Verifying/Loading column: {}", col_name);
                let iter = self.compressed_chunk_iters.get_mut(col_name).unwrap(); // Should exist

                match iter.peek() {
                    Some(Ok(chunk)) => {
                        if chunk.data.is_empty() {
                            // This column's chunk was pruned by Banyan query, but others covering
                            // the same range were not. This is a valid scenario.
                            // We treat this column as having all NULLs for this chunk range.
                            trace!("      Column '{}' chunk is empty FilteredChunk (range {:?}). Treating as NULLs.", col_name, chunk.range);

                            // Consume the empty chunk from the iterator
                            iter.next();

                            // Create a dummy entry for the cache representing all NULLs
                            let null_bitmap = RoaringBitmap::new(); // Empty bitmap
                            let null_values = Arc::new(vec![None; expected_key.count as usize]); // Vec of Nones

                            loaded_data_cache.insert(
                                col_name.clone(),
                                DecompressedColumnData {
                                    bitmap: null_bitmap,
                                    values: null_values,
                                    // Use the *expected* key for range consistency
                                    chunk_key: expected_key.clone(),
                                },
                            );
                            continue; // Move to the next column
                        }

                        // Chunk has data, verify key and range match the anchor
                        let key = &chunk.data[0].1; // Key from peeked data
                        let chunk_range = key.start_offset..(key.start_offset + key.count);

                        if chunk_range == expected_range && *key == expected_key {
                            // Match! Consume the chunk, decompress, and add to cache.
                            trace!("      OK: Chunk matches expected range and key. Consuming and decompressing.");
                            // Consume the FilteredChunk Result
                            let owned_filtered_chunk = iter.next().unwrap().unwrap(); // Unwrap Results, safe due to peek

                            // Extract the ColumnChunk value. We assume only one entry in `data`.
                            let (_, _rich_key_ignored, col_chunk) = match owned_filtered_chunk
                                .data
                                .into_iter()
                                .next()
                            {
                                Some((_, rk, cc)) => (0, rk, cc), // Offset is 0 relative to chunk start
                                None => {
                                    // Should be caught by the is_empty check earlier, but belt-and-suspenders
                                    error!("      Internal Error: Non-empty FilteredChunk yielded empty data vector for column '{}'.", col_name);
                                    return Err(ColumnarError::EmptyChunkData {
                                        column: col_name.clone(),
                                        offset: target_offset, // Offset where the problem occurred
                                    }
                                    .into());
                                }
                            };

                            trace!("      Decompressing column data...");
                            let decompressed_values =
                                Arc::new(compression::decompress_column(&col_chunk)?);
                            let decompressed_len = decompressed_values.len();
                            let expected_len = expected_key.count as usize; // Logical size from key

                            // The decompressed vector length MUST match the logical count from the key
                            if decompressed_len != expected_len {
                                error!(
                                     "      Decompression length mismatch! Column '{}', expected {}, got {}. Key: {:?}",
                                     col_name, expected_len, decompressed_len, expected_key
                                 );
                                return Err(ColumnarError::InconsistentChunkLength {
                                    expected: expected_len,
                                    actual: decompressed_len,
                                    column: col_name.clone(),
                                }
                                .into());
                            }

                            // Extract the presence bitmap
                            let bitmap = match &col_chunk {
                                ColumnChunk::Timestamp { present, .. }
                                | ColumnChunk::Integer { present, .. }
                                | ColumnChunk::Float { present, .. }
                                | ColumnChunk::String { present, .. }
                                | ColumnChunk::Enum { present, .. } => {
                                    present.bitmap().clone() // Clone the RoaringBitmap
                                }
                            };
                            trace!("      Extracted bitmap with cardinality {}. Storing decompressed data.", bitmap.len());

                            // Store in the temporary cache
                            loaded_data_cache.insert(
                                col_name.clone(),
                                DecompressedColumnData {
                                    bitmap,
                                    values: decompressed_values,
                                    chunk_key: expected_key.clone(), // Use the verified anchor key
                                },
                            );
                        } else {
                            // Inconsistency detected during verification phase!
                            error!(
                                 "Inconsistent chunk found during verification! Column '{}' has range {:?}/key {:?} but expected {:?}/{:?}.",
                                 col_name, chunk_range, key, expected_range, expected_key
                             );
                            // This indicates a fundamental problem. Error out.
                            return Err(ColumnarError::InconsistentChunkRange {
                                banyan_range: chunk.range.clone(), // Banyan's view
                                key_range: chunk_range,            // Key's view
                                column: col_name.clone(),
                            }
                            .into());
                            // // Alternative: Try to advance past the problematic range
                            // let advance_to = expected_range.end;
                            // warn!("Advancing offset to {} due to verification inconsistency.", advance_to);
                            // self.current_absolute_offset = advance_to;
                            // return Ok(false);
                        }
                    }
                    Some(Err(_)) => {
                        error!(
                            "      Error peeking iterator during verification for column {}",
                            col_name
                        );
                        return Err(iter.next().unwrap().err().unwrap().into());
                    }
                    None => {
                        // Iterator exhausted unexpectedly during verification phase.
                        error!(
                             "Iterator for column '{}' ended unexpectedly while trying to load chunk for offset {} (expected range {:?}).",
                             col_name, target_offset, expected_range
                         );
                        // We cannot form a complete row. Treat as end of stream or error? Error is safer.
                        return Err(ColumnarError::IteratorStopped(col_name.clone()).into());
                        // // Alternative: Assume we've hit the end of data for this column
                        // let advance_to = self.query_end_offset;
                        // warn!("Iterator for column '{}' ended unexpectedly. Advancing offset to {}.", col_name, advance_to);
                        // self.current_absolute_offset = advance_to;
                        // return Ok(false);
                    }
                } // End match iter.peek() during verification
            } // End Phase 2 Loop

            // --- Finalize Load ---
            // If we reached here, all columns were successfully loaded or handled (as NULLs).
            self.current_decompressed_chunk_cache = loaded_data_cache; // Commit the new cache
            self.current_chunk_range = expected_range; // Update the range covered by the cache
            trace!(
                " -> Phase 2 Complete. Successfully loaded cache for range {:?}",
                self.current_chunk_range
            );
            Ok(true) // Indicate that a chunk covering the target was loaded
        }
    }

    impl<S: manager::ColumnarBanyanStore> Iterator for ColumnarResultIterator<S> {
        type Item = Result<BTreeMap<String, Value>>; // Yield rows as map of column name -> non-null Value

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                trace!(
                    "Iterator::next() loop start, current_offset={}, query_end={}",
                    self.current_absolute_offset,
                    self.query_end_offset
                );
                // Check query bounds
                if self.current_absolute_offset >= self.query_end_offset {
                    trace!(
                        "  Offset {} reached query end {}. Stopping.",
                        self.current_absolute_offset,
                        self.query_end_offset
                    );
                    return None; // Reached the end of the query range
                }

                // Ensure the current offset is covered by the cached chunk data
                if !self
                    .current_chunk_range
                    .contains(&self.current_absolute_offset)
                {
                    trace!(
                        "  Offset {} not in cache range {:?}. Loading next chunk.",
                        self.current_absolute_offset,
                        self.current_chunk_range
                    );
                    match self.load_next_chunk(self.current_absolute_offset) {
                        Ok(true) => {
                            trace!(
                                "    load_next_chunk loaded successfully. New cache range: {:?}",
                                self.current_chunk_range
                            );
                            // Chunk loaded, loop again to check coverage and process the row
                            // Sanity check: did load_next_chunk load the *correct* chunk?
                            if !self
                                .current_chunk_range
                                .contains(&self.current_absolute_offset)
                            {
                                // This indicates a logic error in load_next_chunk if it returned Ok(true)
                                error!(
                                     "Logic Error: load_next_chunk returned true but new range {:?} does not contain target offset {}",
                                     self.current_chunk_range, self.current_absolute_offset
                                 );
                                // Set offset to end to stop iteration safely
                                self.current_absolute_offset = self.query_end_offset;
                                return Some(Err(anyhow!(
                                    "Internal iterator error: Failed to load correct chunk"
                                )));
                            }
                            continue;
                        }
                        Ok(false) => {
                            trace!("    load_next_chunk returned false. Offset potentially advanced to {}. Checking bounds.", self.current_absolute_offset);
                            // No chunk loaded, target offset was in a gap or beyond the end.
                            // load_next_chunk updated self.current_absolute_offset.
                            // Loop again to check the new offset against query_end_offset.
                            continue;
                        }
                        Err(e) => {
                            error!("    load_next_chunk failed: {}", e);
                            // Irrecoverable error during chunk loading. Stop iteration.
                            self.current_absolute_offset = self.query_end_offset; // Prevent further attempts
                            return Some(Err(e)); // Yield the error
                        }
                    }
                }

                // --- Reconstruct Row & Filter ---
                trace!(
                    "  Offset {} is within cache range {:?}. Reconstructing row.",
                    self.current_absolute_offset,
                    self.current_chunk_range
                );
                // Calculate the index relative to the start of the current cached chunk
                let relative_index =
                    (self.current_absolute_offset - self.current_chunk_range.start) as u32;
                trace!("    Relative index: {}", relative_index);

                // Build a partial row containing Option<Value> for all *needed* columns
                let mut current_row_partial_data: BTreeMap<String, Option<Value>> = BTreeMap::new();
                let mut row_has_any_non_null = false; // Track if the row is entirely NULLs

                for col_name in &self.needed_columns {
                    if let Some(decompressed_data) =
                        self.current_decompressed_chunk_cache.get(col_name)
                    {
                        // Check the presence bitmap for this column at the relative index
                        if decompressed_data.bitmap.contains(relative_index) {
                            // Value should be present. Calculate dense index using rank.
                            // rank(n) gives count of set bits <= n. rank is 1-based.
                            let dense_index = decompressed_data
                                .bitmap
                                .rank(relative_index)
                                .saturating_sub(1); // 0-based index

                            // Access the value from the decompressed Vec using the dense index
                            if let Some(value_opt) =
                                decompressed_data.values.get(dense_index as usize)
                            {
                                // Clone the Option<Value> into the partial row map
                                current_row_partial_data
                                    .insert(col_name.clone(), value_opt.clone());
                                if value_opt.is_some() {
                                    row_has_any_non_null = true;
                                }
                                trace!(
                                    "      Column '{}': Present, dense_idx={}, value={:?}",
                                    col_name,
                                    dense_index,
                                    value_opt
                                );
                            } else {
                                // Consistency error: Bitmap says present, but index out of bounds in Vec
                                error!(
                                     "Inconsistency! Column '{}', relative_idx={}, bitmap indicates presence but dense_idx={} is out of bounds for decompressed values (len {}). Cache: {:?}",
                                     col_name, relative_index, dense_index, decompressed_data.values.len(), decompressed_data
                                 );
                                self.current_absolute_offset = self.query_end_offset;
                                return Some(Err(anyhow!(
                                     "Internal iterator error: Bitmap/value inconsistency for column {}",
                                     col_name
                                 )));
                            }
                        } else {
                            // Value is not present (NULL) according to the bitmap
                            trace!("      Column '{}': Not present (NULL)", col_name);
                            current_row_partial_data.insert(col_name.clone(), None);
                        }
                    } else {
                        // Should not happen if load_next_chunk succeeded
                        error!("Internal Error: Decompressed data cache missing for needed column '{}' at offset {}", col_name, self.current_absolute_offset);
                        self.current_absolute_offset = self.query_end_offset;
                        return Some(Err(anyhow!(
                            "Internal iterator error: Missing cache data for column {}",
                            col_name
                        )));
                    }
                }

                // Apply value filters to the reconstructed partial row
                // Note: Filters operate on Option<Value>. apply_value_filters handles None correctly.
                trace!("    Applying {} value filters...", self.filters.len());
                let filters_pass = apply_value_filters(&current_row_partial_data, &self.filters);
                trace!("    Filters pass: {}", filters_pass);

                // --- Advance Offset ---
                // We've processed the row at current_absolute_offset, move to the next.
                self.current_absolute_offset += 1;

                // --- Yield Result ---
                if filters_pass {
                    trace!("    Row passed filters. Constructing final result map.");
                    // Filter passed, construct the final result map containing only *requested* columns
                    // and only non-null values.
                    let result_row: BTreeMap<String, Value> = self
                        .requested_columns
                        .iter()
                        // Get the value Option<Value> from the partial map
                        .filter_map(|req_col_name| {
                            current_row_partial_data
                                .get(req_col_name)
                                // If value is Some(Some(v)), map to Some((name, v))
                                .and_then(|value_opt| {
                                    value_opt.clone().map(|v| (req_col_name.clone(), v))
                                })
                        })
                        .collect();

                    trace!("    Yielding row: {:?}", result_row);
                    return Some(Ok(result_row)); // Yield the successful row
                } else {
                    trace!("    Row failed filters. Continuing to next offset.");
                    // Filter failed, continue the loop to the next offset
                    continue;
                }
            } // End loop
        }
    }
}

// --- Main Library Exports ---
pub use crate::{
    manager::{ColumnarBanyanStore, ColumnarDatastreamManager}, // Expose Manager & Store trait
    query::{Comparison, ValueFilter},                          // Expose Filter types
    types::{ColumnChunk, ColumnarTreeTypes, RichRangeKey, RichRangeSummary}, // Expose Banyan types
                                                               // ColumnDefinition,
                                                               // ColumnType,
                                                               // DataDefinition,
                                                               // Record,
                                                               // Value, // Expose core data types
};
// pub use banyan::{Config, Secrets}; // Re-export core Banyan config types
// pub use banyan_utils::tags::Sha256Digest; // Re-export Link type

// --- Tests ---
#[cfg(test)]
mod tests {
    // Keep the existing tests module content. It should now pass with the corrected manager logic.
    use super::*;
    use banyan::store::MemStore;
    use std::ops::Bound;
    use std::path::Path;
    use tempfile::tempdir; // For testing persistence
    use tracing_subscriber::{fmt, EnvFilter}; // For logging in tests // Ensure Path is imported

    // Helper to initialize logging for tests
    fn setup_logging() {
        let _ = fmt()
            .with_env_filter(
                EnvFilter::from_default_env().add_directive("columnar=trace".parse().unwrap()),
            ) // Set default level trace for our crate
            .with_test_writer() // Write logs to per-test buffer
            .try_init(); // Ignore error if already initialized
    }

    // Helper to create MemStore easily in tests
    fn create_test_mem_store() -> MemStore<Sha256Digest> {
        MemStore::new(usize::MAX, Sha256Digest::digest)
    }

    // Helper to create a manager with standard test schema
    fn create_test_manager(
        store: MemStore<Sha256Digest>,
        path: Option<&Path>,
    ) -> Result<ColumnarDatastreamManager<MemStore<Sha256Digest>>> {
        let config = Config::debug_fast(); // Use fast config for tests
        let secrets = Secrets::default();
        let data_def = DataDefinition::new(vec![
            ColumnDefinition {
                name: "timestamp".to_string(),
                column_type: ColumnType::Timestamp,
            },
            ColumnDefinition {
                name: "device_id".to_string(),
                column_type: ColumnType::String,
            },
            ColumnDefinition {
                name: "temperature".to_string(),
                column_type: ColumnType::Float,
            },
            ColumnDefinition {
                name: "humidity".to_string(),
                column_type: ColumnType::Float,
            },
            ColumnDefinition {
                name: "status".to_string(),
                column_type: ColumnType::Enum(vec!["OK".into(), "WARN".into(), "ERROR".into()]),
            },
        ]);
        // Use load_or_initialize to handle potential existing state file
        ColumnarDatastreamManager::load_or_initialize(store, config, secrets, path, Some(data_def))
    }

    // Generates test records with predictable patterns and some nulls
    fn create_test_records(start_offset: u64, count: usize) -> Vec<Record> {
        let mut records = Vec::new();
        for i in 0..count {
            let current_offset = start_offset + i as u64;
            let mut record = Record::new();
            record.insert(
                "timestamp".to_string(),
                // Timestamp in microseconds, increasing linearly
                Some(Value::Timestamp(current_offset as i64 * 1_000_000)),
            );
            record.insert(
                "device_id".to_string(),
                // Cycles through 10 device IDs
                Some(Value::String(format!("dev_{}", current_offset % 10))),
            );
            record.insert(
                "temperature".to_string(),
                // Base 20.0, adds cyclic offset part + linear index part
                Some(Value::Float(
                    20.0 + (current_offset % 10) as f64 + (i as f64 * 0.1),
                )),
            );
            // Introduce NULLs for humidity every 5 records (i=0, 5, 10, ...)
            if i % 5 != 0 {
                record.insert(
                    "humidity".to_string(),
                    // Base 50.0, subtracts cyclic offset part + linear index part
                    Some(Value::Float(
                        50.0 - (current_offset % 5) as f64 - (i as f64 * 0.05),
                    )),
                );
            } else {
                // Explicitly insert None for NULL
                record.insert("humidity".to_string(), None);
            }
            record.insert(
                "status".to_string(),
                // Cycles through Enum indices 0, 1, 2
                Some(Value::Enum((current_offset % 3) as u32)),
            );

            records.push(record);
        }
        records
    }

    #[test]
    fn test_extend_and_query_basic() -> Result<()> {
        setup_logging();
        let store = create_test_mem_store();
        let mut manager = create_test_manager(store, None)?; // No persistence path

        let records1 = create_test_records(0, 50);
        let records2 = create_test_records(50, 50);

        info!("Extending with first batch (0-49)...");
        manager.extend(&records1)?;
        assert_eq!(manager.total_rows, 50);
        info!("First batch extended. total_rows: {}", manager.total_rows);
        for (name, cid) in &manager.column_cids {
            info!("  Column '{}' CID: {:?}", name, cid);
            assert!(
                cid.is_some(),
                "CID should be Some after first extend for column {}",
                name
            );
        }

        info!("Extending with second batch (50-99)...");
        manager.extend(&records2)?;
        assert_eq!(manager.total_rows, 100);
        info!("Second batch extended. total_rows: {}", manager.total_rows);
        for (name, cid) in &manager.column_cids {
            info!("  Column '{}' CID: {:?}", name, cid);
            assert!(
                cid.is_some(),
                "CID should remain Some after second extend for column {}",
                name
            );
        }

        info!("Querying all data for timestamp and temperature...");
        let results_iter = manager.query(
            vec!["timestamp".to_string(), "temperature".to_string()],
            (Bound::Unbounded, Bound::Unbounded), // Full offset range
            vec![],                               // No value filters
            (Bound::Unbounded, Bound::Unbounded), // Full time range
        )?;

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Query returned {} results.", results.len());

        assert_eq!(results.len(), 100, "Should return all 100 rows");

        // Check first record (offset 0, index i=0 in first batch)
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(0 * 1_000_000)), // offset 0
            "First record timestamp mismatch"
        );
        assert_eq!(
            results[0].get("temperature"),
            // 20.0 + (0 % 10) + (0 * 0.1) = 20.0
            Some(&Value::Float(20.0)),
            "First record temperature mismatch"
        );

        // Check last record (offset 99, index i=49 in second batch)
        assert_eq!(
            results[99].get("timestamp"),
            Some(&Value::Timestamp(99 * 1_000_000)), // offset 99
            "Last record timestamp mismatch"
        );
        assert_eq!(
            results[99].get("temperature"),
            // 20.0 + (99 % 10) + (49 * 0.1) = 20.0 + 9.0 + 4.9 = 33.9
            Some(&Value::Float(33.9)), // Correct calculation: 20 + (99%10=9) + (i=49)*0.1 = 20+9+4.9
            "Last record temperature mismatch"
        );

        Ok(())
    }

    #[test]
    fn test_query_with_offset_range() -> Result<()> {
        setup_logging();
        let store = create_test_mem_store();
        let mut manager = create_test_manager(store, None)?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;
        assert_eq!(manager.total_rows, 100);

        // Query offset range [10, 20) -> should yield 10 rows (offsets 10 through 19)
        info!("Querying offset range [10, 20)...");
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Included(10), Bound::Excluded(20)),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Query [10, 20) returned {} results.", results.len());
        assert_eq!(results.len(), 10, "Offset range [10, 20) failed");
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(10 * 1_000_000)), // First element is offset 10
            "Offset range [10, 20) first element mismatch"
        );
        assert_eq!(
            results[9].get("timestamp"),
            Some(&Value::Timestamp(19 * 1_000_000)), // Last element is offset 19
            "Offset range [10, 20) last element mismatch"
        );

        // Query offset range [95, ...) -> should yield 5 rows (offsets 95 through 99)
        info!("Querying offset range [95, Unbounded)...");
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Included(95), Bound::Unbounded), // Unbounded end should go up to total_rows (100)
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Query [95, ...) returned {} results.", results.len());
        assert_eq!(results.len(), 5, "Offset range [95, ...) failed"); // 100 - 95 = 5 rows
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(95 * 1_000_000)),
            "Offset range [95, ...) first element mismatch"
        );
        assert_eq!(
            results[4].get("timestamp"),
            Some(&Value::Timestamp(99 * 1_000_000)),
            "Offset range [95, ...) last element mismatch"
        );

        Ok(())
    }

    #[test]
    fn test_query_with_time_range() -> Result<()> {
        setup_logging();
        let store = create_test_mem_store();
        let mut manager = create_test_manager(store, None)?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;
        assert_eq!(manager.total_rows, 100);

        // Query time range corresponding to offsets [30, 40)
        // Timestamp for offset N is N * 1_000_000 micros
        let start_time = 30 * 1_000_000; // Included
        let end_time = 40 * 1_000_000; // Excluded (so last included is 39 * 1M)
        info!("Querying time range [{}, {})...", start_time, end_time);
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Unbounded, Bound::Unbounded), // No offset filter
            vec![],
            (Bound::Included(start_time), Bound::Excluded(end_time)), // Time filter
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Time range query returned {} results.", results.len());

        // Should include rows with timestamps 30M, 31M, ..., 39M (10 rows)
        assert_eq!(results.len(), 10, "Time range query count mismatch");
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(start_time)), // First timestamp is 30M
            "Time range first element mismatch"
        );
        assert_eq!(
            results[9].get("timestamp"),
            Some(&Value::Timestamp(39 * 1_000_000)), // Last timestamp is 39M
            "Time range last element mismatch"
        );

        Ok(())
    }

    #[test]
    fn test_query_with_value_filter() -> Result<()> {
        setup_logging();
        let store = create_test_mem_store();
        let mut manager = create_test_manager(store, None)?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;
        assert_eq!(manager.total_rows, 100);

        // Filter: temperature > 28.0
        let filter = ValueFilter {
            column_name: "temperature".to_string(),
            operator: query::Comparison::GreaterThan,
            value: Value::Float(28.0),
        };
        info!("Querying with filter: temperature > 28.0");

        let results_iter = manager.query(
            vec!["timestamp".to_string(), "temperature".to_string()], // Request timestamp and filtered column
            (Bound::Unbounded, Bound::Unbounded),
            vec![filter], // Apply the filter
            (Bound::Unbounded, Bound::Unbounded),
        )?;

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Value filter query returned {} results.", results.len());

        // Manually calculate expected count and verify results
        let mut expected_count = 0;
        let mut checked_count = 0;
        let original_records_for_check = create_test_records(0, 100); // Regenerate for checking

        for i in 0..100 {
            let offset = i as u64;
            // Calculate temperature using the same logic as create_test_records
            // i corresponds to the index within the *single* batch generation here
            let temp = 20.0 + (offset % 10) as f64 + (i as f64 * 0.1);
            if temp > 28.0 {
                expected_count += 1;
                // Find the corresponding result row (results are ordered by offset)
                let result_row = results
                    .get(checked_count)
                    .ok_or_else(|| anyhow!("Result count mismatch during check"))?;
                // Check timestamp matches offset i
                assert_eq!(
                    result_row.get("timestamp"),
                    Some(&Value::Timestamp(offset as i64 * 1_000_000))
                );
                // Check temperature matches calculation and is > 28.0
                match result_row.get("temperature") {
                    Some(Value::Float(t)) => {
                        // Use approximate comparison for floats
                        assert!(
                            (*t - temp).abs() < f64::EPSILON,
                            "Temperature mismatch for offset {}",
                            offset
                        );
                        assert!(*t > 28.0, "Filter failed for offset {}", offset);
                    }
                    _ => panic!(
                        "Temperature column missing or wrong type in result for offset {}",
                        offset
                    ),
                }
                checked_count += 1;
            }
        }
        assert_eq!(results.len(), expected_count, "Final result count mismatch");
        assert_eq!(checked_count, expected_count, "Checked count mismatch");

        Ok(())
    }

    #[test]
    fn test_query_with_null_handling() -> Result<()> {
        setup_logging();
        let store = create_test_mem_store();
        let mut manager = create_test_manager(store, None)?;
        // Create 10 records. humidity is None (NULL) at index i=0 and i=5 (offsets 0, 5).
        let records = create_test_records(0, 10);
        manager.extend(&records)?;
        assert_eq!(manager.total_rows, 10);

        info!("Querying timestamp and humidity (includes NULLs)...");
        let results_iter = manager.query(
            vec!["timestamp".to_string(), "humidity".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![], // No filters initially
            (Bound::Unbounded, Bound::Unbounded),
        )?;

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!("Query returned {} results.", results.len());
        assert_eq!(
            results.len(),
            10,
            "Should return all 10 rows even with NULLs"
        );

        // Check presence/absence of humidity based on creation logic
        // Remember: the result map ONLY contains non-null values.
        assert!(
            results[0].get("humidity").is_none(),
            "Humidity should be NULL (absent) at offset 0"
        ); // i=0
        assert!(
            results[1].get("humidity").is_some(),
            "Humidity should be non-null (present) at offset 1"
        ); // i=1
        assert!(
            results[2].get("humidity").is_some(),
            "Humidity should be non-null (present) at offset 2"
        ); // i=2
        assert!(
            results[3].get("humidity").is_some(),
            "Humidity should be non-null (present) at offset 3"
        ); // i=3
        assert!(
            results[4].get("humidity").is_some(),
            "Humidity should be non-null (present) at offset 4"
        ); // i=4
        assert!(
            results[5].get("humidity").is_none(),
            "Humidity should be NULL (absent) at offset 5"
        ); // i=5
        assert!(
            results[6].get("humidity").is_some(),
            "Humidity should be non-null (present) at offset 6"
        ); // i=6

        // Now query with a filter that applies to humidity
        let filter = ValueFilter {
            column_name: "humidity".to_string(),
            operator: query::Comparison::LessThan, // Example filter
            value: Value::Float(100.0),            // Should match all *non-null* humidity values
        };
        info!("Querying timestamp with filter: humidity < 100.0");
        let results_iter_filtered = manager.query(
            vec!["timestamp".to_string()], // Only request timestamp
            (Bound::Unbounded, Bound::Unbounded),
            vec![filter], // Apply the humidity filter
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_filtered: Vec<BTreeMap<String, Value>> =
            results_iter_filtered.collect::<Result<_>>()?;

        info!(
            "Filtered query returned {} results.",
            results_filtered.len()
        );
        // Expected: 10 total rows - 2 rows where humidity is NULL = 8 rows
        assert_eq!(
            results_filtered.len(),
            8,
            "Filter should exclude rows where humidity is NULL"
        );

        // Check that the timestamps correspond to the non-null humidity rows
        let expected_timestamps: Vec<i64> = vec![1, 2, 3, 4, 6, 7, 8, 9]
            .into_iter()
            .map(|offset| offset * 1_000_000)
            .collect();
        let actual_timestamps: Vec<i64> = results_filtered
            .iter()
            .map(|row| match row.get("timestamp") {
                Some(Value::Timestamp(ts)) => *ts,
                _ => panic!("Timestamp missing in filtered result"),
            })
            .collect();
        assert_eq!(
            actual_timestamps, expected_timestamps,
            "Filtered timestamps do not match expected non-null rows"
        );

        Ok(())
    }

    #[test]
    fn test_persistence_save_load() -> Result<()> {
        setup_logging();
        let temp_dir = tempdir()?;
        let state_path = temp_dir.path().join("manager_state.json");
        info!("Using temp state file: {:?}", state_path);

        let store1 = create_test_mem_store();
        let data_def_for_check = DataDefinition::new(vec![
            /* ... same as in create_test_manager ... */
            ColumnDefinition {
                name: "timestamp".to_string(),
                column_type: ColumnType::Timestamp,
            },
            ColumnDefinition {
                name: "device_id".to_string(),
                column_type: ColumnType::String,
            },
            ColumnDefinition {
                name: "temperature".to_string(),
                column_type: ColumnType::Float,
            },
            ColumnDefinition {
                name: "humidity".to_string(),
                column_type: ColumnType::Float,
            },
            ColumnDefinition {
                name: "status".to_string(),
                column_type: ColumnType::Enum(vec!["OK".into(), "WARN".into(), "ERROR".into()]),
            },
        ]); // Define it once for comparison later
        let mut manager1 = create_test_manager(store1.clone(), Some(&state_path))?;

        let records = create_test_records(0, 25);
        info!("Extending manager1...");
        manager1.extend(&records)?;
        assert_eq!(manager1.total_rows, 25);

        info!("Saving manager1 state...");
        manager1.save_state()?;
        assert!(state_path.exists(), "State file was not created");

        // Store state for comparison before dropping manager1
        let manager1_cids = manager1.column_cids.clone();
        let manager1_rows = manager1.total_rows;
        // let manager1_data_def = manager1.data_definition.clone(); // Clone Arc if needed, or use data_def_for_check
        drop(manager1);

        info!("Creating manager2 from saved state...");
        let mut manager2 = ColumnarDatastreamManager::load_or_initialize(
            store1.clone(),       // Use the same store instance
            Config::debug_fast(), // Provide config/secrets again for loading
            Secrets::default(),
            Some(&state_path),
            None, // Don't provide default def, force load
        )?;

        info!("Checking loaded state...");
        assert_eq!(
            manager2.total_rows, manager1_rows,
            "Loaded total_rows mismatch"
        );
        assert_eq!(
            manager2.column_cids, manager1_cids,
            "Loaded column_cids mismatch"
        );
        // Compare the Arc<DataDefinition> inner values
        assert_eq!(
            *manager2.data_definition, data_def_for_check,
            "Loaded data_definition mismatch"
        );

        info!("Querying loaded manager2...");
        let results_iter = manager2.query(
            vec!["timestamp".to_string()],
            (Bound::Included(20), Bound::Unbounded), // Query range [20, 25)
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        info!(
            "Query on loaded manager returned {} results.",
            results.len()
        );
        assert_eq!(results.len(), 5, "Query after load failed"); // 25 - 20 = 5
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(20 * 1_000_000))
        );
        assert_eq!(
            results[4].get("timestamp"),
            Some(&Value::Timestamp(24 * 1_000_000))
        );

        info!("Extending loaded manager2...");
        let records2 = create_test_records(25, 15); // Add 15 more records
        manager2.extend(&records2)?;
        assert_eq!(
            manager2.total_rows, 40,
            "Total rows after extending loaded manager mismatch"
        );

        info!("Querying extended manager2...");
        let results_iter = manager2.query(
            vec!["timestamp".to_string()],
            (Bound::Unbounded, Bound::Unbounded), // Full range
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(
            results.len(),
            40,
            "Query after extending loaded manager failed"
        );
        assert_eq!(
            results[39].get("timestamp"),
            Some(&Value::Timestamp(39 * 1_000_000))
        );

        info!("Saving extended manager2 state...");
        manager2.save_state()?; // Save again after extending

        Ok(())
    }
} // mod tests
