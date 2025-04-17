/// @testing banyan things
/// eventually fork and upgrade banyan to latest ipld-core things etc
/// rust edition too
use anyhow::{anyhow, Result};
use banyan::{
    index::{CompactSeq, Summarizable, VecSeq},
    store::BranchCache,
    Config, Forest, Secrets, StreamBuilder, Transaction, Tree, TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
use libipld::{
    cbor::DagCborCodec,             
    codec::{Decode, Encode},
    Cid,
    DagCbor,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Read, Seek, Write},
    marker::PhantomData,
    ops::{Bound, Range},
    sync::Arc,
};
use thiserror::Error;

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
    #[error("Banyan error: {0}")]
    BanyanError(#[from] anyhow::Error), // Wrap generic anyhow errors from Banyan
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
}

// --- Basic Data Types (Potentially shared with user or adapted from row-based lib) ---

// Making these public so they can be exported easily at the end
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DagCbor)]
pub enum Value {
    Timestamp(i64), // Store as i64 microseconds directly
    Integer(i64),
    Float(f64),
    String(String),
    Enum(u32), // Store enum variant index
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Timestamp,
    Integer,
    Float,
    String,
    Enum(Vec<String>), // Store possible choices for Enum type
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
    // Add other metadata if needed, e.g., indexing hints (though Banyan tree itself is the index here)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataDefinition {
    pub columns: Vec<ColumnDefinition>,
    // Add mapping for faster lookup by name
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
}

// User data representation (Map from column name to optional value)
pub type Record = BTreeMap<String, Option<Value>>;

// --- Banyan Tree Types Implementation ---
pub mod types {
    use super::*;

    // Key for Banyan trees (same for all columns)
    // Derive DagCbor - should work as fields are simple
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
    pub struct RichRangeKey {
        pub start_offset: u64,
        pub count: u64,
        pub min_timestamp_micros: i64,
        pub max_timestamp_micros: i64,
    }

    // Summary for Banyan branches (same for all columns)
    // Derive DagCbor - should work as fields are simple
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
    pub struct RichRangeSummary {
        pub start_offset: u64,
        pub total_count: u64,
        pub min_timestamp_micros: i64,
        pub max_timestamp_micros: i64,
    }

    // Wrapper for RoaringBitmap to make it CBOR serializable
    // Remove Eq derive, keep PartialEq
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct CborRoaringBitmap {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        #[serde(skip)]
        bitmap: RoaringBitmap,
    }

    impl CborRoaringBitmap {
        pub fn new() -> Self {
            let bitmap = RoaringBitmap::new();
            let mut bytes = Vec::new();
            // Pre-serialize to ensure bytes are available if needed before explicit serialize call
            bitmap.serialize_into(&mut bytes).unwrap(); // Handle error properly
            Self { bytes, bitmap }
        }

        pub fn from_bitmap(bitmap: RoaringBitmap) -> Result<Self> {
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes)?;
            Ok(Self { bytes, bitmap })
        }

        pub fn bitmap(&self) -> &RoaringBitmap {
            &self.bitmap
        }

        pub fn bitmap_mut(&mut self) -> &mut RoaringBitmap {
            &mut self.bitmap
        }

        // Ensure bytes are updated after mutation
        pub fn update_bytes(&mut self) -> Result<()> {
            self.bytes.clear();
            self.bitmap.serialize_into(&mut self.bytes)?;
            Ok(())
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
            // Serialize the pre-computed bytes
            self.bytes.encode(c, w)
        }
    }

    impl Decode<DagCborCodec> for CborRoaringBitmap {
        fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> Result<Self> {
            let bytes: Vec<u8> = Decode::decode(c, r)?;
            let bitmap = RoaringBitmap::deserialize_from(&bytes[..])?;
            Ok(Self { bytes, bitmap })
        }
    }

    // Value stored in Banyan leaves (Columnar Data Chunk)
    // Derive DagCbor. Replace ByteBuf with Vec<u8>.
    #[derive(Clone, Debug, PartialEq, DagCbor)]
    #[ipld(repr = "kinded")]
    pub enum ColumnChunk {
        Timestamp {
            present: CborRoaringBitmap,
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
        // Potentially add variants for Bool, etc.
    }

    // BanyanValue requires DagCbor (which implies Encode/Decode for DagCborCodec) + Send + 'static
    // NVM REDUNDANT
    // impl BanyanValue for ColumnChunk {}

    // Define the TreeTypes implementation
    #[derive(Clone, Debug)]
    pub struct ColumnarTreeTypes;

    impl TreeTypes for ColumnarTreeTypes {
        type Key = RichRangeKey;
        type Summary = RichRangeSummary;
        // Using VecSeq for simplicity, could optimize later if needed
        type KeySeq = VecSeq<Self::Key>; // This now works because RichRangeKey derives DagCbor
        type SummarySeq = VecSeq<Self::Summary>; // This now works because RichRangeSummary derives DagCbor
        type Link = Sha256Digest; // Or libipld::Cid if preferred
    }

    // --- Summarizable Implementations ---

    // No changes needed here, VecSeq bounds are met now
    impl Summarizable<RichRangeSummary> for VecSeq<RichRangeKey> {
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
            let mut summary = RichRangeSummary {
                start_offset: first.start_offset,
                total_count: 0,
                min_timestamp_micros: first.min_timestamp_micros,
                max_timestamp_micros: first.max_timestamp_micros,
            };

            for key in self.as_ref().iter() {
                summary.total_count += key.count;
                summary.min_timestamp_micros =
                    summary.min_timestamp_micros.min(key.min_timestamp_micros);
                summary.max_timestamp_micros =
                    summary.max_timestamp_micros.max(key.max_timestamp_micros);
                // start_offset is the minimum start_offset in the sequence
                summary.start_offset = summary.start_offset.min(key.start_offset);
            }
            summary
        }
    }

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
            let mut summary = RichRangeSummary {
                start_offset: first.start_offset,
                total_count: 0,
                min_timestamp_micros: first.min_timestamp_micros,
                max_timestamp_micros: first.max_timestamp_micros,
            };

            for child_summary in self.as_ref().iter() {
                summary.total_count += child_summary.total_count;
                summary.min_timestamp_micros = summary
                    .min_timestamp_micros
                    .min(child_summary.min_timestamp_micros);
                summary.max_timestamp_micros = summary
                    .max_timestamp_micros
                    .max(child_summary.max_timestamp_micros);
                // start_offset is the minimum start_offset in the sequence
                summary.start_offset = summary.start_offset.min(child_summary.start_offset);
            }
            summary
        }
    }
}

// --- Compression Logic ---
pub mod compression {
    use super::types::CborRoaringBitmap;
    use super::*;
    use libipld::prelude::Codec; // Import Codec trait
    use roaring::RoaringBitmap;
    use zstd::stream::{copy_encode, decode_all};

    // Placeholder: Implement actual compression algorithms here
    fn compress_i64_delta_zstd(values: &[i64]) -> Result<Vec<u8>> {
        let mut deltas = Vec::with_capacity(values.len());
        let mut last = 0i64;
        for &val in values {
            deltas.push(val.wrapping_sub(last));
            last = val;
        }
        let cbor_deltas = DagCborCodec.encode(&deltas)?; // Use trait method
        let mut compressed = Vec::new();
        copy_encode(&cbor_deltas[..], &mut compressed, 3)?; // Write to vec
        Ok(compressed)
    }

    fn decompress_i64_delta_zstd(data: &[u8]) -> Result<Vec<i64>> {
        let cbor_deltas = decode_all(data)?;
        // Use Decode trait method
        let deltas: Vec<i64> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_deltas))?;
        let mut values = Vec::with_capacity(deltas.len());
        let mut current = 0i64;
        for delta in deltas {
            current = current.wrapping_add(delta);
            values.push(current);
        }
        Ok(values)
    }

    fn compress_f64_zstd(values: &[f64]) -> Result<Vec<u8>> {
        // weird encoding thing. requires .to_vec()
        // see if acceptable, fork/upgrade deps for banyan necessary at some point anyway
        let cbor_data = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_data[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_f64_zstd(data: &[u8]) -> Result<Vec<f64>> {
        let cbor_data = decode_all(data)?;
        let values: Vec<f64> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_data))?;
        Ok(values)
    }

    fn compress_string_zstd(values: &[String]) -> Result<Vec<u8>> {
        // weird encoding thing. requires .to_vec()
        // see if acceptable, fork/upgrade deps for banyan necessary at some point anyway
        let cbor_data = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_data[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_string_zstd(data: &[u8]) -> Result<Vec<String>> {
        let cbor_data = decode_all(data)?;
        let values: Vec<String> =
            Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_data))?;
        Ok(values)
    }

    fn compress_enum_zstd(values: &[u32]) -> Result<Vec<u8>> {
        // weird encoding thing. requires .to_vec()
        // see if acceptable, fork/upgrade deps for banyan necessary at some point anyway
        let cbor_data = DagCborCodec.encode(&values.to_vec())?;
        let mut compressed = Vec::new();
        copy_encode(&cbor_data[..], &mut compressed, 3)?;
        Ok(compressed)
    }

    fn decompress_enum_zstd(data: &[u8]) -> Result<Vec<u32>> {
        let cbor_data = decode_all(data)?;
        let values: Vec<u32> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_data))?;
        Ok(values)
    }

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

        let compressed_data = match col_type {
            ColumnType::Timestamp => {
                // Use ? for error propagation which converts ColumnarError to anyhow::Error
                let dense_i64s: Vec<i64> = dense_values
                    .into_iter()
                    .map(|v| match v {
                        Value::Timestamp(ts) => Ok(ts),
                        _ => Err(ColumnarError::TypeError {
                            expected: "Timestamp".to_string(),
                            actual: format!("{:?}", v),
                        }),
                    })
                    .collect::<Result<Vec<i64>, _>>()?; // Collect with specific error type
                compress_i64_delta_zstd(&dense_i64s)?
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
                    .collect::<Result<Vec<i64>, _>>()?;
                compress_i64_delta_zstd(&dense_i64s)?
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
                    .collect::<Result<Vec<f64>, _>>()?;
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
                    .collect::<Result<Vec<String>, _>>()?;
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
                    .collect::<Result<Vec<u32>, _>>()?;
                compress_enum_zstd(&dense_enums)?
            }
        };

        Ok((
            CborRoaringBitmap::from_bitmap(present_bitmap)?,
            compressed_data,
        ))
    }

    pub fn decompress_column(chunk: &types::ColumnChunk) -> Result<Vec<Option<Value>>> {
        let (present_bitmap, dense_values) = match chunk {
            types::ColumnChunk::Timestamp { present, data } => {
                let vals = decompress_i64_delta_zstd(data)?;
                (
                    present.bitmap(),
                    vals.into_iter()
                        .map(Value::Timestamp)
                        .collect::<Vec<Value>>(),
                )
            }
            types::ColumnChunk::Integer { present, data } => {
                let vals = decompress_i64_delta_zstd(data)?;
                (
                    present.bitmap(),
                    vals.into_iter().map(Value::Integer).collect::<Vec<Value>>(),
                )
            }
            types::ColumnChunk::Float { present, data } => {
                let vals = decompress_f64_zstd(data)?;
                (
                    present.bitmap(),
                    vals.into_iter().map(Value::Float).collect::<Vec<Value>>(),
                )
            }
            types::ColumnChunk::String { present, data } => {
                let vals = decompress_string_zstd(data)?;
                (
                    present.bitmap(),
                    vals.into_iter().map(Value::String).collect::<Vec<Value>>(),
                )
            }
            types::ColumnChunk::Enum { present, data } => {
                let vals = decompress_enum_zstd(data)?;
                (
                    present.bitmap(),
                    vals.into_iter().map(Value::Enum).collect::<Vec<Value>>(),
                )
            }
        };

        let max_index = present_bitmap.max().map(|m| m + 1).unwrap_or(0); // Use max+1 for len
        let chunk_len = max_index as usize;

        let mut result = vec![None; chunk_len];
        let mut dense_iter = dense_values.into_iter();

        for i in 0..chunk_len {
            if present_bitmap.contains(i as u32) {
                result[i] = dense_iter.next();
            }
        }
        if dense_iter.next().is_some() {
            return Err(ColumnarError::DecompressionError(
                "Mismatch between bitmap and decompressed value count".to_string(),
            )
            .into());
        }

        Ok(result)
    }
}

// --- Banyan Query Logic ---
pub mod query {
    use super::types::{ColumnarTreeTypes, RichRangeKey, RichRangeSummary};
    use super::*;
    use crate::Value; // Use the user-facing Value
    use banyan::index::{BranchIndex, LeafIndex}; // Import needed types
    use banyan::query::Query;
    // Remove import: use banyan::util::RangeBoundsExt;

    // Banyan Query struct - only contains offset and time ranges
    #[derive(Debug, Clone)]
    pub struct ColumnarBanyanQuery {
        pub offset_range: (Bound<u64>, Bound<u64>),
        pub time_range_micros: (Bound<i64>, Bound<i64>),
    }

    // Helper function to check range intersection manually
    fn ranges_intersect<T: PartialOrd>(r1: (Bound<T>, Bound<T>), r2: (Bound<T>, Bound<T>)) -> bool {
        !range_lt(r1.1, r2.0) && !range_lt(r2.1, r1.0)
    }

    // Helper: Check if bound `a` is strictly less than bound `b`
    fn range_lt<T: PartialOrd>(a: Bound<T>, b: Bound<T>) -> bool {
        match (a, b) {
            (Bound::Unbounded, _) => false, // Unbounded end is never less than anything
            (_, Bound::Unbounded) => true,  // Anything is less than Unbounded start
            (Bound::Included(a_val), Bound::Included(b_val)) => a_val < b_val,
            (Bound::Included(a_val), Bound::Excluded(b_val)) => a_val < b_val, // a <= b means !(a > b) or !(a >= b)
            (Bound::Excluded(a_val), Bound::Included(b_val)) => a_val <= b_val,
            (Bound::Excluded(a_val), Bound::Excluded(b_val)) => a_val <= b_val,
        }
    }

    impl Query<ColumnarTreeTypes> for ColumnarBanyanQuery {
        fn containing(&self, _offset: u64, index: &LeafIndex<ColumnarTreeTypes>, res: &mut [bool]) {
            let key = &index.keys.first();
            let key_offset_range = (
                Bound::Included(key.start_offset),
                Bound::Excluded(key.start_offset + key.count),
            );
            let key_time_range = (
                Bound::Included(key.min_timestamp_micros),
                Bound::Included(key.max_timestamp_micros),
            );

            let overlaps = ranges_intersect(self.offset_range, key_offset_range)
                && ranges_intersect(self.time_range_micros, key_time_range);

            for r in res.iter_mut() {
                *r &= overlaps;
            }
        }

        fn intersecting(
            &self,
            _offset: u64,
            index: &BranchIndex<ColumnarTreeTypes>,
            res: &mut [bool],
        ) {
            for (i, summary) in index.summaries.as_ref().iter().enumerate() {
                if res[i] {
                    let summary_offset_range = (
                        Bound::Included(summary.start_offset),
                        Bound::Excluded(summary.start_offset + summary.total_count),
                    );
                    let summary_time_range = (
                        Bound::Included(summary.min_timestamp_micros),
                        Bound::Included(summary.max_timestamp_micros),
                    );

                    res[i] = ranges_intersect(self.offset_range, summary_offset_range)
                        && ranges_intersect(self.time_range_micros, summary_time_range);
                }
            }
        }
    }

    // --- Value Filters (Applied *after* Banyan retrieval) ---

    #[derive(Debug, Clone)]
    pub enum Comparison {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        // Add In, NotIn, etc. if needed
    }

    #[derive(Debug, Clone)]
    pub struct ValueFilter {
        pub column_name: String,
        pub operator: Comparison,
        pub value: Value, // The value to compare against
    }

    // Helper to apply filters to a reconstructed row
    pub fn apply_value_filters(
        row: &BTreeMap<String, Option<Value>>,
        filters: &[ValueFilter],
    ) -> bool {
        if filters.is_empty() {
            return true;
        }
        filters.iter().all(|filter| {
            match row.get(&filter.column_name) {
                Some(Some(row_value)) => compare_values(row_value, &filter.value, &filter.operator),
                Some(None) => false, // Filter usually fails if value is NULL, adjust if needed
                None => false,       // Column not present in (partial) row, filter fails
            }
        })
    }

    // Comparison logic (simplified, needs robust type handling)
    fn compare_values(left: &Value, right: &Value, op: &Comparison) -> bool {
        match op {
            Comparison::Equals => left == right,
            Comparison::NotEquals => left != right,
            Comparison::GreaterThan => match (left, right) {
                (Value::Integer(l), Value::Integer(r)) => l > r,
                (Value::Float(l), Value::Float(r)) => l > r,
                (Value::Timestamp(l), Value::Timestamp(r)) => l > r,
                // Attempt float/int comparison
                (Value::Float(l), Value::Integer(r)) => *l > (*r as f64),
                (Value::Integer(l), Value::Float(r)) => (*l as f64) > *r,
                _ => false, // Type mismatch for GT
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
    use super::types::{ColumnChunk, ColumnarTreeTypes, RichRangeKey}; // Use types module
    use super::*; // Import top-level things like DataDefinition, Record etc.
    use banyan::store::{BlockWriter, ReadOnlyStore}; // Keep these imports specific
    use banyan_utils::tags::Sha256Digest; // Or Cid
    use std::path::Path;
    use std::sync::Arc;

    // Define a concrete store type or use a generic BanyanStore trait
    pub trait ColumnarBanyanStore:
        ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
    {
    }
    impl<S> ColumnarBanyanStore for S where
        S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
    {
    }

    // Implement Serialize/Deserialize for Sha256Digest via Cid string
    // impl Serialize for Sha256Digest {
    //     fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    //     where
    //         S: Serializer,
    //     {
    //         Cid::from(*self).to_string().serialize(serializer)
    //     }
    // }

    // impl<'de> Deserialize<'de> for Sha256Digest {
    //     fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    //     where
    //         D: Deserializer<'de>,
    //     {
    //         let s = String::deserialize(deserializer)?;
    //         let cid = Cid::try_from(s).map_err(serde::de::Error::custom)?;
    //         Sha256Digest::try_from(cid).map_err(serde::de::Error::custom)
    //     }
    // }

    pub struct SerializableCid(pub Cid); // Make inner Cid public

    impl Serialize for SerializableCid {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            // Serialize as a string
            self.0.to_string().serialize(serializer)
        }
    }



    impl<'de> Deserialize<'de> for SerializableCid {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>, // D is the specific Deserializer being used
        {
            let s = String::deserialize(deserializer)?;
            Cid::try_from(s)
                .map(SerializableCid)
                .map_err(|cid_error| {
                    // Convert the underlying error to a message string
                    let msg = format!("Failed to parse CID from string: {}", cid_error);
                    // Use the ::custom associated function from the Error trait,
                    // which is implemented by D::Error.
                    D::Error::custom(msg)
                })
        }
    }
    // Simple state persistence (replace with proper serialization/storage)
    // maybe store this metadata in ipfs as well? not sure here.. hmm.
    #[derive(Serialize, Deserialize)]
    struct ManagerState {
        column_cids: BTreeMap<String, Option<SerializableCid>>,
        total_rows: u64,
    }

    pub struct ColumnarDatastreamManager<S: ColumnarBanyanStore> {
        store: S,
        forest: Forest<ColumnarTreeTypes, S>, // Keep forest for transactions
        data_definition: Arc<DataDefinition>,
        column_builders: BTreeMap<String, StreamBuilder<ColumnarTreeTypes, ColumnChunk>>,
        column_cids: BTreeMap<String, Option<Sha256Digest>>, // Store CIDs for persistence
        // pub for now for tests, figure out interfaces for this fully w/ persistence methods etc
        pub total_rows: u64,
        config: Config, // Banyan config
        secrets: Secrets, // Banyan secrets
                        // Potentially add path for persistence state file
                        // persistence_path: Option<PathBuf>,
    }

    impl<S: ColumnarBanyanStore> ColumnarDatastreamManager<S> {
        pub fn new(
            store: S,
            data_definition: DataDefinition,
            config: Config,
            secrets: Secrets,
            // persistence_path: Option<&Path>, // Optional path to load/save state
        ) -> Result<Self> {
            let forest = Forest::new(store.clone(), BranchCache::default());
            let data_definition = Arc::new(data_definition);
            let column_names = data_definition
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();

            let mut manager = Self {
                store: store.clone(), // Clone store for forest and manager state
                forest,
                data_definition: data_definition.clone(),
                column_builders: BTreeMap::new(),
                column_cids: BTreeMap::new(),
                total_rows: 0,
                config: config.clone(), // Clone config
                secrets: secrets.clone(), // Clone secrets
                                        // persistence_path: persistence_path.map(|p| p.to_path_buf()),
            };

            // Initialize builders and CIDs
            for name in column_names {
                manager.column_builders.insert(
                    name.clone(),
                    StreamBuilder::new(manager.config.clone(), manager.secrets.clone()),
                );
                manager.column_cids.insert(name, None);
            }

            // // Attempt to load state if path provided
            // if let Some(path) = &manager.persistence_path {
            //     if path.exists() {
            //         manager.load_state(path)?;
            //     }
            // }

            Ok(manager)
        }

        // Placeholder for loading state from a file/store
        fn load_state(&mut self, _path: &Path) -> Result<()> {
            // let data = std::fs::read(path)?;
            // let state: ManagerState = serde_json::from_slice(&data)?; // Or use CBOR etc.
            // self.column_cids = state.column_cids;
            // self.total_rows = state.total_rows;

            // Recreate StreamBuilders from CIDs
            let mut txn = Transaction::new(self.forest.clone(), self.store.clone());
            self.column_builders.clear();
            for (name, cid_opt) in &self.column_cids {
                let builder = match cid_opt {
                    Some(cid) => {
                        txn.load_stream_builder(self.secrets.clone(), self.config.clone(), *cid)?
                    }
                    None => StreamBuilder::new(self.config.clone(), self.secrets.clone()),
                };
                self.column_builders.insert(name.clone(), builder);
            }
            Ok(())
        }

        // Placeholder for saving state
        pub fn save_state(&self, _path: &Path) -> Result<()> {
            // // Update CIDs from builders before saving
            // let mut cids_to_save = self.column_cids.clone();
            // for (name, builder) in &self.column_builders {
            //     cids_to_save.insert(name.clone(), builder.link());
            // }

            // let state = ManagerState {
            //     column_cids: cids_to_save,
            //     total_rows: self.total_rows,
            // };
            // let data = serde_json::to_vec_pretty(&state)?; // Or use CBOR etc.
            // std::fs::write(path, data)?;
            Ok(())
        }

        pub fn extend(&mut self, records: &[Record]) -> Result<()> {
            if records.is_empty() {
                return Ok(());
            }

            let count = records.len() as u64;
            let start_offset = self.total_rows;

            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;
            for record in records {
                if let Some(Some(Value::Timestamp(ts))) = record.get("timestamp") {
                    min_ts = min_ts.min(*ts);
                    max_ts = max_ts.max(*ts);
                }
            }
            if min_ts == i64::MAX {
                min_ts = 0;
                max_ts = 0;
            }

            let key = types::RichRangeKey {
                start_offset,
                count,
                min_timestamp_micros: min_ts,
                max_timestamp_micros: max_ts,
            };

            let mut txn = Transaction::new(self.forest.clone(), self.store.clone());

            for col_def in &self.data_definition.columns {
                let column_name = &col_def.name;

                let column_values: Vec<Option<Value>> = records
                    .iter()
                    .map(|record| record.get(column_name).cloned().flatten())
                    .collect();

                let (bitmap, compressed_data) =
                    compression::compress_column(&column_values, &col_def.column_type)?;

                let chunk = match col_def.column_type {
                    ColumnType::Timestamp => ColumnChunk::Timestamp {
                        present: bitmap,
                        data: compressed_data,
                    },
                    ColumnType::Integer => ColumnChunk::Integer {
                        present: bitmap,
                        data: compressed_data,
                    },
                    ColumnType::Float => ColumnChunk::Float {
                        present: bitmap,
                        data: compressed_data,
                    },
                    ColumnType::String => ColumnChunk::String {
                        present: bitmap,
                        data: compressed_data,
                    },
                    ColumnType::Enum(_) => ColumnChunk::Enum {
                        present: bitmap,
                        data: compressed_data,
                    },
                };

                let builder = self
                    .column_builders
                    .get_mut(column_name)
                    .ok_or_else(|| ColumnarError::ColumnNotFound(column_name.clone()))?;

                txn.extend_unpacked(builder, vec![(key.clone(), chunk)])?;

                self.column_cids.insert(column_name.clone(), builder.link());
            }

            self.total_rows += count;

            let _writer = txn.into_writer();

            // if let Some(path) = &self.persistence_path {
            //     self.save_state(path)?;
            // }

            Ok(())
        }

        pub fn query(
            &self,
            requested_columns: Vec<String>,
            offset_range: (Bound<u64>, Bound<u64>),
            filters: Vec<ValueFilter>, // Value-based filters
            time_range_micros: (Bound<i64>, Bound<i64>), // Time-based filter
        ) -> Result<ColumnarResultIterator<S>> {
            let mut needed_columns = requested_columns.clone();
            //let mut filter_columns = Vec::new(); // Keep track separately if needed later
            // Add columns needed for filtering
            for filter in &filters {
                if !needed_columns.contains(&filter.column_name) {
                    needed_columns.push(filter.column_name.clone());
                }
                // filter_columns.push(filter.column_name.clone());
            }
            needed_columns.sort();
            needed_columns.dedup();

            let banyan_query = ColumnarBanyanQuery {
                offset_range: offset_range.clone(),
                time_range_micros,
            };

            let mut column_chunk_iters = BTreeMap::new();
            let txn = Transaction::new(self.forest.clone(), self.store.clone()); // Read-only transaction

            for col_name in &needed_columns {
                let cid = self
                    .column_cids
                    .get(col_name)
                    .ok_or_else(|| ColumnarError::ColumnNotFound(col_name.clone()))?
                    .ok_or_else(|| anyhow!("Column '{}' has no data (CID is None)", col_name))?;

                let tree: Tree<ColumnarTreeTypes, ColumnChunk> =
                    txn.load_tree(self.secrets.clone(), cid)?;

                let iter = self
                    .forest
                    .iter_filtered_chunked(&tree, banyan_query.clone(), &|_| ());
                column_chunk_iters.insert(col_name.clone(), iter.peekable());
            }

            let query_start_offset = match offset_range.0 {
                Bound::Included(s) => s,
                Bound::Excluded(s) => s + 1,
                Bound::Unbounded => 0,
            };
            let query_end_offset = match offset_range.1 {
                Bound::Included(e) => e + 1, // End is exclusive in iterators
                Bound::Excluded(e) => e,
                Bound::Unbounded => self.total_rows, // Iterate up to the current total rows
            };

            ColumnarResultIterator::new(
                self.data_definition.clone(),
                needed_columns,
                requested_columns,
                filters,
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
    use banyan::FilteredChunk;
    // use std::collections::btree_map::Entry; // Removed unused import warning
    use std::iter::Peekable;

    // Stores the currently loaded and decompressed chunk for efficient row reconstruction
    #[derive(Debug, Clone)]
    struct DecompressedColumnData {
        bitmap: RoaringBitmap, // Use RoaringBitmap directly after getting from Cbor wrapper
        values: Arc<Vec<Option<Value>>>, // Use Arc for cheap cloning if needed by multiple rows
        chunk_key: RichRangeKey, // Keep the key for range info
    }

    // Helper trait to box iterators
    trait BoxedIteratorExt: Iterator + Sized + Send + 'static {
        fn boxed(self) -> Box<dyn Iterator<Item = Self::Item> + Send> {
            Box::new(self)
        }
    }
    impl<I: Iterator + Sized + Send + 'static> BoxedIteratorExt for I {}

    pub struct ColumnarResultIterator<S: manager::ColumnarBanyanStore> {
        data_definition: Arc<DataDefinition>,
        needed_columns: Vec<String>, // Columns needed for result + filtering
        requested_columns: Vec<String>, // Columns to include in the final output
        filters: Vec<ValueFilter>,   // Filters to apply after decompression

        // Input iterators producing compressed chunks for each needed column
        compressed_chunk_iters: BTreeMap<
            String,
            // Change the type parameter V in FilteredChunk to match Banyan's output
            Peekable<
                Box<
                    dyn Iterator<Item = Result<FilteredChunk<(u64, RichRangeKey, ColumnChunk), ()>>>
                        + Send,
                >,
            >,
        >,

        // State for the current row being processed
        current_absolute_offset: u64,
        query_end_offset: u64, // The exclusive end offset for the query

        // Cache for the currently loaded and decompressed chunk data for each needed column
        current_decompressed_chunk_cache: BTreeMap<String, DecompressedColumnData>,
        current_chunk_range: Range<u64>, // Range covered by the *currently cached* chunks
        _store_phantom: PhantomData<S>,  // To hold the store type if needed later
    }

    impl<S: manager::ColumnarBanyanStore> ColumnarResultIterator<S> {
        #[allow(clippy::too_many_arguments)]
        pub(super) fn new(
            data_definition: Arc<DataDefinition>,
            needed_columns: Vec<String>,
            requested_columns: Vec<String>,
            filters: Vec<ValueFilter>,
            // Change the signature to accept the correct iterator type
            compressed_chunk_iters: BTreeMap<
                String,
                Peekable<
                    impl Iterator<Item = Result<FilteredChunk<(u64, RichRangeKey, ColumnChunk), ()>>>
                        + Send
                        + 'static,
                >,
            >,
            query_start_offset: u64,
            query_end_offset: u64,
        ) -> Result<Self> {
            // Box the iterators - remove the incorrect map_data call
            let boxed_iters = compressed_chunk_iters
                .into_iter()
                // The iterator already yields the correct type, just box it.
                .map(|(k, v)| (k, v.boxed().peekable()))
                .collect();

            Ok(Self {
                data_definition,
                needed_columns,
                requested_columns,
                filters,
                compressed_chunk_iters: boxed_iters,
                current_absolute_offset: query_start_offset,
                query_end_offset,
                current_decompressed_chunk_cache: BTreeMap::new(),
                current_chunk_range: 0..0, // Initial empty range
                _store_phantom: PhantomData,
            })
        }

        // Load and decompress the next set of aligned chunks covering the target_offset
        fn load_next_chunk(&mut self, target_offset: u64) -> Result<bool> {
            self.current_decompressed_chunk_cache.clear();
            let mut anchor_range: Option<Range<u64>> = None;
            let mut anchor_key: Option<RichRangeKey> = None;
            let mut next_candidate_start = u64::MAX; // Earliest start >= target_offset
            let mut advance_target = target_offset; // If we fail, where should we jump to?
        
            // --- Phase 1: Probe for next candidate range and anchor ---
            for col_name in &self.needed_columns {
                let iter = self.compressed_chunk_iters.get_mut(col_name)
                    .ok_or_else(|| ColumnarError::ColumnNotFound(col_name.clone()))?;
        
                // Skip chunks entirely before the target offset
                while let Some(Ok(peeked_chunk)) = iter.peek() {
                    if peeked_chunk.data.is_empty() {
                        iter.next(); // Consume empty chunk
                        continue;
                    }
                     // Assume data[0].1 is RichRangeKey (checked later more robustly)
                    let key = match peeked_chunk.data.get(0) {
                         Some((_, k, _)) => k.clone(),
                         None => { // Should not happen if !is_empty checked
                              iter.next(); // Consume problematic chunk
                              continue;
                         }
                    };
                    let chunk_range = key.start_offset..(key.start_offset + key.count);
        
                    if chunk_range.end <= target_offset {
                        iter.next(); // Consume chunk before target
                    } else {
                        break; // Found chunk potentially covering or after target
                    }
                }
        
                // Check the chunk at or after the target
                match iter.peek() {
                    Some(Ok(chunk)) => {
                         if chunk.data.is_empty() { continue; } // Skip if Banyan filtered it
                         let key = chunk.data[0].1.clone();
                         let chunk_range = key.start_offset..(key.start_offset + key.count);
        
                         // Update the earliest possible start for the *next* valid chunk
                         next_candidate_start = next_candidate_start.min(chunk_range.start);
        
                         if chunk_range.contains(&target_offset) {
                             if anchor_range.is_none() {
                                 // This is the first column we found that covers the target
                                 anchor_range = Some(chunk_range);
                                 anchor_key = Some(key);
                             } else {
                                 // Check consistency with already found anchor
                                 if Some(&chunk_range) != anchor_range.as_ref() || Some(&key) != anchor_key.as_ref() {
                                     // Inconsistency detected during probe! We must skip this target.
                                     // Advance past the *first* range we found.
                                     advance_target = anchor_range.unwrap().end;
                                     self.current_absolute_offset = advance_target;
                                     return Ok(false);
                                 }
                             }
                         }
                         // else: This chunk starts after target_offset, handled by next_candidate_start
                    }
                    Some(Err(_)) => return Err(iter.next().unwrap().err().unwrap()), // Propagate error
                    None => {
                         // This iterator is exhausted before finding the target or a later chunk.
                         // The overall query might still succeed if other columns cover the range,
                         // but this specific probe yields no candidate start.
                    }
                }
            } // End Phase 1 Probe Loop
        
            // --- Check Probe Results ---
            let (expected_range, expected_key) = match (anchor_range, anchor_key) {
                (Some(r), Some(k)) => (r, k), // Found a candidate chunk covering the target
                _ => {
                    // No chunk found covering target_offset for *any* column.
                    // Advance to the next possible start or end of query.
                    if next_candidate_start > target_offset && next_candidate_start != u64::MAX {
                        self.current_absolute_offset = next_candidate_start; // Skip gap
                    } else {
                        self.current_absolute_offset = self.query_end_offset; // Likely end of all streams
                    }
                    return Ok(false);
                }
            };
        
            // --- Phase 2: Verify and Load All Columns ---
            let mut loaded_data = BTreeMap::new();
            for col_name in &self.needed_columns {
                let iter = self.compressed_chunk_iters.get_mut(col_name).unwrap(); // Known to exist
        
                match iter.peek() {
                    Some(Ok(chunk)) => {
                         if chunk.data.is_empty() {
                             // Inconsistency: Banyan filtered this chunk, but others were not?
                             eprintln!("Warning: Banyan filtered chunk for column '{}' at expected range {:?}, inconsistent with other columns. Advancing.", col_name, expected_range);
                             self.current_absolute_offset = expected_range.end;
                             return Ok(false);
                         }
                         let key = chunk.data[0].1.clone();
                         let chunk_range = key.start_offset..(key.start_offset + key.count);
        
                         if chunk_range == expected_range && key == expected_key {
                             // Consume the verified chunk
                             let owned_chunk = iter.next().unwrap().unwrap(); // Safe due to peek
                             let (_, rich_key, col_chunk) = owned_chunk.data.into_iter().next().unwrap();
        
                             // Decompress
                             let decompressed_values = Arc::new(compression::decompress_column(&col_chunk)?);
                             let expected_len = rich_key.count as usize;
                             if decompressed_values.len() != expected_len {
                                  return Err(ColumnarError::InconsistentChunkLength{ expected: expected_len, actual: decompressed_values.len(), column: col_name.clone() }.into());
                             }
        
                             // Extract bitmap
                              let bitmap = match &col_chunk {
                                  types::ColumnChunk::Timestamp { present, .. } |
                                  types::ColumnChunk::Integer { present, .. } |
                                  types::ColumnChunk::Float { present, .. } |
                                  types::ColumnChunk::String { present, .. } |
                                  types::ColumnChunk::Enum { present, .. } => present.bitmap().clone(),
                              };
        
                              // Store for caching
                              loaded_data.insert(col_name.clone(), DecompressedColumnData {
                                  bitmap,
                                  values: decompressed_values,
                                  chunk_key: rich_key,
                              });
        
                         } else {
                              // Inconsistency found during verification phase
                              eprintln!("Warning: Inconsistent chunk found during load for column '{}' at offset {}. Expected {:?}/{:?}, got {:?}/{:?}. Advancing.",
                                         col_name, target_offset, expected_range, expected_key, chunk_range, key);
                              self.current_absolute_offset = expected_range.end; // Skip this inconsistent range
                              return Ok(false);
                         }
                    }
                    Some(Err(_)) => return Err(iter.next().unwrap().err().unwrap()), // Propagate error
                    None => {
                          // Iterator ended when we expected data - inconsistency
                          eprintln!("Warning: Iterator for column '{}' ended unexpectedly when verifying offset {}. Advancing.", col_name, target_offset);
                          self.current_absolute_offset = expected_range.end; // Skip this inconsistent range
                          return Ok(false);
                    }
                }
            } // End Phase 2 Loop
        
            // If we reach here, all columns were successfully loaded and verified
            self.current_decompressed_chunk_cache = loaded_data;
            self.current_chunk_range = expected_range;
            Ok(true)
        }
    }

    impl<S: manager::ColumnarBanyanStore> Iterator for ColumnarResultIterator<S> {
        type Item = Result<BTreeMap<String, Value>>; // Return map of requested column name -> Value

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                if self.current_absolute_offset >= self.query_end_offset {
                    return None;
                }

                if !self
                    .current_chunk_range
                    .contains(&self.current_absolute_offset)
                {
                    match self.load_next_chunk(self.current_absolute_offset) {
                        Ok(true) => {} // Continue loop
                        Ok(false) => {
                            if self.current_absolute_offset >= self.query_end_offset {
                                return None;
                            }
                            continue; // Try loading at the updated offset
                        }
                        Err(e) => {
                            self.current_absolute_offset = self.query_end_offset;
                            return Some(Err(e));
                        }
                    }
                }

                let relative_index =
                    (self.current_absolute_offset - self.current_chunk_range.start) as u32;
                let mut current_row_partial_data: BTreeMap<String, Option<Value>> = BTreeMap::new();
                let mut row_is_present_somewhere = false;

                for col_name in &self.needed_columns {
                    if let Some(decompressed_data) =
                        self.current_decompressed_chunk_cache.get(col_name)
                    {
                        if decompressed_data.bitmap.contains(relative_index) {
                            let dense_index = decompressed_data
                                .bitmap
                                .rank(relative_index)
                                .saturating_sub(1); // rank is 1-based
                            if let Some(value_opt) =
                                decompressed_data.values.get(dense_index as usize)
                            {
                                current_row_partial_data
                                    .insert(col_name.clone(), value_opt.clone());
                                if value_opt.is_some() {
                                    row_is_present_somewhere = true;
                                }
                            } else {
                                self.current_absolute_offset = self.query_end_offset;
                                return Some(Err(anyhow!(
                                     "Inconsistency: Bitmap indicates presence but value missing at relative index {} (dense index {}) for column {}",
                                     relative_index, dense_index, col_name
                                 )));
                            }
                        } else {
                            current_row_partial_data.insert(col_name.clone(), None);
                        }
                    } else {
                        self.current_absolute_offset = self.query_end_offset;
                        return Some(Err(anyhow!(
                            "Decompressed data cache missing for needed column '{}'",
                            col_name
                        )));
                    }
                }

                let filters_pass = if !row_is_present_somewhere && !self.filters.is_empty() {
                    false
                } else {
                    // Use corrected variable name here
                    apply_value_filters(&current_row_partial_data, &self.filters)
                };

                self.current_absolute_offset += 1;

                if filters_pass {
                    let result_row: BTreeMap<String, Value> = self
                        .requested_columns
                        .iter()
                        .filter_map(|req_col_name| {
                            current_row_partial_data
                                .get(req_col_name)
                                .and_then(|value_opt| value_opt.clone())
                                .map(|value| (req_col_name.clone(), value))
                        })
                        .collect();

                    return Some(Ok(result_row));
                } else {
                    continue;
                }
            }
        }
    }
}

// --- Main Library Entry Point & Exports ---
// Make top-level types public for library users
pub use crate::{
    manager::ColumnarDatastreamManager,
    query::ValueFilter,
    // Import types directly, not via `types` module alias
    // ColumnChunk, ColumnDefinition, ColumnType, ColumnarTreeTypes, DataDefinition, Record, Value,
};
// pub use banyan::{Config, Secrets}; // Re-export core Banyan types

// Example usage function (replace with actual library API)
#[cfg(test)]
mod tests {
    use super::*;
    use banyan::store::MemStore;
    use std::ops::Bound;

    // Helper to create MemStore easily in tests
    fn create_test_mem_store() -> MemStore<Sha256Digest> {
        println!("creatin store");
        let store = MemStore::new(usize::MAX, Sha256Digest::digest);
        println!("store created");
        store
    }

    fn create_test_manager() -> Result<ColumnarDatastreamManager<MemStore<Sha256Digest>>> {
        let store = create_test_mem_store();
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
        ColumnarDatastreamManager::new(store, data_def, config, secrets)
    }

    fn create_test_records(start_offset: u64, count: usize) -> Vec<Record> {
        let mut records = Vec::new();
        for i in 0..count {
            let offset = start_offset + i as u64;
            let mut record = Record::new();
            record.insert(
                "timestamp".to_string(),
                Some(Value::Timestamp(offset as i64 * 1_000_000)),
            ); // Example timestamp in micros
            record.insert(
                "device_id".to_string(),
                Some(Value::String(format!("dev_{}", offset % 10))),
            );
            record.insert(
                "temperature".to_string(),
                Some(Value::Float(20.0 + (offset % 10) as f64 + (i as f64 * 0.1))),
            );
            // Introduce some nulls
            if i % 5 != 0 {
                record.insert(
                    "humidity".to_string(),
                    Some(Value::Float(50.0 - (offset % 5) as f64 - (i as f64 * 0.05))),
                );
            } else {
                record.insert("humidity".to_string(), None);
            }
            record.insert("status".to_string(), Some(Value::Enum((offset % 3) as u32)));

            records.push(record);
        }
        records
    }

    #[test]
    fn test_extend_and_query_basic() -> Result<()> {
        let mut manager = create_test_manager()?;
        let records1 = create_test_records(0, 50);
        let records2 = create_test_records(50, 50);
        println!("extending with records1");
        manager.extend(&records1)?;
        assert_eq!(manager.total_rows, 50);
        manager.extend(&records2)?;
        assert_eq!(manager.total_rows, 100);
        println!("extending with records2");

        // Query all data for specific columns
        let results_iter = manager.query(
            vec!["timestamp".to_string(), "temperature".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![], // No value filters
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        println!("querying all data");

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(results.len(), 100);
        // Check first record's data (relative to creation logic)
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(0 * 1_000_000))
        );
        assert_eq!(
            results[0].get("temperature"),
            Some(&Value::Float(20.0 + (0 % 10) as f64 + (0.0 * 0.1)))
        ); // Fixed calculation
           // Check last record's data
        assert_eq!(
            results[99].get("timestamp"),
            Some(&Value::Timestamp(99 * 1_000_000))
        );
        // Index i for the second batch runs from 0 to 49. For absolute offset 99, i is 49.
        assert_eq!(
            results[99].get("temperature"),
            Some(&Value::Float(20.0 + (99 % 10) as f64 + (49.0 * 0.1)))
        ); // Fixed calculation

        Ok(())
    }

    #[test]
    fn test_query_with_offset_range() -> Result<()> {
        let mut manager = create_test_manager()?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;

        // Query offset range [10, 20)
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Included(10), Bound::Excluded(20)),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(results.len(), 10); // 20 - 10
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(10 * 1_000_000))
        );
        assert_eq!(
            results[9].get("timestamp"),
            Some(&Value::Timestamp(19 * 1_000_000))
        );

        // Query offset range [95, ...)
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Included(95), Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(results.len(), 5); // 100 - 95
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(95 * 1_000_000))
        );
        assert_eq!(
            results[4].get("timestamp"),
            Some(&Value::Timestamp(99 * 1_000_000))
        );

        Ok(())
    }

    #[test]
    fn test_query_with_time_range() -> Result<()> {
        let mut manager = create_test_manager()?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;

        // Query time range corresponding to offsets [30, 40)
        let start_time = 30 * 1_000_000;
        let end_time = 40 * 1_000_000; // Exclusive
        let results_iter = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Included(start_time), Bound::Excluded(end_time)),
        )?;
        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(results.len(), 10);
        assert_eq!(
            results[0].get("timestamp"),
            Some(&Value::Timestamp(start_time))
        );
        assert_eq!(
            results[9].get("timestamp"),
            Some(&Value::Timestamp(end_time - 1_000_000))
        ); // last included ts

        Ok(())
    }

    #[test]
    fn test_query_with_value_filter() -> Result<()> {
        let mut manager = create_test_manager()?;
        let records = create_test_records(0, 100);
        manager.extend(&records)?;

        let filter = ValueFilter {
            column_name: "temperature".to_string(),
            operator: query::Comparison::GreaterThan,
            value: Value::Float(28.0),
        };

        let results_iter = manager.query(
            vec!["timestamp".to_string(), "temperature".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;

        // Manually calculate expected count
        let mut expected_count = 0;
        for i in 0..100 {
            let offset = i as u64;
            let temp = 20.0 + (offset % 10) as f64 + (i as f64 * 0.1); // Use absolute offset 'i' for record index 'i'
            if temp > 28.0 {
                expected_count += 1;
            }
        }
        assert_eq!(results.len(), expected_count);

        for row in results {
            match row.get("temperature") {
                Some(Value::Float(t)) => assert!(*t > 28.0),
                _ => panic!("Temperature column missing or wrong type in result"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_query_with_null_handling() -> Result<()> {
        let mut manager = create_test_manager()?;
        let records = create_test_records(0, 10); // Create 10 records, humidity is null at i=0, i=5
        manager.extend(&records)?;

        let results_iter = manager.query(
            vec!["timestamp".to_string(), "humidity".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;

        let results: Vec<BTreeMap<String, Value>> = results_iter.collect::<Result<_>>()?;
        assert_eq!(results.len(), 10);

        // In the result map, missing keys indicate NULL values from the source.
        assert!(results[0].get("humidity").is_none());
        assert!(results[5].get("humidity").is_none());

        assert!(results[1].get("humidity").is_some());
        assert!(results[2].get("humidity").is_some());

        let filter = ValueFilter {
            column_name: "humidity".to_string(),
            operator: query::Comparison::LessThan,
            value: Value::Float(100.0), // Should match all non-null values
        };
        let results_iter_filtered = manager.query(
            vec!["timestamp".to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_filtered: Vec<BTreeMap<String, Value>> =
            results_iter_filtered.collect::<Result<_>>()?;
        assert_eq!(results_filtered.len(), 8); // 10 total - 2 nulls

        Ok(())
    }
}
