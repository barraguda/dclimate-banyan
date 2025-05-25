//! Core data types defining the schema, values, and Banyan tree elements.

use banyan::{
    index::{CompactSeq, Summarizable, VecSeq}, // Use specific imports
    TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
// use cbor_data::codec::{ReadCbor, WriteCbor};
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    // prelude::Codec, // Import Codec trait explicitly
    DagCbor,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Read, Seek, Write},
};

// --- Schema Definition ---

/// Represents the possible data types for a column within the datastream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DagCbor)]
pub enum ColumnType {
    /// Represents a point in time, stored internally as `i64` microseconds
    /// since the Unix epoch (UTC). Assumed to be the primary time index.
    Timestamp,
    /// A 64-bit signed integer.
    Integer,
    /// A 64-bit floating-point number.
    Float,
    /// A UTF-8 encoded string.
    String,
    /// An enumerated type, represented by a fixed set of predefined strings.
    /// Stored internally as a `u32` index into the defining list.
    Enum(Vec<String>),
}

/// Defines a single column, including its name and data type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DagCbor)]
pub struct ColumnDefinition {
    /// The unique identifier for the column.
    pub name: String,
    /// The data type of the values stored in this column.
    pub column_type: ColumnType,
    // Note: Indexing hints are less relevant here as Banyan indexes primarily
    // on the ChunkKey (offset, time). Filtering is often post-retrieval or
    // potentially via future ChunkSummary enhancements.
}

/// Defines the overall structure (schema) of the data stream.
///
/// This includes the set of columns and their types. It also maintains an
/// internal mapping from column names to their index for efficient access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDefinition {
    /// Ordered list of column definitions composing the schema.
    pub columns: Vec<ColumnDefinition>,
    /// Internal mapping from column name to its index in the `columns` vector.
    /// This is built automatically and not serialized.
    #[serde(skip)]
    name_to_index: BTreeMap<String, usize>,
}

impl DataDefinition {
    /// Creates a new `DataDefinition` from a list of columns.
    ///
    /// Automatically builds the internal name-to-index mapping.
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

    /// Retrieves a column's definition by its name.
    pub fn get_col_def(&self, name: &str) -> Option<&ColumnDefinition> {
        self.name_to_index
            .get(name)
            .and_then(|&i| self.columns.get(i))
    }

    /// Retrieves a column's index (position) in the schema by its name.
    pub fn get_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).cloned()
    }

    /// Reconstructs the internal name-to-index mapping.
    ///
    /// This is typically needed after deserializing a `DataDefinition`.
    pub fn rebuild_index(&mut self) {
        self.name_to_index = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
    }
}

// --- Data Values ---

/// Represents a single data value within a row.
///
/// Values are stored efficiently based on their `ColumnType`. Nullability
/// is handled by wrapping this enum in `Option<Value>` within a `Record`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] // No DagCbor needed directly here
pub enum Value {
    /// Microseconds since Unix epoch UTC.
    Timestamp(i64),
    /// Signed 64-bit integer.
    Integer(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 String.
    String(String),
    /// Index into the `Vec<String>` of the corresponding `ColumnType::Enum`.
    Enum(u32),
}

/// Represents a single row of data as perceived by the user.
///
/// It's a map from column names (strings) to optional `Value`s.
/// `None` indicates a null value for that column in this row.
pub type Record = BTreeMap<String, Option<Value>>;

// --- Banyan Tree Structure ---

/// The key used in the Banyan tree index.
///
/// Each key identifies a specific chunk (`RowSeqData`) of rows, defining its
/// absolute offset range and timestamp range. Keys must be monotonically
/// increasing based on `start_offset`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
pub struct ChunkKey {
    /// The absolute row index (starting from 0) of the first row in this chunk.
    pub start_offset: u64,
    /// The number of logical rows contained within this chunk.
    pub count: u32,
    /// The minimum timestamp (microseconds UTC) found within this chunk's rows.
    pub min_timestamp_micros: i64,
    /// The maximum timestamp (microseconds UTC) found within this chunk's rows.
    pub max_timestamp_micros: i64,
}

/// The summary stored in Banyan tree branch nodes.
///
/// Aggregates the ranges (offset and time) covered by all descendant chunks or summaries.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, DagCbor)]
pub struct ChunkSummary {
    /// The minimum `start_offset` among all descendant keys/summaries.
    pub start_offset: u64,
    /// The total number of logical rows covered by all descendants.
    pub total_count: u64,
    /// The overall minimum timestamp across all descendants.
    pub min_timestamp_micros: i64,
    /// The overall maximum timestamp across all descendants.
    pub max_timestamp_micros: i64,
    // Potential future enhancements:
    // - Bloom filters for specific column values
    // - Min/max values for indexed columns within the summarized range
}

/// A wrapper around `roaring::RoaringBitmap` to enable CBOR/IPLD serialization.
///
/// This manually handles encoding/decoding by serializing the bitmap to its
/// optimized byte format and then encoding those bytes using DagCbor.
#[derive(Clone, Debug, PartialEq)]
pub struct CborRoaringBitmap {
    bitmap: RoaringBitmap,
}

impl CborRoaringBitmap {
    /// Creates a new, empty bitmap wrapper.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Creates a wrapper from an existing `RoaringBitmap`.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self { bitmap }
    }

    /// Returns a reference to the inner `RoaringBitmap`.
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Consumes the wrapper and returns the inner `RoaringBitmap`.
    pub fn into_bitmap(self) -> RoaringBitmap {
        self.bitmap
    }

    /// Returns the number of set bits (cardinality) in the bitmap.
    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    /// Checks if the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }
}

// Manual implementation for DagCbor encoding/decoding
impl Encode<DagCborCodec> for CborRoaringBitmap {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        self.bitmap.serialize_into(&mut bytes)?; // Use RoaringBitmap's efficient serialization
        bytes.encode(c, w) // Encode the bytes using DagCbor
    }
}

impl Decode<DagCborCodec> for CborRoaringBitmap {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        let bytes: Vec<u8> = Decode::decode(c, r)?;
        let bitmap = RoaringBitmap::deserialize_from(&bytes[..])?;
        Ok(Self { bitmap })
    }
}

/// Stores compressed data for a single column within a `RowSeqData` chunk.
#[derive(Clone, Debug, PartialEq, DagCbor)]
pub struct ColumnChunkData {
    /// Bitmap indicating which rows *within this chunk* (relative indices 0 to N-1)
    /// have non-null values for this column.
    pub present: CborRoaringBitmap,
    /// The actual compressed data bytes for the non-null values.
    /// The compression method depends on the `ColumnType`.
    pub data: Vec<u8>,
}

/// The value type stored in the leaves of the Banyan tree.
///
/// Represents a chunk of multiple logical rows, stored columnarly.
/// Each column's data is compressed independently based on its type.
#[derive(Clone, Debug, PartialEq, DagCbor)]
pub struct RowSeqData {
    /// The number of logical rows this chunk represents. Max ~4 billion.
    /// This defines the length of the decompressed columns.
    pub num_rows: u32,
    /// Compressed data for the mandatory 'timestamp' column.
    /// Assumes timestamps are non-null and dense within the chunk for this simple schema.
    /// If timestamps could be null, a separate `CborRoaringBitmap` would be needed here.
    // TODO: Revisit if timestamps can be nullable. If so, store a bitmap.
    pub timestamps_micros: Vec<u8>,
    /// Compressed data for all other columns defined in the schema, keyed by column name.
    /// If a column is entirely null within this chunk, its entry might be omitted
    /// from this map (handled during decompression).
    pub columns: BTreeMap<String, ColumnChunkData>,
}

// --- Banyan TreeTypes Implementation ---

/// Defines the concrete types used for the Banyan tree in this library.
#[derive(Clone, Debug)]
pub struct RowSeqTT;

impl TreeTypes for RowSeqTT {
    /// The key identifying data chunks (offset, time range).
    type Key = ChunkKey;
    /// The summary aggregating ranges in branch nodes.
    type Summary = ChunkSummary;
    /// How sequences of keys are stored (using DagCbor compatible VecSeq).
    type KeySeq = VecSeq<Self::Key>;
    /// How sequences of summaries are stored (using DagCbor compatible VecSeq).
    type SummarySeq = VecSeq<Self::Summary>;
    /// The type used for linking tree nodes (standard Banyan digest).
    type Link = Sha256Digest;
}

// --- Banyan Summarizable Implementations ---

/// Defines how to create a `ChunkSummary` from a sequence of `ChunkKey`s (leaf level).
impl Summarizable<ChunkSummary> for VecSeq<ChunkKey> {
    fn summarize(&self) -> ChunkSummary {
        if self.is_empty() {
            return ChunkSummary {
                start_offset: 0, // Or u64::MAX? Using 0 for now.
                total_count: 0,
                min_timestamp_micros: i64::MAX,
                max_timestamp_micros: i64::MIN,
            };
        }
        // Assumes keys are ordered by start_offset within the sequence
        let first = self.first();
        let mut min_ts = first.min_timestamp_micros;
        let mut max_ts = first.max_timestamp_micros;
        let mut total_count = 0;

        for key in self.as_ref().iter() {
            total_count += key.count as u64; // Sum counts
            min_ts = min_ts.min(key.min_timestamp_micros);
            max_ts = max_ts.max(key.max_timestamp_micros);
        }

        ChunkSummary {
            start_offset: first.start_offset, // Min start offset from first key
            total_count,
            min_timestamp_micros: min_ts,
            max_timestamp_micros: max_ts,
        }
    }
}

/// Defines how to create a `ChunkSummary` from a sequence of child `ChunkSummary`s (branch level).
impl Summarizable<ChunkSummary> for VecSeq<ChunkSummary> {
    fn summarize(&self) -> ChunkSummary {
        if self.is_empty() {
            return ChunkSummary {
                start_offset: 0,
                total_count: 0,
                min_timestamp_micros: i64::MAX,
                max_timestamp_micros: i64::MIN,
            };
        }

        let first = self.first();
        let mut combined = ChunkSummary {
            start_offset: first.start_offset,
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
            combined.start_offset = combined.start_offset.min(child_summary.start_offset);
        }
        combined
    }
}

// --- User Query Filter Types ---

/// Comparison operators available for user-defined value filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserFilterOp {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    // Future possibilities:
    // IsNull, IsNotNull, In(Vec<Value>), NotIn(Vec<Value>), Contains (for strings)
}

/// Represents a single filter condition applied to reconstructed rows *after*
/// potentially relevant chunks have been retrieved from the Banyan tree.
#[derive(Debug, Clone)]
pub struct UserFilter {
    /// The name of the column to apply the filter on.
    pub column_name: String,
    /// The comparison operator to use.
    pub operator: UserFilterOp,
    /// The value to compare the column's data against.
    pub value: Value,
}
