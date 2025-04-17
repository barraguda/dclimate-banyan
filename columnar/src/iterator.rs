// --- Result Iterator ---
use anyhow::{anyhow, Result};
use banyan::{index::CompactSeq, FilteredChunk};
use roaring::RoaringBitmap;
use std::{
    collections::BTreeMap,
    fmt::Debug,
    iter::Peekable,
    marker::PhantomData,
    ops::{Bound, Range},
    sync::Arc,
};
use tracing::{debug, error, trace, warn};

use crate::{
    data::ColumnChunk,
    query::apply_value_filters,
    store::ColumnarBanyanStore,
    types::{ColumnarError, DataDefinition, Value},
};

use crate::compression;
use crate::query::ValueFilter;
use crate::types::RichRangeKey;

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

/// helper function to check if a value is within a specified range.
fn bound_contains<T: PartialOrd>(range: &(Bound<T>, Bound<T>), value: &T) -> bool {
    let (start, end) = range;
    let after_start = match start {
        Bound::Included(s) => value >= s,
        Bound::Excluded(s) => value > s,
        Bound::Unbounded => true,
    };
    let before_end = match end {
        Bound::Included(e) => value <= e,
        Bound::Excluded(e) => value < e,
        Bound::Unbounded => true,
    };
    after_start && before_end
}

/// Iterator that yields rows by fetching, decompressing, and aligning chunks
/// from multiple column Banyan trees.
pub struct ColumnarResultIterator<S: ColumnarBanyanStore> {
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
                dyn Iterator<Item = Result<FilteredChunk<(u64, RichRangeKey, ColumnChunk), ()>>>
                    + Send,
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

impl<S: ColumnarBanyanStore> ColumnarResultIterator<S> {
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
                impl Iterator<Item = Result<FilteredChunk<(u64, RichRangeKey, ColumnChunk), ()>>>
                    + Send
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
    // --- REVISED load_next_chunk ---
    fn load_next_chunk(&mut self, target_offset: u64) -> Result<bool> {
        debug!(
            "load_next_chunk(target_offset={}) - Current state: offset={}, cache_range={:?}",
            target_offset, self.current_absolute_offset, self.current_chunk_range
        );
        self.current_decompressed_chunk_cache.clear();
        let mut anchor_range: Option<Range<u64>> = None;
        let mut anchor_key: Option<RichRangeKey> = None;
        let mut next_candidate_start = u64::MAX;

        trace!(" -> Phase 1: Probing columns...");
        for col_name in &self.needed_columns {
            let iter = self
                .compressed_chunk_iters
                .get_mut(col_name)
                .ok_or_else(|| ColumnarError::ColumnNotFound(col_name.clone()))?;

            trace!("  --> Probing column: {}", col_name);
            loop {
                match iter.peek() {
                    Some(Ok(peeked_filtered_chunk)) => {
                        if peeked_filtered_chunk.data.is_empty() {
                            trace!(
                                "      Skipping empty FilteredChunk (range {:?}) during pre-skip",
                                peeked_filtered_chunk.range
                            );
                            iter.next();
                            continue;
                        }
                        let key = &peeked_filtered_chunk.data[0].1;
                        let chunk_range = key.start_offset..(key.start_offset + key.count);
                        if chunk_range.end <= target_offset {
                            trace!(
                                "      Skipping chunk range {:?} (ends before target {})",
                                chunk_range,
                                target_offset
                            );
                            iter.next();
                        } else {
                            trace!(
                                "      Found candidate chunk range {:?} with key {:?}",
                                chunk_range,
                                key
                            );
                            break;
                        }
                    }
                    Some(Err(_)) => {
                        error!(
                            "      Error peeking iterator during pre-skip for column {}",
                            col_name
                        );
                        return Err(iter.next().unwrap().err().unwrap().into());
                    }
                    None => {
                        trace!(
                            "      Iterator exhausted during pre-skip for column {}",
                            col_name
                        );
                        break;
                    }
                }
            }

            match iter.peek() {
                Some(Ok(chunk)) => {
                    if chunk.data.is_empty() {
                        trace!("      Peeked chunk is empty FilteredChunk (range {:?}), ignoring for anchoring.", chunk.range);
                        next_candidate_start = next_candidate_start.min(chunk.range.start);
                        continue;
                    }
                    let key = chunk.data[0].1.clone();
                    let chunk_range = key.start_offset..(key.start_offset + key.count);
                    trace!(
                        "      Candidate chunk range {:?} with key {:?}",
                        chunk_range,
                        key
                    );
                    next_candidate_start = next_candidate_start.min(chunk_range.start);
                    trace!(
                        "         (next_candidate_start updated to {})",
                        next_candidate_start
                    );

                    if chunk_range.contains(&target_offset) {
                        trace!("      -> Chunk covers target_offset {}", target_offset);
                        if anchor_range.is_none() {
                            trace!(
                                "         Setting anchor_range={:?}, anchor_key={:?}",
                                chunk_range,
                                key
                            );
                            anchor_range = Some(chunk_range);
                            anchor_key = Some(key);
                        } else {
                            if Some(&chunk_range) != anchor_range.as_ref()
                                || Some(&key) != anchor_key.as_ref()
                            {
                                error!("Inconsistent chunk keys/ranges found during probe! Column '{}' has {:?}/{:?}, but anchor is {:?}/{:?}.", col_name, chunk_range, key, anchor_range, anchor_key);
                                return Err(ColumnarError::InconsistentChunkRange {
                                    banyan_range: chunk_range,
                                    key_range: key.start_offset..(key.start_offset + key.count),
                                    column: col_name.clone(),
                                }
                                .into());
                            } else {
                                trace!("         Chunk matches existing anchor. OK.");
                            }
                        }
                    } else {
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
                    trace!("      Iterator exhausted for column {}", col_name);
                }
            }
        }
        trace!(" -> Phase 1 Complete.");

        let (expected_range, expected_key) = match (anchor_range, anchor_key) {
            (Some(r), Some(k)) => {
                trace!(
                    " -> Found anchor covering target {}: range={:?}, key={:?}",
                    target_offset,
                    r,
                    k
                );
                (r, k)
            }
            (None, None) => {
                let advance_to =
                    if next_candidate_start > target_offset && next_candidate_start != u64::MAX {
                        next_candidate_start
                    } else {
                        self.query_end_offset
                    };
                trace!(" -> No anchor found covering target {}. Advancing iterator offset to {} (next_candidate_start={}, query_end={}).", target_offset, advance_to, next_candidate_start, self.query_end_offset);
                self.current_absolute_offset = advance_to;
                return Ok(false);
            }
            _ => unreachable!(
                "Logic error: anchor_range and anchor_key should be Some or None together"
            ),
        };

        trace!(
            " -> Phase 2: Verifying and loading range {:?} with key {:?}",
            expected_range,
            expected_key
        );
        let mut loaded_data_cache = BTreeMap::new();
        let chunk_len = expected_key.count as usize; // Expected sparse length

        for col_name in &self.needed_columns {
            trace!("  --> Verifying/Loading column: {}", col_name);
            let iter = self.compressed_chunk_iters.get_mut(col_name).unwrap();
            match iter.peek() {
                Some(Ok(chunk)) => {
                    if chunk.data.is_empty() {
                        trace!("      Column '{}' chunk is empty FilteredChunk (range {:?}). Treating as NULLs.", col_name, chunk.range);
                        iter.next();
                        let null_bitmap = RoaringBitmap::new();
                        let null_values = Arc::new(vec![None; chunk_len]); // Use chunk_len
                        loaded_data_cache.insert(
                            col_name.clone(),
                            DecompressedColumnData {
                                bitmap: null_bitmap,
                                values: null_values,
                                chunk_key: expected_key.clone(),
                            },
                        );
                        continue;
                    }

                    let (key, _col_chunk_peek) = (&chunk.data[0].1, &chunk.data[0].2);
                    let chunk_range = key.start_offset..(key.start_offset + key.count);

                    if chunk_range == expected_range && *key == expected_key {
                        trace!("      OK: Chunk matches expected range and key. Consuming and decompressing.");
                        let owned_filtered_chunk = iter.next().unwrap().unwrap();
                        let (_, _key, col_chunk) =
                            owned_filtered_chunk.data.into_iter().next().unwrap();

                        let present_bitmap = match &col_chunk {
                            ColumnChunk::Timestamp { present, .. } => present.bitmap().clone(),
                            ColumnChunk::Integer { present, .. } => present.bitmap().clone(),
                            ColumnChunk::Float { present, .. } => present.bitmap().clone(),
                            ColumnChunk::String { present, .. } => present.bitmap().clone(),
                            ColumnChunk::Enum { present, .. } => present.bitmap().clone(),
                        };
                        trace!(
                            "      Extracted bitmap with cardinality {}",
                            present_bitmap.len()
                        );

                        trace!("      Decompressing dense data...");
                        let dense_values = compression::decompress_dense_data(&col_chunk)?;
                        trace!("      Decompressed {} dense values.", dense_values.len());

                        let bitmap_cardinality = present_bitmap.len() as usize;
                        if dense_values.len() != bitmap_cardinality {
                            error!("      Dense value count {} does not match bitmap cardinality {} for column '{}'", dense_values.len(), bitmap_cardinality, col_name);
                            return Err(ColumnarError::InconsistentChunkData {
                                expected: bitmap_cardinality,
                                actual: dense_values.len(),
                                column: col_name.clone(),
                            }
                            .into());
                        }

                        trace!(
                            "      Reconstructing sparse vector (length {})...",
                            chunk_len
                        );
                        let mut reconstructed_values: Vec<Option<Value>> = vec![None; chunk_len];
                        let mut dense_iter = dense_values.into_iter();
                        for present_index in present_bitmap.iter() {
                            let idx = present_index as usize;
                            if idx < chunk_len {
                                if let Some(value) = dense_iter.next() {
                                    reconstructed_values[idx] = Some(value);
                                } else {
                                    error!("Bitmap/value count mismatch during reconstruction (dense iterator exhausted) for column '{}'", col_name);
                                    return Err(ColumnarError::BitmapValueMismatch {
                                        index: present_index,
                                        column: col_name.clone(),
                                    }
                                    .into());
                                }
                            } else {
                                error!("Bitmap index {} out of bounds for key count {} for column '{}'", present_index, chunk_len, col_name);
                                return Err(ColumnarError::BitmapIndexOutOfBounds {
                                    index: present_index,
                                    len: chunk_len,
                                    column: col_name.clone(),
                                }
                                .into());
                            }
                        }
                        if dense_iter.next().is_some() {
                            error!(
                                "Extra dense values found after reconstruction for column '{}'",
                                col_name
                            );
                            return Err(ColumnarError::ExtraDenseValues {
                                column: col_name.clone(),
                            }
                            .into());
                        }
                        trace!("      Reconstruction complete.");

                        loaded_data_cache.insert(
                            col_name.clone(),
                            DecompressedColumnData {
                                bitmap: present_bitmap,
                                values: Arc::new(reconstructed_values),
                                chunk_key: expected_key.clone(),
                            },
                        );
                    } else {
                        error!("Inconsistent chunk found during verification! Column '{}' has range {:?}/key {:?} but expected {:?}/{:?}.", col_name, chunk_range, key, expected_range, expected_key);
                        return Err(ColumnarError::InconsistentChunkRange {
                            banyan_range: chunk.range.clone(),
                            key_range: chunk_range,
                            column: col_name.clone(),
                        }
                        .into());
                    }
                }
                Some(Err(_)) => {
                    error!(
                        "Error peeking iterator during verification for column {}",
                        col_name
                    );
                    return Err(iter.next().unwrap().err().unwrap().into());
                }
                None => {
                    error!("Iterator for column '{}' ended unexpectedly while trying to load chunk for offset {} (expected range {:?}).", col_name, target_offset, expected_range);
                    return Err(ColumnarError::IteratorStopped(col_name.clone()).into());
                }
            }
        }

        self.current_decompressed_chunk_cache = loaded_data_cache;
        self.current_chunk_range = expected_range;
        trace!(
            " -> Phase 2 Complete. Successfully loaded cache for range {:?}",
            self.current_chunk_range
        );
        Ok(true)
    }
}

impl<S: ColumnarBanyanStore> Iterator for ColumnarResultIterator<S> {
    type Item = Result<BTreeMap<String, Value>>; // Yield rows as map of column name -> non-null Value

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            trace!(
                "Iterator::next() loop start, current_offset={}, query_end={}",
                self.current_absolute_offset,
                self.query_end_offset
            );
            if self.current_absolute_offset >= self.query_end_offset {
                trace!(
                    "  Offset {} reached query end {}. Stopping.",
                    self.current_absolute_offset,
                    self.query_end_offset
                );
                return None;
            }

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
                        if !self
                            .current_chunk_range
                            .contains(&self.current_absolute_offset)
                        {
                            error!("Logic Error: load_next_chunk returned true but new range {:?} does not contain target offset {}", self.current_chunk_range, self.current_absolute_offset);
                            self.current_absolute_offset = self.query_end_offset;
                            return Some(Err(anyhow!(
                                "Internal iterator error: Failed to load correct chunk"
                            )));
                        }
                        continue;
                    }
                    Ok(false) => {
                        trace!("    load_next_chunk returned false. Offset potentially advanced to {}. Checking bounds.", self.current_absolute_offset);
                        continue;
                    }
                    Err(e) => {
                        error!("    load_next_chunk failed: {}", e);
                        self.current_absolute_offset = self.query_end_offset;
                        return Some(Err(e));
                    }
                }
            }

            trace!(
                "  Offset {} is within cache range {:?}. Reconstructing row.",
                self.current_absolute_offset,
                self.current_chunk_range
            );
            let relative_index =
                (self.current_absolute_offset - self.current_chunk_range.start) as u32;
            trace!("    Relative index: {}", relative_index);
            let mut current_row_partial_data: BTreeMap<String, Option<Value>> = BTreeMap::new();

            for col_name in &self.needed_columns {
                if let Some(decompressed_data) = self.current_decompressed_chunk_cache.get(col_name)
                {
                    // Access the value directly from the cached sparse Vec using relative_index
                    let sparse_index = relative_index as usize;
                    if let Some(value_opt) = decompressed_data.values.get(sparse_index) {
                        current_row_partial_data.insert(col_name.clone(), value_opt.clone());
                        trace!(
                            "      Column '{}': Value from cache={:?}",
                            col_name,
                            value_opt
                        );
                    } else {
                        error!("Cache Inconsistency! Column '{}', relative_idx {}, vec len {}. Cache: {:?}", col_name, relative_index, decompressed_data.values.len(), decompressed_data);
                        self.current_absolute_offset = self.query_end_offset;
                        return Some(Err(ColumnarError::CacheIndexOutOfBounds {
                            index: relative_index,
                            len: decompressed_data.values.len(),
                            column: col_name.clone(),
                        }
                        .into()));
                    }
                } else {
                    error!("Internal Error: Decompressed data cache missing for needed column '{}' at offset {}", col_name, self.current_absolute_offset);
                    self.current_absolute_offset = self.query_end_offset;
                    return Some(Err(anyhow!(
                        "Internal iterator error: Missing cache data for column {}",
                        col_name
                    )));
                }
            }

            let value_filters_pass = apply_value_filters(&current_row_partial_data, &self.filters);
            trace!("    Value Filters pass: {}", value_filters_pass);

            let mut time_filter_pass = true; // Default to true if no time filter or if value filters failed
            if value_filters_pass {
                // Only check time if value filters passed AND a time range is specified
                if self.query_time_range_micros != (Bound::Unbounded, Bound::Unbounded) {
                    let ts_col_name = "timestamp";
                    let ts_opt_val = current_row_partial_data.get(ts_col_name);
                    time_filter_pass = match ts_opt_val {
                        Some(Some(Value::Timestamp(ts))) => {
                            bound_contains(&self.query_time_range_micros, ts)
                        }
                        Some(None) | None => {
                            // Row is missing timestamp or it's NULL. It fails the time filter unless the range is unbounded.
                            false
                        }
                        Some(Some(_other)) => {
                            warn!(
                                "Timestamp column '{}' contained unexpected type: {:?}",
                                ts_col_name, _other
                            );
                            false
                        }
                    };
                    trace!(
                        "    Time filter check: ts_val={:?}, range={:?}, pass={}",
                        ts_opt_val.cloned().flatten(),
                        self.query_time_range_micros,
                        time_filter_pass
                    );
                } else {
                    // No time range specified in query, so time filter passes by default
                    time_filter_pass = true;
                    trace!("    Time filter check: skipped (no time range specified in query)");
                }
            } else {
                trace!("    Time filter check: skipped (value filters failed)");
                // Ensure time_filter_pass is false if value_filters_pass is false
                time_filter_pass = false;
            }

            let final_filters_pass = value_filters_pass && time_filter_pass;
            self.current_absolute_offset += 1;

            if final_filters_pass {
                let result_row: BTreeMap<String, Value> = self
                    .requested_columns
                    .iter()
                    .filter_map(|req_col_name| {
                        current_row_partial_data
                            .get(req_col_name)
                            .and_then(|value_opt| {
                                value_opt.clone().map(|v| (req_col_name.clone(), v))
                            })
                    })
                    .collect();
                trace!("    Yielding row: {:?}", result_row);
                return Some(Ok(result_row));
            } else {
                trace!("    Row failed final filters (value: {}, time: {}). Continuing to next offset.", value_filters_pass, time_filter_pass);
                continue;
            }
        }
    }
}
