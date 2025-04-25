//! Result iterator that yields rows matching query criteria.
//!
//! This iterator consumes chunks from the Banyan query, decompresses relevant
//! columns, reconstructs rows, and applies final filtering based on offset,
//! time, and user-defined value predicates.

use crate::{
    compression,
    error::{BanyanRowSeqError, Result}, // Use local Result alias
    query,
    types::{ChunkKey, DataDefinition, Record, RowSeqData, UserFilter, Value},
    BanyanRowSeqStore,
};
use banyan::FilteredChunk;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    ops::Bound,
    sync::Arc,
};
use tracing::{debug, error, trace, warn};

/// Caches decompressed columns for the currently processed chunk.
/// Uses `Arc` to potentially share decompressed data if needed elsewhere,
/// though currently primarily used for efficient access within the iterator.
struct DecompressedChunkCache {
    /// Map from column name to its decompressed `Vec<Option<Value>>`.
    columns: HashMap<String, Arc<Vec<Option<Value>>>>,
    /// The absolute start offset of the cached chunk.
    _start_offset: u64, // Keep for context if needed, currently unused directly
    /// The number of rows in the cached chunk.
    num_rows: u32,
}

/// An iterator that yields `Record`s matching the query criteria.
///
/// It processes `RowSeqData` chunks obtained from a Banyan query,
/// decompresses necessary columns, reconstructs rows one by one,
/// and applies offset, time, and value filters before yielding.
pub struct RowSeqResultIterator<S: BanyanRowSeqStore> {
    /// Shared reference to the data schema.
    schema: Arc<DataDefinition>,
    /// Iterator over filtered chunks provided by the Banyan query engine.
    /// Boxed to handle the dynamic dispatch of the underlying iterator type.
    banyan_iter: Box<
        dyn Iterator<Item = anyhow::Result<FilteredChunk<(u64, ChunkKey, RowSeqData), ()>>> + Send,
    >,
    /// List of column names requested by the user to include in the result records.
    requested_columns: Vec<String>,
    /// List of user-defined filters to apply to reconstructed rows.
    filters: Vec<UserFilter>,
    /// The absolute start offset (inclusive) for the query range.
    query_start_offset: u64,
    /// The absolute end offset (exclusive) for the query range.
    query_end_offset_exclusive: u64,
    /// The timestamp range (inclusive bounds) for the query.
    query_time_range_micros: (Bound<i64>, Bound<i64>),

    // --- Iterator State ---
    /// Cache for the decompressed columns of the *currently* processed chunk.
    current_chunk_cache: Option<DecompressedChunkCache>,
    /// The key of the *currently* processed chunk.
    current_chunk_key: Option<ChunkKey>,
    /// The absolute offset of the *next* row this iterator expects to yield.
    /// Used for offset filtering and tracking progress.
    next_absolute_offset: u64,
    /// Internal buffer holding reconstructed rows that passed all filters
    /// and are ready to be yielded.
    row_buffer: VecDeque<Record>,
    /// Phantom data to associate the iterator with the store type `S`.
    _store_phantom: PhantomData<S>,
}

impl<S: BanyanRowSeqStore> RowSeqResultIterator<S> {
    /// Creates a new `RowSeqResultIterator`.
    ///
    /// # Arguments
    ///
    /// * `schema` - The data definition for the stream being queried.
    /// * `banyan_iter` - The iterator yielding filtered Banyan chunks.
    /// * `requested_columns` - Columns to include in the output `Record`s.
    /// * `filters` - Row-level value filters to apply.
    /// * `query_start_offset` - Inclusive start offset for the query.
    /// * `query_end_offset_exclusive` - Exclusive end offset for the query.
    /// * `query_time_range_micros` - Inclusive time bounds for the query.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new iterator or a `BanyanRowSeqError` if
    /// validation fails (e.g., requested/filtered columns not in schema).
    #[allow(clippy::too_many_arguments)] // Construction needs these parameters
    pub fn new(
        schema: Arc<DataDefinition>,
        banyan_iter: Box<
            dyn Iterator<Item = anyhow::Result<FilteredChunk<(u64, ChunkKey, RowSeqData), ()>>>
                + Send,
        >,
        requested_columns: Vec<String>,
        filters: Vec<UserFilter>,
        query_start_offset: u64,
        query_end_offset_exclusive: u64,
        query_time_range_micros: (Bound<i64>, Bound<i64>),
    ) -> Result<Self> {
        // --- Validate Inputs ---
        // Ensure requested columns exist in the schema
        for col_name in &requested_columns {
            if schema.get_col_def(col_name).is_none() {
                error!(column = %col_name, "Requested column not found in schema");
                return Err(BanyanRowSeqError::ColumnNotFound(col_name.clone()));
            }
        }
        // Ensure filtered columns exist in the schema
        for filter in &filters {
            if schema.get_col_def(&filter.column_name).is_none() {
                error!(column = %filter.column_name, "Column specified in filter not found in schema");
                return Err(BanyanRowSeqError::ColumnNotFound(
                    filter.column_name.clone(),
                ));
            }
            // TODO: Add validation comparing filter.value type with schema column type?
        }
        // --- End Validation ---

        debug!(
            ?requested_columns,
            num_filters = filters.len(),
            query_offset = ?(query_start_offset, query_end_offset_exclusive),
            query_time = ?query_time_range_micros,
            "Creating RowSeqResultIterator"
        );

        Ok(Self {
            schema,
            banyan_iter,
            requested_columns,
            filters,
            query_start_offset,
            query_end_offset_exclusive,
            query_time_range_micros,
            current_chunk_cache: None,
            current_chunk_key: None,
            // Start seeking from the query's beginning offset
            next_absolute_offset: query_start_offset,
            row_buffer: VecDeque::new(),
            _store_phantom: PhantomData,
        })
    }

    /// Attempts to fill the internal `row_buffer` by processing the next relevant
    /// chunk from the Banyan iterator.
    ///
    /// This involves:
    /// 1. Getting the next `FilteredChunk` from `banyan_iter`.
    /// 2. Skipping chunks that are entirely before the `next_absolute_offset`.
    /// 3. Decompressing columns needed for filtering and requested output.
    /// 4. Iterating through rows within the chunk:
    ///    - Skipping rows outside the query's offset range.
    ///    - Applying the time range filter.
    ///    - Reconstructing the row `Record`.
    ///    - Applying user value filters.
    ///    - Adding passing rows (projected to `requested_columns`) to `row_buffer`.
    /// 5. Updating the `current_chunk_cache` and `current_chunk_key`.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the buffer was successfully populated with at least one row.
    /// * `Ok(false)` - If the Banyan iterator is exhausted or no more relevant chunks were found.
    /// * `Err(BanyanRowSeqError)` - If an error occurred during chunk retrieval, decompression, or reconstruction.
    fn fill_buffer(&mut self) -> Result<bool> {
        trace!(
            next_absolute_offset = self.next_absolute_offset,
            query_end_offset = self.query_end_offset_exclusive,
            buffer_current_len = self.row_buffer.len(),
            "Attempting to fill row buffer"
        );

        // Stop if we've already processed past the query's end offset
        if self.next_absolute_offset >= self.query_end_offset_exclusive {
            trace!("Reached query end offset. Buffer fill stops.");
            return Ok(false); // Indicate no more data can be added
        }

        // --- Step 1: Get the next relevant RowSeqData chunk from Banyan ---
        let (chunk_key, chunk_data) = loop {
            match self.banyan_iter.next() {
                Some(Ok(filtered_chunk)) => {
                    // Banyan yields FilteredChunk which contains matching elements.
                    // In our setup, each relevant leaf yields one element tuple: (offset, key, value)
                    trace!(banyan_range = ?filtered_chunk.range, data_items = filtered_chunk.data.len(), "Processing FilteredChunk from Banyan");

                    if filtered_chunk.data.is_empty() {
                        // This might happen if the query pruned everything within the chunk range?
                        trace!("FilteredChunk data is empty, skipping.");
                        continue;
                    }
                    // We expect exactly one item matching our RowSeqData structure per relevant leaf
                    if filtered_chunk.data.len() > 1 {
                        warn!(
                            num_items = filtered_chunk.data.len(),
                            "Received FilteredChunk with more than one item, using only the first."
                        );
                    }

                    // Extract the key and value (RowSeqData) from the first item
                    // The first element of the tuple (offset) is Banyan's internal offset, less relevant here than the key's offset.
                    let (_, key, value) = filtered_chunk.data.into_iter().next().unwrap();

                    // Calculate chunk's absolute offset range (exclusive end)
                    let chunk_end_offset_exclusive =
                        key.start_offset.saturating_add(key.count as u64);

                    debug!(
                        loaded_chunk_key = ?key,
                        loaded_chunk_offset_range = ?(key.start_offset..chunk_end_offset_exclusive),
                        loaded_chunk_time_range=? (key.min_timestamp_micros..=key.max_timestamp_micros),
                        "Chunk yielded by Banyan iterator"
                    );

                    // --- Filter 1a: Skip chunks entirely *before* the next needed offset ---
                    // If the chunk ends before or exactly at the offset we're looking for next, skip it.
                    if chunk_end_offset_exclusive <= self.next_absolute_offset {
                        trace!(
                            ?key,
                            chunk_end = chunk_end_offset_exclusive,
                            next_req = self.next_absolute_offset,
                            "Skipping chunk: ends before next needed offset."
                        );
                        continue; // Get the next chunk from Banyan
                    }

                    // --- Filter 1b: Stop if chunk starts *after* the query ends ---
                    // If the chunk starts at or after the query's exclusive end, we're done.
                    if key.start_offset >= self.query_end_offset_exclusive {
                        trace!(
                            ?key,
                            query_end = self.query_end_offset_exclusive,
                            "Stopping iteration: Chunk starts at or after query end offset."
                        );
                        return Ok(false); // Indicate Banyan iterator is effectively exhausted for this query
                    }

                    // This chunk is potentially relevant (overlaps with the remaining query offset range)
                    trace!(?key, "Found relevant chunk from Banyan iterator");
                    break (key, value); // Proceed to decompress this chunk
                }
                Some(Err(e)) => {
                    error!("Error fetching chunk from Banyan iterator: {}", e);
                    // Wrap the anyhow::Error from Banyan into our StoreError variant
                    return Err(BanyanRowSeqError::StoreError(e));
                }
                None => {
                    trace!("Banyan iterator exhausted.");
                    return Ok(false); // Indicate no more chunks are available
                }
            }
        }; // End loop to find next relevant chunk

        // --- Step 2: Decompress columns needed for this chunk ---
        let decompress_span = tracing::debug_span!(
            "decompress_chunk_columns",
            chunk_offset = chunk_key.start_offset,
            chunk_rows = chunk_key.count
        )
        .entered();

        let mut decompressed_cache = HashMap::new();
        // Determine which columns are needed: requested output + filter columns + timestamp (if time filtering)
        let mut needed_columns = self.requested_columns.clone();
        for filter in &self.filters {
            needed_columns.push(filter.column_name.clone());
        }
        let has_time_filter = self.query_time_range_micros != (Bound::Unbounded, Bound::Unbounded);
        if has_time_filter {
            needed_columns.push("timestamp".to_string());
        }
        needed_columns.sort(); // Sort for potential caching benefits later
        needed_columns.dedup(); // Remove duplicates

        trace!(needed = ?needed_columns, "Decompressing needed columns for chunk");

        for col_name in &needed_columns {
            match compression::decompress_column_from_chunk(&chunk_data, col_name, &self.schema) {
                Ok(decompressed_vec_opt) => {
                    // Basic validation: check decompressed length against chunk metadata
                    if decompressed_vec_opt.len() != chunk_key.count as usize {
                        error!( column=%col_name, expected_rows=chunk_key.count, decompressed_rows=decompressed_vec_opt.len(), "Decompressed column length mismatch!" );
                        return Err(BanyanRowSeqError::DecompressionError(format!(
                            "Decompression consistency error: Column '{}' decompressed to {} rows, but chunk key expected {}",
                            col_name,
                            decompressed_vec_opt.len(),
                            chunk_key.count
                        )));
                    }
                    trace!(column=%col_name, rows=decompressed_vec_opt.len(), "Decompressed column successfully");
                    decompressed_cache.insert(col_name.clone(), Arc::new(decompressed_vec_opt));
                }
                Err(BanyanRowSeqError::ColumnNotFound(_)) => {
                    // This is expected if the column was entirely NULL in this chunk.
                    // The decompressor handles this by returning vec![None; num_rows].
                    // We might get this error if schema lookup *itself* failed inside decompress,
                    // which shouldn't happen if schema validation passed in `new`.
                    // Let's assume `decompress_column_from_chunk` handles the "all null" case internally now.
                    // Re-check `decompress_column_from_chunk`: it *does* handle the "not in map" case.
                    // So, if we get ColumnNotFound *here*, it means the schema lookup failed, which is bad.
                    warn!(column = %col_name, "Decompression failed: Column not found in schema (should have been caught earlier). Treating as missing data.");
                    // Insert a vector of Nones, but log a warning
                    let null_vec = vec![None; chunk_key.count as usize];
                    decompressed_cache.insert(col_name.clone(), Arc::new(null_vec));
                }
                Err(e) => {
                    // Propagate other decompression errors
                    error!(column = %col_name, error = ?e, "Error during column decompression");
                    return Err(e);
                }
            }
        }
        drop(decompress_span);

        // --- Step 3: Reconstruct rows, filter (Offset, Time, Value), add to buffer ---
        let reconstruct_span =
            tracing::debug_span!("reconstruct_and_filter", chunk_rows = chunk_key.count).entered();
        let mut rows_added_to_buffer = 0;
        let mut rows_processed_in_chunk = 0;
        let mut rows_passing_all_filters = 0;

        // Iterate through each logical row *within the current chunk*
        for relative_idx in 0..chunk_key.count {
            rows_processed_in_chunk += 1;
            let current_absolute_offset = chunk_key.start_offset + relative_idx as u64;

            // --- Filter 2: Check Offset Range ---
            // Skip if before the start offset we're currently seeking *or* at/after the query end offset.
            if current_absolute_offset < self.next_absolute_offset
                || current_absolute_offset >= self.query_end_offset_exclusive
            {
                trace!(
                    absolute_offset = current_absolute_offset,
                    query_range = ?(self.next_absolute_offset, self.query_end_offset_exclusive),
                    "Skipping row: outside effective query offset range [{}, {})",
                    self.next_absolute_offset, self.query_end_offset_exclusive
                );
                continue;
            }

            // --- Get Timestamp for Time Filter (if needed) ---
            // We need the timestamp value *before* full row reconstruction if time filtering is active.
            let row_timestamp_micros: Option<i64> = if has_time_filter {
                match decompressed_cache.get("timestamp") {
                    Some(ts_arc) => {
                        // Get the Option<Value> for this row index
                        match ts_arc.get(relative_idx as usize).cloned().flatten() {
                            Some(Value::Timestamp(ts)) => Some(ts),
                            Some(other) => {
                                warn!(absolute_offset = current_absolute_offset, unexpected_type = ?other, "Found non-Timestamp value in timestamp column cache");
                                None // Treat type mismatch as non-matching timestamp
                            }
                            None => None, // Timestamp is NULL
                        }
                    }
                    None => {
                        // This shouldn't happen if 'timestamp' was added to needed_columns
                        error!(
                            absolute_offset = current_absolute_offset,
                            "Timestamp column missing from cache despite time filter being active"
                        );
                        None // Treat missing cache as non-matching timestamp
                    }
                }
            } else {
                None // No time filter, don't need the timestamp value here
            };

            // --- Filter 3: Check Time Range ---
            if has_time_filter {
                match row_timestamp_micros {
                    Some(ts) => {
                        // Check against query bounds
                        let after_start = match self.query_time_range_micros.0 {
                            Bound::Included(start) => ts >= start,
                            Bound::Excluded(start) => ts > start,
                            Bound::Unbounded => true,
                        };
                        let before_end = match self.query_time_range_micros.1 {
                            Bound::Included(end) => ts <= end,
                            Bound::Excluded(end) => ts < end,
                            Bound::Unbounded => true,
                        };

                        if !(after_start && before_end) {
                            trace!(absolute_offset = current_absolute_offset, timestamp = ts, query_time = ?self.query_time_range_micros, "Skipping row: outside query time range");
                            continue; // Skip to next row in chunk
                        }
                        // Time filter passed
                    }
                    None => {
                        // Timestamp was NULL or missing/invalid type
                        trace!(
                            absolute_offset = current_absolute_offset,
                            "Skipping row: NULL or invalid timestamp value for time filter"
                        );
                        continue; // Skip rows with null/invalid timestamps if filtering by time
                    }
                }
            }
            // --- End Time Filter ---

            // Row has passed Offset and Time filters, now check Value filters.
            // We need to reconstruct the row (or relevant parts) for this.

            // --- Reconstruct Row (or necessary parts) ---
            // Optimization: Only reconstruct columns needed for filters OR final output.
            let needs_reconstruction =
                !self.filters.is_empty() || !self.requested_columns.is_empty();
            let mut current_row = Record::new(); // Use BTreeMap for Record

            if needs_reconstruction {
                let reconstruct_row_span = tracing::trace_span!(
                    "reconstruct_row",
                    absolute_offset = current_absolute_offset
                )
                .entered();
                // Use `needed_columns` which includes requested + filtered cols
                for col_name in &needed_columns {
                    match decompressed_cache.get(col_name) {
                        Some(decompressed_col_arc) => {
                            // Get the Option<Value> for this row index.
                            // `flatten()` converts Option<&Option<Value>> to Option<Value>
                            // `cloned()` is needed because `get` returns a reference.
                            let value_opt = decompressed_col_arc
                                .get(relative_idx as usize)
                                .cloned()
                                .flatten();
                            current_row.insert(col_name.clone(), value_opt);
                        }
                        None => {
                            // Column was needed but missing from cache (should only happen if decompression failed earlier and logged warning)
                            // Insert None, consistent with missing data.
                            current_row.insert(col_name.clone(), None);
                        }
                    }
                }
                drop(reconstruct_row_span);
            }

            // --- Filter 4: Apply User Value Filters ---
            let passes_value_filters = query::apply_user_filters(&current_row, &self.filters);

            if !passes_value_filters {
                trace!(
                    absolute_offset = current_absolute_offset,
                    "Skipping row: failed value filters"
                );
                continue; // Skip to next row
            }

            // --- All Filters Passed ---
            rows_passing_all_filters += 1;
            trace!(
                absolute_offset = current_absolute_offset,
                "Row passed ALL filters"
            );

            // Project the reconstructed row to only the columns requested by the user
            let final_record: Record = self
                .requested_columns
                .iter()
                .map(|req_col| {
                    // Clone the Option<Value> from the potentially larger `current_row`
                    let value = current_row.get(req_col).cloned().flatten();
                    (req_col.clone(), value)
                })
                .collect();

            // Add the final record to the output buffer
            self.row_buffer.push_back(final_record);
            rows_added_to_buffer += 1;
        } // End loop over rows within the chunk
        drop(reconstruct_span);

        debug!(
            chunk_key = ?chunk_key,
            rows_in_chunk = rows_processed_in_chunk,
            rows_passed_filters = rows_passing_all_filters,
            rows_added_to_buffer,
            "Finished processing loaded chunk"
        );

        // Update iterator state with the chunk we just processed
        self.current_chunk_cache = Some(DecompressedChunkCache {
            columns: decompressed_cache,
            _start_offset: chunk_key.start_offset,
            num_rows: chunk_key.count,
        });
        self.current_chunk_key = Some(chunk_key);

        Ok(rows_added_to_buffer > 0) // Return true if we added anything to the buffer
    } // End fill_buffer
}

impl<S: BanyanRowSeqStore> Iterator for RowSeqResultIterator<S> {
    type Item = Result<Record>; // Yields Result<Record, BanyanRowSeqError>

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to yield a row from the buffer first
            if let Some(row) = self.row_buffer.pop_front() {
                let yielded_offset = self.next_absolute_offset;
                // IMPORTANT: Increment the offset tracker *after* yielding
                self.next_absolute_offset = yielded_offset.saturating_add(1);
                trace!(
                    yielded_offset,
                    new_next_offset = self.next_absolute_offset,
                    "Yielding row from buffer"
                );
                return Some(Ok(row));
            }

            // If buffer is empty, check if we've reached the end of the query range
            if self.next_absolute_offset >= self.query_end_offset_exclusive {
                trace!(
                    next_offset = self.next_absolute_offset,
                    end_offset = self.query_end_offset_exclusive,
                    "Iterator exhausted (reached query end offset)"
                );
                return None; // Query range is fully processed
            }

            // Buffer is empty and we haven't reached the end offset, try to fill it
            trace!(
                buffer_len = self.row_buffer.len(),
                next_abs_offset = self.next_absolute_offset,
                "Buffer empty, attempting to fill..."
            );
            match self.fill_buffer() {
                Ok(true) => {
                    // Buffer was refilled, loop again to pop from it
                    trace!("Buffer refilled, continuing loop");
                    continue;
                }
                Ok(false) => {
                    // fill_buffer returned false, meaning Banyan iterator is exhausted
                    // or no more relevant chunks were found. Mark iterator as finished.
                    trace!("fill_buffer returned false (source exhausted), stopping iterator.");
                    // Ensure we don't try to fill again
                    self.next_absolute_offset = self.query_end_offset_exclusive;
                    return None;
                }
                Err(e) => {
                    // An error occurred while trying to fill the buffer.
                    error!("Error filling buffer: {}", e);
                    // Mark iterator as finished to prevent further attempts
                    self.next_absolute_offset = self.query_end_offset_exclusive;
                    // Yield the error
                    return Some(Err(e));
                }
            }
        } // End loop
    } // End next()
}
