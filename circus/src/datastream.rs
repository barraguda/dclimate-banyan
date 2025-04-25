//! The main datastream structure for managing and querying time-series data.

use crate::{
    compression,
    error::{BanyanRowSeqError, Result}, // Use local Result alias
    iterator::RowSeqResultIterator,
    query::RowSeqBanyanQuery,
    types::{
        ChunkKey, ColumnDefinition, DataDefinition, Record, RowSeqData, RowSeqTT, UserFilter, Value,
    },
    BanyanRowSeqStore,
};
use anyhow::{anyhow, Context}; // Use anyhow for context chaining
use banyan::{
    store::{BlockWriter, BranchCache},
    Config, FilteredChunk, Forest, Secrets, StreamBuilder, Transaction, Tree, TreeTypes,
};
use banyan_utils::tags::Sha256Digest;
use libipld::Cid;
use serde::{Deserialize, Serialize};
use std::{ops::Bound, path::Path, sync::Arc};
use tracing::{debug, error, info, trace, warn}; // Use specific tracing levels

/// Internal state structure for persistence.
///
/// Stores the essential information needed to reconstruct the datastream state:
/// the root CID of the Banyan tree, the total number of rows, and the schema.
#[derive(Serialize, Deserialize, Debug)]
struct DatastreamState {
    /// The CID of the Banyan tree root, stored as a string. None if the tree is empty.
    root_cid_str: Option<String>,
    /// The total number of logical rows stored in the datastream across all chunks.
    total_rows: u64,
    /// The data definition (schema) associated with this datastream.
    schema: DataDefinition,
}

/// Manages a time-series datastream stored in a single Banyan tree.
///
/// This struct provides methods to initialize, load, extend (append data),
/// query, and persist the state of the datastream. It uses `RowSeqData`
/// chunks as values in the Banyan tree leaves, enabling columnar storage
/// and compression.
///
/// Type Parameters:
/// * `S`: The type of the Banyan store backend (e.g., `MemStore`, `IpfsStore`),
///        which must implement the `BanyanRowSeqStore` trait alias.
#[derive(Clone)] // Forest and Arc<DataDefinition> are cloneable
pub struct DatastreamRowSeq<S: BanyanRowSeqStore> {
    /// The Banyan Forest managing the tree structure and interactions with the store.
    forest: Forest<RowSeqTT, S>,
    /// A clone of the underlying Banyan store, used for creating transactions.
    store: S,
    /// Configuration for building Banyan tree nodes (e.g., target chunk size).
    config: Config,
    /// Secrets used for encrypting/decrypting tree nodes (if applicable).
    secrets: Secrets,
    /// The schema definition for the data in this stream, wrapped in Arc for cheap cloning.
    schema: Arc<DataDefinition>,
    /// The CID of the current root node of the Banyan tree. `None` if the stream is empty.
    root_cid: Option<Cid>,
    /// The total number of logical rows currently stored in the stream.
    total_rows: u64,
    /// Optional path to a file for persisting the `DatastreamState`.
    persistence_path: Option<std::path::PathBuf>,
    // Note: StreamBuilder is transiently created within transactions, not stored here.
}

impl<S: BanyanRowSeqStore> DatastreamRowSeq<S> {
    /// Creates a new, empty datastream instance.
    ///
    /// # Arguments
    ///
    /// * `store` - The Banyan store backend to use.
    /// * `schema` - The `DataDefinition` describing the data structure.
    /// * `config` - Banyan tree configuration.
    /// * `secrets` - Banyan secrets for encryption.
    /// * `persistence_path` - Optional path to a file for saving/loading state.
    ///
    /// # Returns
    ///
    /// A new `DatastreamRowSeq` instance.
    pub fn new(
        store: S,
        schema: DataDefinition, // Takes ownership of the initial schema
        config: Config,
        secrets: Secrets,
        persistence_path: Option<&Path>,
    ) -> Self {
        info!(
            ?schema,
            ?config,
            ?persistence_path,
            "Creating new DatastreamRowSeq"
        );
        let forest = Forest::new(store.clone(), BranchCache::default());
        // Ensure the schema's internal index is built
        let mut schema_mut = schema;
        schema_mut.rebuild_index();

        Self {
            forest,
            store,
            config,
            secrets,
            schema: Arc::new(schema_mut),
            root_cid: None,
            total_rows: 0,
            persistence_path: persistence_path.map(|p| p.to_path_buf()),
        }
    }

    /// Loads datastream state from a persistence file or initializes a new one.
    ///
    /// If `persistence_path` is provided and the file exists and can be loaded
    /// successfully, the state (root CID, total rows, schema) is restored.
    /// The `required_schema` is then *verified* against the loaded schema;
    /// if they don't match, a `SchemaMismatch` error is returned.
    ///
    /// If the file doesn't exist, cannot be loaded, or `persistence_path` is `None`,
    /// a new datastream is initialized using the `required_schema`.
    ///
    /// # Arguments
    ///
    /// * `store` - The Banyan store backend.
    /// * `config` - Banyan tree configuration.
    /// * `secrets` - Banyan secrets.
    /// * `persistence_path` - Optional path to the persistence file.
    /// * `required_schema` - The schema definition. Used for initialization if loading fails,
    ///                      and for verification if loading succeeds.
    ///
    /// # Returns
    ///
    /// A `Result` containing the loaded or initialized `DatastreamRowSeq`, or an error
    /// (e.g., IO error, deserialization error, schema mismatch).
    pub fn load_or_initialize(
        store: S,
        config: Config,
        secrets: Secrets,
        persistence_path: Option<&Path>,
        required_schema: DataDefinition, // Schema for initialization or verification
    ) -> Result<Self> {
        let path_buf = persistence_path.map(|p| p.to_path_buf());

        if let Some(path) = &path_buf {
            if path.exists() {
                info!("Attempting to load datastream state from {:?}", path);
                match Self::load_from_file(path, store.clone(), config.clone(), secrets.clone()) {
                    Ok(mut loaded_ds) => {
                        // --- Schema Verification ---
                        // Rebuild index of required schema for consistent comparison
                        let mut req_schema_indexed = required_schema;
                        req_schema_indexed.rebuild_index();
                        // Rebuild index of loaded schema (serde skips the index field)
                        Arc::make_mut(&mut loaded_ds.schema).rebuild_index();

                        if loaded_ds.schema.columns != req_schema_indexed.columns {
                            error!(
                                path = ?path,
                                loaded_schema = ?loaded_ds.schema.columns,
                                required_schema = ?req_schema_indexed.columns,
                                "Schema mismatch between loaded state and required schema!"
                            );
                            return Err(BanyanRowSeqError::SchemaMismatch);
                        }
                        info!(
                            path = ?path,
                            root_cid = ?loaded_ds.root_cid,
                            total_rows = loaded_ds.total_rows,
                            "Successfully loaded and verified datastream state."
                        );
                        // Ensure the loaded schema is used
                        loaded_ds.schema = Arc::new(req_schema_indexed); // Use the verified schema instance
                        return Ok(loaded_ds);
                    }
                    Err(e) => {
                        warn!(
                            path = ?path,
                            error = ?e,
                            "Failed to load state from file. Initializing new datastream."
                        );
                        // Fall through to initialize below
                    }
                }
            } else {
                info!(
                    "Persistence file not found at {:?}. Initializing new datastream.",
                    path
                );
            }
        } else {
            info!("No persistence path provided. Initializing new datastream.");
        }

        // Initialization path
        Ok(Self::new(
            store,
            required_schema, // Use the provided schema for initialization
            config,
            secrets,
            persistence_path, // Pass the original Option<&Path>
        ))
    }

    /// Internal helper to load state directly from a file.
    fn load_from_file(path: &Path, store: S, config: Config, secrets: Secrets) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open state file: {:?}", path))?;
        let state: DatastreamState = serde_json::from_reader(file)
            .with_context(|| format!("Failed to deserialize state from: {:?}", path))?;

        debug!(?state, "Deserialized state from file");

        // Parse CID from string
        let root_cid = match state.root_cid_str {
            Some(s) => Some(
                Cid::try_from(s.as_str())
                    .map_err(|e| anyhow!("Failed to parse root CID string '{}': {}", s, e))?,
            ),
            None => None,
        };

        let forest = Forest::new(store.clone(), BranchCache::default());
        let mut loaded_schema = state.schema;
        loaded_schema.rebuild_index(); // Rebuild index after deserialization

        Ok(Self {
            forest,
            store,
            config,
            secrets,
            schema: Arc::new(loaded_schema), // Schema will be verified by caller
            root_cid,
            total_rows: state.total_rows,
            persistence_path: Some(path.to_path_buf()),
        })
    }

    /// Saves the current datastream state (root CID, total rows, schema) to the persistence path.
    ///
    /// Does nothing if no persistence path was configured.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if saving fails (IO or serialization).
    pub fn save_state(&self) -> Result<()> {
        if let Some(path) = &self.persistence_path {
            debug!("Saving datastream state to {:?}", path);
            let state = DatastreamState {
                root_cid_str: self.root_cid.map(|c| c.to_string()),
                total_rows: self.total_rows,
                // Clone the inner DataDefinition from the Arc for serialization
                schema: (*self.schema).clone(),
            };

            // Create parent directories if they don't exist
            if let Some(parent_dir) = path.parent() {
                std::fs::create_dir_all(parent_dir).with_context(|| {
                    format!(
                        "Failed to create parent directory for state file: {:?}",
                        parent_dir
                    )
                })?;
            }

            // Use temp file writing for atomicity
            let temp_path = path.with_extension("tmp");
            let file = std::fs::File::create(&temp_path).with_context(|| {
                format!("Failed to create temporary state file: {:?}", temp_path)
            })?;

            serde_json::to_writer_pretty(&file, &state).with_context(|| {
                format!(
                    "Failed to serialize state to temporary file: {:?}",
                    temp_path
                )
            })?;

            // Ensure data is flushed to disk before renaming
            file.sync_all()
                .with_context(|| format!("Failed to sync temporary state file: {:?}", temp_path))?;
            drop(file); // Close the file before renaming

            // Atomically replace the old state file with the new one
            std::fs::rename(&temp_path, path).with_context(|| {
                format!(
                    "Failed to rename temporary state file {:?} to {:?}",
                    temp_path, path
                )
            })?;

            debug!(
                path = ?path,
                root_cid = ?state.root_cid_str,
                total_rows = state.total_rows,
                "Datastream state saved successfully."
            );
        } else {
            trace!("Attempted to save state, but no persistence path configured.");
        }
        Ok(())
    }

    /// Returns the CID of the current Banyan tree root, if any.
    pub fn root_cid(&self) -> Option<Cid> {
        self.root_cid
    }

    /// Returns the total number of logical rows stored in the datastream.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Returns a shared reference to the datastream's schema (`DataDefinition`).
    pub fn schema(&self) -> Arc<DataDefinition> {
        self.schema.clone()
    }

    /// Extends the datastream with a batch of new records.
    ///
    /// This operation performs the following steps:
    /// 1. Compresses the provided `records` into a `RowSeqData` chunk using `compression::compress_row_seq_data`.
    /// 2. Calculates the `ChunkKey` for the new chunk based on the current `total_rows` and the timestamp range of the batch.
    /// 3. Starts a Banyan `Transaction`.
    /// 4. Loads the current `StreamBuilder` (or creates a new one if the stream is empty).
    /// 5. Extends the `StreamBuilder` with the new `(ChunkKey, RowSeqData)` pair.
    /// 6. Retrieves the new root link (digest/CID) from the builder.
    /// 7. Updates the internal `root_cid` and `total_rows`.
    /// 8. Optionally saves the updated state to the persistence file.
    ///
    /// # Arguments
    ///
    /// * `records` - A slice of `Record`s to append to the stream. The records must conform to the datastream's schema.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if compression, Banyan operations, or persistence fails.
    ///
    /// # Errors
    ///
    /// Can return errors related to schema mismatch (`TypeError`), compression (`CompressionError`),
    /// Banyan operations (`StoreError`), or persistence (`IOError`, `SerializationError`).
    pub fn extend(&mut self, records: &[Record]) -> Result<()> {
        let batch_size = records.len();
        if batch_size == 0 {
            debug!("Extend called with empty records slice, doing nothing.");
            return Ok(());
        }
        let batch_size_u32 = batch_size as u32; // Safe cast after checking > 0

        let extend_span = tracing::info_span!("extend_datastream", batch_size).entered();
        trace!("Starting extend operation");

        // Capture current state before modification
        let start_offset = self.total_rows;
        let current_root_cid = self.root_cid;

        // --- 1. Compress Batch to RowSeqData ---
        let compress_span = tracing::debug_span!("compress_batch").entered();
        let row_seq_value = match compression::compress_row_seq_data(records, &self.schema) {
            Ok(data) => data,
            Err(e) => {
                error!(error = ?e, "Failed to compress record batch");
                return Err(e);
            }
        };
        drop(compress_span);
        trace!(
            chunk_rows = row_seq_value.num_rows,
            num_columns = row_seq_value.columns.len() + 1, // +1 for timestamp
            "Compressed batch into RowSeqData"
        );
        // Sanity check compressed rows vs batch size
        if row_seq_value.num_rows != batch_size_u32 {
            error!(
                expected = batch_size_u32,
                actual = row_seq_value.num_rows,
                "Compressed RowSeqData num_rows mismatch with input batch size"
            );
            return Err(BanyanRowSeqError::CompressionError(
                "Compressed row count mismatch".to_string(),
            ));
        }

        // --- 2. Calculate ChunkKey ---
        // Requires iterating through records again to find min/max timestamp.
        // Optimization: compress_row_seq_data could potentially return this info.
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut found_timestamp = false;
        for record in records {
            // Assume 'timestamp' column exists and is correct type (validated by compression)
            if let Some(Some(Value::Timestamp(ts))) = record.get("timestamp") {
                min_ts = min_ts.min(*ts);
                max_ts = max_ts.max(*ts);
                found_timestamp = true;
            } else {
                // This case should have been caught by compression if timestamps are mandatory non-null
                warn!("Record missing expected timestamp during ChunkKey calculation (should have failed compression)");
                // Handle defensively - maybe use a default? Or error?
                // For now, let the default min/max stand if NO timestamps found.
            }
        }

        // Handle case where batch had 0 valid timestamps (should not happen if mandatory)
        if !found_timestamp {
            warn!(batch_size, "No valid timestamps found in batch to calculate ChunkKey time range. Using default [0, 0].");
            min_ts = 0; // Or some other sentinel value?
            max_ts = 0;
        }

        let key = ChunkKey {
            start_offset, // Starts after the last existing row
            count: batch_size_u32,
            min_timestamp_micros: min_ts,
            max_timestamp_micros: max_ts,
        };
        trace!(?key, "Calculated ChunkKey for new batch");

        // 3. Perform Banyan Transaction
        let banyan_txn_span = tracing::debug_span!("banyan_transaction").entered();
        let mut txn = Transaction::new(self.forest.clone(), self.store.clone());

        // Load or create the StreamBuilder
        let mut builder = match self.root_cid {
            Some(cid_val) => {
                let link = Sha256Digest::try_from(cid_val)?;
                trace!(%cid_val, %link, "Loading existing StreamBuilder");
                txn.load_stream_builder(self.secrets.clone(), self.config.clone(), link)?
            }
            None => {
                trace!("Creating new StreamBuilder");
                StreamBuilder::new(self.config.clone(), self.secrets.clone())
            }
        };

        // Extend the builder - use unpacked for faster appends, pack later if needed
        trace!("Extending Banyan StreamBuilder (unpacked)");
        txn.extend_unpacked(&mut builder, vec![(key, row_seq_value)])?;

        // Get the new root link (digest) and update state
        if let Some(new_link) = builder.link() {
            let new_cid = Cid::from(new_link);
            trace!(%new_cid, "Banyan tree updated");
            self.root_cid = Some(new_cid);
        } else {
            // This case might happen if extend resulted in an empty tree somehow? Or initial build.
            warn!("StreamBuilder link was None after extend");
            self.root_cid = None;
        }
        drop(banyan_txn_span);

        // Update total rows
        self.total_rows = start_offset + batch_size as u64;
        debug!(new_total_rows = self.total_rows, "Updated total rows");

        // 4. Persist State (optional, based on config)
        if self.persistence_path.is_some() {
            let save_span = tracing::debug_span!("save_state").entered();
            if let Err(e) = self.save_state() {
                error!("Failed to save state after extend: {}", e);
                // Decide if this should be a hard error
            }
            drop(save_span);
        }

        drop(extend_span);
        Ok(())
    }

    /// Queries the datastream for rows matching the specified criteria.
    ///
    /// # Arguments
    ///
    /// * `requested_columns` - A `Vec<String>` of column names to include in the results.
    ///                         If empty, resulting `Record`s will be empty.
    /// * `offset_range` - A tuple defining the desired absolute row offset range.
    ///                    Uses `std::ops::Bound` (Included, Excluded, Unbounded).
    /// * `filters` - A `Vec<UserFilter>` specifying row-level value predicates to apply.
    /// * `time_range_micros` - A tuple defining the desired timestamp range (microseconds UTC).
    ///                         Uses `std::ops::Bound`.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `RowSeqResultIterator` that will yield the matching `Record`s,
    /// or a `BanyanRowSeqError` if the query cannot be initiated (e.g., empty datastream,
    /// invalid query parameters).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Assuming `ds` is a DatastreamRowSeq instance
    /// let results = ds.query(
    ///     vec!["timestamp".to_string(), "temperature".to_string()],
    ///     (Bound::Included(1000), Bound::Excluded(2000)), // Offsets 1000..2000
    ///     vec![UserFilter {
    ///         column_name: "status".to_string(),
    ///         operator: UserFilterOp::Equals,
    ///         value: Value::Enum(0), // Assuming status is enum, index 0
    ///     }],
    ///     (Bound::Unbounded, Bound::Included(some_timestamp_micros)), // Up to a certain time
    /// )?;
    ///
    /// for result_record in results {
    ///     match result_record {
    ///         Ok(record) => println!("{:?}", record),
    ///         Err(e) => eprintln!("Error reading row: {}", e),
    ///     }
    /// }
    /// ```
    pub fn query(
        &self,
        requested_columns: Vec<String>,
        offset_range: (Bound<u64>, Bound<u64>),
        filters: Vec<UserFilter>, // Already using Vec<UserFilter>
        time_range_micros: (Bound<i64>, Bound<i64>),
    ) -> Result<RowSeqResultIterator<S>> {
        // Return the specific iterator type

        let query_span = tracing::info_span!(
            "query_datastream",
            ?requested_columns,
            ?offset_range,
            num_filters = filters.len(),
            ?time_range_micros
        )
        .entered();

        let root_cid = self.root_cid.ok_or(BanyanRowSeqError::InvalidQuery(
            "Cannot query an empty datastream (no root CID)".to_string(),
        ))?;

        // --- 1. Load the Banyan Tree ---
        // This accesses the store but doesn't require a full transaction.
        let tree: Tree<RowSeqTT, RowSeqData> = self
            .forest
            .load_tree(self.secrets.clone(), Sha256Digest::try_from(root_cid)?)
            .map_err(|e| {
                BanyanRowSeqError::StoreError(anyhow!(
                    "Failed to load Banyan tree with root {}: {}",
                    root_cid,
                    e
                ))
            })?;

        trace!(%root_cid, tree_count = tree.count(), "Loaded Banyan tree for query");
        if tree.is_empty() {
            debug!("Querying an empty Banyan tree (root exists but no elements). Returning empty iterator.");
            // Although root_cid exists, the tree might be logically empty.
            // Create an empty iterator immediately.
            // We can achieve this by providing an empty Vec to the Box::new iterator below.
            // Or, more cleanly, handle it in the RowSeqResultIterator::new or fill_buffer.
            // Let's let the standard iterator handle it - it will find no chunks.
        }

        // --- 2. Create Banyan-Level Query ---
        // This query prunes based on ChunkKey/ChunkSummary ranges.
        let banyan_query = RowSeqBanyanQuery {
            offset_range: offset_range.clone(), // Clone bounds for the query struct
            time_range_micros: time_range_micros.clone(),
        };
        debug!(
            ?banyan_query,
            "Constructed Banyan-level query for tree traversal"
        );

        // --- 3. Get Banyan Chunk Iterator ---
        // Use iter_filtered_chunked to get chunks passing the banyan_query.
        // The iterator yields Result<FilteredChunk<...>, _>
        // The `()` in FilteredChunk is the 'extra' data type (E) from the query closure, unused here.
        // Box the concrete iterator type returned by banyan into a trait object.
        let chunk_iter_results = self.forest.iter_filtered_chunked::<
            _,          // Q: Query type (RowSeqBanyanQuery)
            RowSeqData, // V: Value type in leaves
            (),         // E: Extra data type from query closure (unused)
            _           // F: Closure type (inferred)
        >(
            &tree,
            banyan_query, // Pass the Banyan-level query
            &|_node_info| (), // Provide a dummy closure for extra data (E = ())
        );

        // The iter_filtered_chunked returns the iterator directly, not a Result.
        // The items *within* the iterator are Results.
        let chunk_iter: Box<
            dyn Iterator<Item = anyhow::Result<FilteredChunk<(u64, ChunkKey, RowSeqData), ()>>>
                + Send,
        > = Box::new(chunk_iter_results);
        trace!("Obtained Banyan chunk iterator");

        // --- 4. Calculate Absolute Offset Bounds for Result Iterator ---
        // These define the precise range the *final* iterator should yield.
        // The Banyan query might yield chunks that *overlap* this range.
        let query_start_offset_inclusive = match offset_range.0 {
            Bound::Included(s) => s,
            Bound::Excluded(s) => s.saturating_add(1),
            Bound::Unbounded => 0, // Start from the beginning if unbounded
        };
        // The iterator needs an *exclusive* end offset for its internal logic
        let query_end_offset_exclusive = match offset_range.1 {
            Bound::Included(e) => e.saturating_add(1), // End after the included offset
            Bound::Excluded(e) => e,                   // End before the excluded offset
            Bound::Unbounded => self.total_rows,       // End after the last known row if unbounded
        };
        debug!(
            query_start_offset = query_start_offset_inclusive,
            query_end_offset_exclusive, "Calculated absolute offset bounds for result iterator"
        );

        // --- 5. Create and Return the RowSeqResultIterator ---
        // This iterator handles decompression, row reconstruction, and final filtering.
        let result_iterator = RowSeqResultIterator::new(
            self.schema.clone(),
            chunk_iter, // Pass the boxed Banyan iterator
            requested_columns,
            filters,
            query_start_offset_inclusive, // Pass the calculated bounds
            query_end_offset_exclusive,
            time_range_micros.clone(), // Pass the time range for final filtering
        )?; // Propagate errors from iterator::new (e.g., column validation)

        trace!("Created RowSeqResultIterator to handle results");

        drop(query_span); // Drop span before returning the iterator
        Ok(result_iterator)
    }

    // TODO: Add pack() method?
    // pub fn pack(&mut self) -> Result<()> { ... txn.pack(&builder)? ... }
    // This would force consolidation of tree nodes, potentially improving query
    // performance but incurring write cost. Might be useful periodically or manually.
}
