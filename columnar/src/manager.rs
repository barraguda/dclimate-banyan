// --- High-Level Manager ---
use anyhow::Result;
use banyan::{
    store::{BlockWriter, BranchCache, ReadOnlyStore},
    Config, Forest, Secrets, StreamBuilder, Transaction, Tree,
};
use banyan_utils::tags::Sha256Digest;
use libipld::Cid;
use serde::{de::Error as SerdeDeError, Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::Bound, path::Path, sync::Arc};
use tracing::{debug, error, info, trace, warn};

use crate::types::{ColumnType, ColumnarError, DataDefinition, Record, Value};

use super::compression;
use super::iterator::ColumnarResultIterator;
use super::query::ValueFilter;
use super::types::{ColumnChunk, ColumnarTreeTypes, RichRangeKey};
use serde_json;
use std::path::PathBuf;

/// Define a concrete store trait alias for convenience
pub trait ColumnarBanyanStore:
    ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
{
}
impl<S> ColumnarBanyanStore for S where
    S: ReadOnlyStore<Sha256Digest> + BlockWriter<Sha256Digest> + Clone + Send + Sync + 'static
{
}

// --- Persistence Helper ---
// fix if better ergonomics found.
mod serde_link_option {
    // Use super:: to access types from the manager module scope or crate root
    use super::{Cid, SerdeDeError, Sha256Digest};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(link_opt: &Option<Sha256Digest>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match link_opt {
            Some(link) => serializer.serialize_some(&Cid::from(*link).to_string()),
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
                // Add hash check if needed later
                let digest = Sha256Digest::try_from(cid).map_err(SerdeDeError::custom)?;
                Ok(Some(digest))
            }
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct SerializableLinkOption(#[serde(with = "serde_link_option")] Option<Sha256Digest>);

// State to be persisted
#[derive(Serialize, Deserialize)]
struct ManagerState {
    // Store DataDefinition directly
    data_definition: DataDefinition,
    // Map column name to optional Link (CID)
    column_cids: BTreeMap<String, SerializableLinkOption>,
    total_rows: u64,
}

/// TODO: Docs
pub struct ColumnarDatastreamManager<S: ColumnarBanyanStore> {
    store: S,
    forest: Forest<ColumnarTreeTypes, S>,
    /// TODO docs
    pub data_definition: Arc<DataDefinition>, // Keep Arc for internal sharing
    // Builders are transient, recreated on load or extension
    // TODO: #[serde(skip)]
    // column_builders: BTreeMap<String, StreamBuilder<ColumnarTreeTypes, ColumnChunk>>,
    // CIDs are the persistent state of the trees
    /// TODO: Docs
    pub column_cids: BTreeMap<String, Option<Sha256Digest>>,
    /// TODO: Docs
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
                match Self::load_from_path(store.clone(), config.clone(), secrets.clone(), path) {
                    Ok(manager) => {
                        info!("Successfully loaded manager state.");
                        return Ok(manager);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to load state from {:?}: {}. Proceeding with initialization.",
                            path, e
                        );
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
            ColumnarError::PersistenceError(format!("Failed to open state file {:?}: {}", path, e))
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
            info!("Extend called with 0 records. No changes made.");
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
        debug!(key = ?key, "Created RichRangeKey for batch");

        // --- Banyan Transaction ---
        // Create a transaction context holding the store for reading and writing.
        let mut txn = Transaction::new(self.forest.clone(), self.store.clone());
        // Store the CIDs resulting from this operation temporarily.
        let mut latest_cids_this_batch = BTreeMap::new();

        for col_def in &self.data_definition.columns {
            let column_name = &col_def.name;
            trace!(column = %column_name, "Processing column for extend");

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
                column = %column_name,
                new_cid = ?new_cid,
                "Obtained new CID after extend_unpacked"
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
                    return Err(ColumnarError::ColumnNotFound(filter.column_name.clone()).into());
                    // Qualify
                }
                needed_columns.push(filter.column_name.clone());
            }
        }
        needed_columns.sort();
        needed_columns.dedup();
        debug!(columns = ?needed_columns, "Query needs columns (requested + filter cols)");

        let banyan_query = super::query::ColumnarBanyanQuery {
            // Qualify
            offset_range: offset_range.clone(),
            time_range_micros: time_range_micros.clone(), // Clone time range for banyan query
        };
        debug!(query = ?banyan_query, "Created Banyan Query");

        let mut column_chunk_iters = BTreeMap::new();
        let txn = Transaction::new(self.forest.clone(), self.store.clone());

        for col_name in &needed_columns {
            trace!("Setting up iterator for column: {}", col_name);
            let cid = self.column_cids.get(col_name).cloned().flatten();
            debug!(column = %col_name, cid = ?cid, "Loading CID for column query");

            match cid {
                Some(cid) => {
                    trace!("  Loading tree for CID: {}", cid);
                    // Qualify types::* and ColumnChunk
                    let tree: Tree<super::types::ColumnarTreeTypes, super::types::ColumnChunk> =
                        txn.load_tree(self.secrets.clone(), cid)?;

                    let iter =
                        self.forest
                            .iter_filtered_chunked(&tree, banyan_query.clone(), &|_| ());

                    column_chunk_iters.insert(col_name.clone(), iter.peekable());
                }
                None => {
                    warn!(
                        "Query involves column '{}' which has no data yet (CID is None).",
                        col_name
                    );
                    return Err(ColumnarError::ColumnNotInitialized(col_name.clone()).into());
                }
            }
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
