//! Query logic for Banyan tree traversal and row-level filtering.
use crate::{
    error::{BanyanRowSeqError, Result},
    types::{ChunkKey, ChunkSummary, Record, RowSeqTT, UserFilter, UserFilterOp, Value},
};
use banyan::{
    index::{BranchIndex, CompactSeq, LeafIndex},
    query::Query, // Import the Banyan Query trait
};
use std::ops::Bound;
use tracing::{debug, trace, warn}; // Added trace, warn

/// The query structure executed during Banyan tree traversal.
///
/// This query primarily uses offset and timestamp ranges defined in `ChunkKey`
/// and `ChunkSummary` to prune branches of the tree that cannot possibly
/// contain relevant data.
///
/// **TODO:** Enhance this query to leverage potential future value summaries
/// (min/max, bloom filters) stored in `ChunkSummary` for more aggressive pruning.
#[derive(Debug, Clone)]
pub struct RowSeqBanyanQuery {
    /// The desired absolute row offset range (inclusive start, exclusive end).
    pub offset_range: (Bound<u64>, Bound<u64>),
    /// The desired timestamp range (microseconds UTC, inclusive bounds).
    pub time_range_micros: (Bound<i64>, Bound<i64>),
    // Potential future fields:
    // pub value_filters_summary: PrecomputedSummaryFilters,
}

/// Helper function to check if two potentially unbounded ranges overlap.
///
/// This is crucial for determining if a query range intersects with the
/// range covered by a Banyan tree node (leaf key or branch summary).
fn ranges_intersect<T: PartialOrd>(
    r1_start: Bound<T>,
    r1_end: Bound<T>,
    r2_start: Bound<T>,
    r2_end: Bound<T>,
) -> bool {
    let r1_before_r2 = match (&r1_end, &r2_start) {
        (Bound::Included(e1), Bound::Included(s2)) => e1 < s2,
        (Bound::Excluded(e1), Bound::Included(s2)) => e1 <= s2,
        (Bound::Included(e1), Bound::Excluded(s2)) => e1 <= s2,
        (Bound::Excluded(e1), Bound::Excluded(s2)) => e1 <= s2,
        (_, Bound::Unbounded) => false,
        (Bound::Unbounded, _) => false,
    };
    let r2_before_r1 = match (&r2_end, &r1_start) {
        (Bound::Included(e2), Bound::Included(s1)) => e2 < s1,
        (Bound::Excluded(e2), Bound::Included(s1)) => e2 <= s1,
        (Bound::Included(e2), Bound::Excluded(s1)) => e2 <= s1,
        (Bound::Excluded(e2), Bound::Excluded(s1)) => e2 <= s1,
        (_, Bound::Unbounded) => false,
        (Bound::Unbounded, _) => false,
    };
    !r1_before_r2 && !r2_before_r1
}

impl Query<RowSeqTT> for RowSeqBanyanQuery {
    /// Called at leaf nodes. Determines if the chunk represented by this leaf
    /// *might* contain data relevant to the query.
    ///
    /// It checks if the chunk's offset and time range (from `ChunkKey`) overlaps
    /// with the query's ranges. If they don't overlap, all elements (`res` bits)
    /// corresponding to this leaf are set to `false`, pruning the entire chunk.
    fn containing(
        &self,
        _offset: u64,
        index: &banyan::index::LeafIndex<RowSeqTT>,
        res: &mut [bool],
    ) {
        // A leaf index corresponds to a single RowSeqData chunk.
        // The key contains the necessary range info.
        if index.keys.is_empty() {
            warn!("RowSeqBanyanQuery::containing called on leaf with empty keys!");
            res.fill(false); // Mark all as non-matching
            return;
        }
        // Get the ChunkKey for this leaf
        let key: ChunkKey = index.keys.first();

        // Check if the *chunk's* ranges overlap with the *query's* ranges
        let chunk_offset_start = Bound::Included(key.start_offset);
        // Exclusive end offset for the chunk
        let chunk_offset_end = Bound::Excluded(key.start_offset.saturating_add(key.count as u64));
        let chunk_time_start = Bound::Included(key.min_timestamp_micros);
        let chunk_time_end = Bound::Included(key.max_timestamp_micros); // Summary range is inclusive

        let offset_intersects = ranges_intersect(
            chunk_offset_start,
            chunk_offset_end,
            self.offset_range.0,
            self.offset_range.1,
        );
        let time_intersects = ranges_intersect(
            chunk_time_start,
            chunk_time_end,
            self.time_range_micros.0,
            self.time_range_micros.1,
        );

        // TODO: Add checks against value summaries here if they exist in ChunkSummary/Key

        let keep_chunk = offset_intersects && time_intersects; // && value_summary_intersects;

        trace!(leaf_key = ?key, ?offset_intersects, ?time_intersects, %keep_chunk, "BanyanQuery::containing check");

        // *** ADDED BANYAN QUERY LOGGING ***
        debug!(
            query_offset=?self.offset_range,
            query_time=?self.time_range_micros,
            leaf_offset=_offset, // Banyan's leaf offset (might differ from key.start_offset if tree structure changes?)
            leaf_key = ?key,    // The key defining the chunk's range
            ?offset_intersects,
            ?time_intersects,
            %keep_chunk,
            "BanyanQuery::containing decision"
        );

        // If the chunk key/summary doesn't intersect, prune all elements within it.
        // Otherwise, keep the existing `res` bits (set by `intersecting` on parent).
        // The final row-level filtering happens in the iterator.
        if !keep_chunk {
            res.fill(false);
        }
        // No need to iterate res elements here, containing applies to the whole leaf index.
    }

    /// Called at branch nodes. Determines which child branches *might* contain
    /// relevant data based on their `ChunkSummary`.
    ///
    /// It iterates through the summaries of child nodes. For each child whose
    /// corresponding `res` bit is currently `true`, it checks if the child's
    /// summarized offset and time range overlaps with the query's ranges.
    /// If they don't overlap, the child's `res` bit is set to `false`, pruning
    /// that entire subtree.
    fn intersecting(&self, _offset: u64, index: &BranchIndex<RowSeqTT>, res: &mut [bool]) {
        // Iterate through child summaries and their corresponding result bits
        for (i, summary) in index.summaries.as_ref().iter().enumerate() {
            // Only check children that haven't already been pruned by ancestor checks
            if res.get(i).map_or(false, |&r| r) {
                // Define the summary's ranges
                let summary_offset_start = Bound::Included(&summary.start_offset);
                // Summary end is exclusive: start + total_count
                let summary_offset_end_val =
                    summary.start_offset.saturating_add(summary.total_count);
                let summary_offset_end = Bound::Excluded(&summary_offset_end_val);

                let summary_time_start = Bound::Included(&summary.min_timestamp_micros);
                let summary_time_end = Bound::Included(&summary.max_timestamp_micros); // Summary range inclusive

                // Check intersection with query ranges
                let offset_intersects = ranges_intersect(
                    summary_offset_start,
                    summary_offset_end,
                    self.offset_range.0.as_ref(),
                    self.offset_range.1.as_ref(),
                );
                let time_intersects = ranges_intersect(
                    summary_time_start,
                    summary_time_end,
                    self.time_range_micros.0.as_ref(),
                    self.time_range_micros.1.as_ref(),
                );

                // TODO: Add checks against value summaries here

                let keep_child = offset_intersects && time_intersects;

                // Log the decision for this child branch
                trace!(
                    branch_banyan_offset = _offset,
                    child_idx = i,
                    child_summary = ?summary,
                    query_offset = ?self.offset_range,
                    query_time = ?self.time_range_micros,
                    offset_match = offset_intersects,
                    time_match = time_intersects,
                    keep = keep_child,
                    "BanyanQuery::intersecting check"
                );

                // If the child summary's range doesn't intersect, prune it by setting res[i] = false
                if !keep_child {
                    // Safely set the bit if the index is valid
                    if let Some(r) = res.get_mut(i) {
                        *r = false;
                    } else {
                        warn!(
                            child_idx = i,
                            res_len = res.len(),
                            "Child index out of bounds for res slice in intersecting"
                        );
                    }
                }
                // If keep_child is true, res[i] remains true, indicating we should descend into this child.
            } else {
                // This child was already pruned by a higher-level check
                trace!(child_idx = i, child_summary = ?summary, "Skipping already pruned child branch");
            }
        }
    }
}

// --- Row-Level Filtering ---

/// Applies user-defined value filters to a single, fully reconstructed row (`Record`).
///
/// This function is called by the result iterator *after* a row has been
/// reconstructed from the decompressed columnar data. It checks if the row
/// satisfies ALL provided `UserFilter` conditions (AND logic).
///
/// # Arguments
/// * `row` - The reconstructed row (`BTreeMap<String, Option<Value>>`) to filter.
/// * `filters` - A slice of `UserFilter` conditions to apply.
///
/// # Returns
/// `true` if the row satisfies all filters (or if filters is empty), `false` otherwise.
pub fn apply_user_filters(row: &Record, filters: &[UserFilter]) -> bool {
    if filters.is_empty() {
        return true; // No filters means the row passes automatically
    }

    // Use `all()` for AND logic: every filter must return true
    filters.iter().all(|filter| {
        match row.get(&filter.column_name) {
            Some(Some(row_value)) => {
                // Column exists and has a non-null value
                let matches = compare_values(row_value, &filter.value, &filter.operator);
                trace!(filter = ?filter, ?row_value, matches, "Applying value filter");
                matches
            }
            Some(None) => {
                // Column exists but is NULL. Standard comparisons with NULL usually fail.
                // TODO: Add specific IsNull/IsNotNull operators if needed.
                trace!(filter = ?filter, row_value = "None", matches = false, "Applying value filter to NULL");
                false
            }
            None => {
                // Column name in filter doesn't exist in the row (e.g., wasn't requested or doesn't exist in schema)
                // This should generally not happen if validation is done correctly upstream,
                // but handle defensively: a filter on a non-existent column fails.
                warn!(filter_column = %filter.column_name, "Filter applied to column not present in the reconstructed row");
                false
            }
        }
    })
}

/// Compares two `Value` instances based on the specified operator.
///
/// Handles basic type promotions (e.g., Integer to Float) for comparisons.
/// Returns `false` for comparisons between incompatible types.
fn compare_values(left: &Value, right: &Value, op: &UserFilterOp) -> bool {
    match op {
        // Equality checks handle type mismatch implicitly via PartialEq
        UserFilterOp::Equals => left == right,
        UserFilterOp::NotEquals => left != right,

        // Ordered comparisons require type matching or promotion
        UserFilterOp::GreaterThan => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l > r,
            (Value::Float(l), Value::Float(r)) => l > r,
            (Value::Timestamp(l), Value::Timestamp(r)) => l > r,
            (Value::String(l), Value::String(r)) => l > r,
            (Value::Enum(l), Value::Enum(r)) => l > r, // Compare enum indices
            // Promotions
            (Value::Float(l), Value::Integer(r)) => *l > (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) > *r,
            // Incompatible types for '>'
            _ => false,
        },
        UserFilterOp::GreaterThanOrEqual => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l >= r,
            (Value::Float(l), Value::Float(r)) => l >= r,
            (Value::Timestamp(l), Value::Timestamp(r)) => l >= r,
            (Value::String(l), Value::String(r)) => l >= r,
            (Value::Enum(l), Value::Enum(r)) => l >= r,
            // Promotions
            (Value::Float(l), Value::Integer(r)) => *l >= (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) >= *r,
            _ => false,
        },
        UserFilterOp::LessThan => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l < r,
            (Value::Float(l), Value::Float(r)) => l < r,
            (Value::Timestamp(l), Value::Timestamp(r)) => l < r,
            (Value::String(l), Value::String(r)) => l < r,
            (Value::Enum(l), Value::Enum(r)) => l < r,
            // Promotions
            (Value::Float(l), Value::Integer(r)) => *l < (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) < *r,
            _ => false,
        },
        UserFilterOp::LessThanOrEqual => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => l <= r,
            (Value::Float(l), Value::Float(r)) => l <= r,
            (Value::Timestamp(l), Value::Timestamp(r)) => l <= r,
            (Value::String(l), Value::String(r)) => l <= r,
            (Value::Enum(l), Value::Enum(r)) => l <= r,
            // Promotions
            (Value::Float(l), Value::Integer(r)) => *l <= (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) <= *r,
            _ => false,
        },
    }
}
