use crate::{
    // Import types needed for both row-based and columnar queries
    codec::{SummaryValue, TreeKey, TreeSummary, TreeType, TreeValue},
    // Removed TreeType import as it's specific to row-based
    columnar::{ColumnarSummary, ColumnarTT}, // Import ColumnarTT for trait impl
    error::Result,        // Keep Result if needed by methods
};
use banyan::{
    index::{BranchIndex, LeafIndex}, // Keep necessary index types
    query::Query as BanyanApiQuery,  // Alias banyan's Query trait
};
use std::cmp::{max, min, Ordering};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Comparison {
    LT, LE, EQ, NE, GE, GT,
}
pub(crate) use Comparison::*;

// --- User-Facing Query Structure ---
// This structure remains the same for the user to build queries.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    // Disjunctive Normal Form (OR of ANDs)
    clauses: Vec<QueryAnd>,
}

impl Query {
    // Constructor remains public
    pub fn new(position: usize, operator: Comparison, value: TreeValue) -> Query {
        let query = QueryCol {
            position,
            operator,
            value,
        };
        let query = QueryAnd {
            clauses: vec![query],
        };
        Query {
            clauses: vec![query],
        }
    }

    // Keep and/or public, return Result for potential errors during combination?
    // For simplicity, keeping them infallible for now.
    pub fn and(self, other: Query) -> Self {
        let mut or_clauses = vec![];
        for left in &self.clauses {
            for right in &other.clauses {
                let mut and_clauses = left.clauses.clone();
                and_clauses.extend(right.clauses.clone());
                or_clauses.push(QueryAnd {
                    clauses: and_clauses,
                });
            }
        }
        Self {
            clauses: or_clauses,
        }
    }

    pub fn or(mut self, other: Query) -> Self {
        self.clauses.extend(other.clauses);
        self
    }

    // --- Evaluation Helpers (pub(crate)) ---
    // Evaluate against a single row (TreeKey = Row for row-based)
    pub(crate) fn eval_scalar(&self, value: &crate::codec::Row) -> bool {
        self.clauses.iter().any(|clause| clause.eval_scalar(value))
    }

    // Evaluate against a row-based summary
    pub(crate) fn eval_summary(&self, summary: &TreeSummary) -> bool {
        self.clauses
            .iter()
            .any(|clause| clause.eval_summary(summary))
    }

    // Evaluate against a columnar summary
    pub(crate) fn eval_columnar_summary(&self, summary: &ColumnarSummary) -> bool {
        self.clauses
            .iter()
            .any(|clause| clause.eval_columnar_summary(summary))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QueryAnd {
    clauses: Vec<QueryCol>,
}

impl QueryAnd {
    // Evaluate against a single row (TreeKey = Row for row-based)
    pub(crate) fn eval_scalar(&self, value: &crate::codec::Row) -> bool {
        self.clauses.iter().all(|clause| clause.eval_scalar(value))
    }
    // Evaluate against a row-based summary
    pub(crate) fn eval_summary(&self, summary: &TreeSummary) -> bool {
        self.clauses
            .iter()
            .all(|clause| clause.eval_summary(summary))
    }
    // Evaluate against a columnar summary
    pub(crate) fn eval_columnar_summary(&self, summary: &ColumnarSummary) -> bool {
        self.clauses
            .iter()
            .all(|clause| clause.eval_columnar_summary(summary))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct QueryCol {
    operator: Comparison,
    position: usize,
    value: TreeValue,
}

impl QueryCol {
    // Evaluate against a single row (TreeKey = Row for row-based)
    pub(crate) fn eval_scalar(&self, row: &crate::codec::Row) -> bool {
        match row.get(self.position) {
            Some(value) => self.compare_value(value),
            None => self.operator == NE, // Treat missing as not equal only if NE
        }
    }
    // Evaluate against a row-based summary
    pub(crate) fn eval_summary(&self, summary: &TreeSummary) -> bool {
        match summary.get(self.position) {
            Some(summary_val) => self.compare_summary(summary_val),
            None => false, // Column not present in summary range
        }
    }
    // Evaluate against a columnar summary
    pub(crate) fn eval_columnar_summary(&self, summary: &ColumnarSummary) -> bool {
        match summary.get_column_summary(self.position) {
            Some(summary_val) => self.compare_summary(summary_val),
            None => false, // Column not present in this columnar summary
        }
    }

    // Helper for comparing a TreeValue against the query's value
    fn compare_value(&self, value: &TreeValue) -> bool {
        match self.operator {
            LE => *value <= self.value, LT => *value < self.value,
            EQ => *value == self.value, NE => *value != self.value,
            GT => *value > self.value,  GE => *value >= self.value,
        }
    }

    // Helper for comparing a SummaryValue against the query's value
    fn compare_summary(&self, summary_val: &SummaryValue) -> bool {
        match self.operator {
            LE => *summary_val <= self.value, LT => *summary_val < self.value,
            EQ => *summary_val == self.value, NE => summary_val.not_contains_only(&self.value),
            GT => *summary_val > self.value,  GE => *summary_val >= self.value,
        }
    }
}

// --- PartialOrd/PartialEq implementations (keep as before) ---
// (Ensure these are correct and handle type mismatches gracefully)
impl PartialOrd for TreeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (TreeValue::Timestamp(s), TreeValue::Timestamp(o)) => s.partial_cmp(o),
            (TreeValue::Integer(s), TreeValue::Integer(o)) => s.partial_cmp(o),
            (TreeValue::Float(s), TreeValue::Float(o)) => s.partial_cmp(o),
            (TreeValue::String(s), TreeValue::String(o)) => s.partial_cmp(o),
            (TreeValue::Enum(s), TreeValue::Enum(o)) => s.partial_cmp(o),
            _ => None, // Incomparable types
        }
    }
}

impl PartialEq<TreeValue> for SummaryValue {
     fn eq(&self, other: &TreeValue) -> bool {
        match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => !(o < &range.lhs || o > &range.rhs),
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => !(o < &range.lhs || o > &range.rhs),
            (SummaryValue::Float(range), TreeValue::Float(o)) => !(o < &range.lhs || o > &range.rhs),
            (SummaryValue::String(range), TreeValue::String(o)) => !(o < &range.lhs || o > &range.rhs),
            _ => false, // Different types or Enum involved
        }
    }
}

impl SummaryValue {
    pub(crate) fn not_contains_only(&self, other: &TreeValue) -> bool {
         match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => !(o == &range.lhs && o == &range.rhs),
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => !(o == &range.lhs && o == &range.rhs),
            (SummaryValue::Float(range), TreeValue::Float(o)) => !(o == &range.lhs && o == &range.rhs),
            (SummaryValue::String(range), TreeValue::String(o)) => !(o == &range.lhs && o == &range.rhs),
            _ => true, // Different types cannot be equal
        }
    }
}


impl PartialOrd<TreeValue> for SummaryValue {
    fn partial_cmp(&self, _other: &TreeValue) -> Option<Ordering> {
        // This direct comparison is ill-defined for ranges vs single values.
        // We rely on the specific lt, le, gt, ge methods below.
        None
    }

    // Does *any part* of the summary range satisfy "less than other"?
    // True if the lower bound of the range is less than the value.
    fn lt(&self, other: &TreeValue) -> bool {
        match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => range.lhs < *o,
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => range.lhs < *o,
            (SummaryValue::Float(range), TreeValue::Float(o)) => range.lhs < *o,
            (SummaryValue::String(range), TreeValue::String(o)) => range.lhs < *o,
            _ => false, // Type mismatch or Enum
        }
    }

    // Does *any part* of the summary range satisfy "less than or equal to other"?
    // True if the lower bound of the range is less than or equal to the value.
    fn le(&self, other: &TreeValue) -> bool {
        match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => range.lhs <= *o,
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => range.lhs <= *o,
            (SummaryValue::Float(range), TreeValue::Float(o)) => range.lhs <= *o,
            (SummaryValue::String(range), TreeValue::String(o)) => range.lhs <= *o,
             _ => false,
        }
    }

    // Does *any part* of the summary range satisfy "greater than other"?
    // True if the upper bound of the range is greater than the value.
    fn gt(&self, other: &TreeValue) -> bool {
         match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => range.rhs > *o,
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => range.rhs > *o,
            (SummaryValue::Float(range), TreeValue::Float(o)) => range.rhs > *o,
            (SummaryValue::String(range), TreeValue::String(o)) => range.rhs > *o,
             _ => false,
        }
    }

    // Does *any part* of the summary range satisfy "greater than or equal to other"?
    // True if the upper bound of the range is greater than or equal to the value.
    fn ge(&self, other: &TreeValue) -> bool {
         match (self, other) {
            (SummaryValue::Timestamp(range), TreeValue::Timestamp(o)) => range.rhs >= *o,
            (SummaryValue::Integer(range), TreeValue::Integer(o)) => range.rhs >= *o,
            (SummaryValue::Float(range), TreeValue::Float(o)) => range.rhs >= *o,
            (SummaryValue::String(range), TreeValue::String(o)) => range.rhs >= *o,
             _ => false,
        }
    }
}
// 


#[derive(Clone, Debug)]
pub(crate) struct BanyanQuery {
    pub query: Option<Query>,
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

impl banyan::query::Query<TreeType> for BanyanQuery {
    fn containing(
        &self,
        offset: u64,
        index: &banyan::index::LeafIndex<TreeType>,
        res: &mut [bool],
    ) {
        let range_start = match self.range_start {
            Some(range_start) => max(range_start, offset),
            None => offset,
        };
        let range_end = match self.range_end {
            Some(range_end) => min(range_end, offset + res.len() as u64),
            None => offset + res.len() as u64,
        };

        for (i, key) in index.keys.as_ref().iter().enumerate() {
            let index = offset + i as u64;
            let in_range = !(index < range_start || index >= range_end);
            res[i] = in_range
                && match &self.query {
                    Some(query) => query.eval_scalar(key),
                    None => true,
                };
        }
    }

    fn intersecting(
        &self,
        offset: u64,
        index: &banyan::index::BranchIndex<TreeType>,
        res: &mut [bool],
    ) {
        let range_start = match self.range_start {
            Some(range_start) => max(range_start, offset),
            None => offset,
        };
        let range_end = match self.range_end {
            Some(range_end) => min(range_end, offset + index.count),
            None => offset + index.count,
        };

        let mut start_index = offset;
        for (i, summary) in index.summaries.as_ref().iter().enumerate() {
            let end_index = start_index + summary.rows;
            let in_range = !(end_index <= range_start || start_index >= range_end);

            res[i] = in_range
                && match &self.query {
                    Some(query) => query.eval_summary(summary),
                    None => true,
                };
            start_index = end_index;
        }
    }
}


// --- Banyan Query Implementation for Columnar ---
// This struct wraps the user query and offset range for Banyan's traversal.
#[derive(Clone, Debug)]
pub(crate) struct ColumnarBanyanQuery {
    pub query: Option<Query>, // User's column query
    pub range_start: Option<u64>, // Logical row offset start
    pub range_end: Option<u64>,   // Logical row offset end (exclusive)
}

// Implement Banyan's Query trait for the *Columnar* TreeType
impl BanyanApiQuery<ColumnarTT> for ColumnarBanyanQuery {
    /// Filter leaf index entries (chunk start timestamps).
    fn containing(
        &self,
        offset: u64, // Offset of the *first* row represented by the *first* key in this index node
        index: &LeafIndex<ColumnarTT>, // Contains VecSeq<i64> (first timestamp of each chunk)
        res: &mut [bool], // Result mask for the keys in this index node
    ) {
        let keys = index.keys.as_ref(); // Vec<i64>

        // We need the *summary* associated with this leaf index to filter accurately.
        // Banyan's `containing` doesn't provide the leaf's summary directly.
        // We rely on `intersecting` on the parent branch to do the main filtering.
        // Here, we can only do a very basic check based on the key itself (first timestamp).
        // This might let through leaves that `intersecting` would have pruned.

        for i in 0..keys.len().min(res.len()) {
            if !res[i] { continue; }

            let first_timestamp_in_chunk = keys[i];
            let mut keep = true; // Assume keep unless proven otherwise

            // 1. Check offset range (only check start condition here)
            if self.range_start.map_or(false, |start| offset < start) {
                 // The start of the range this leaf *might* cover is before the query start.
                 // We can't be sure without the row count, so we keep it for now.
                 // A stricter check could be done if we had the leaf summary's count.
            }
            if self.range_end.map_or(false, |end| offset >= end) {
                 // The start of the range this leaf *might* cover is already >= the query end.
                 keep = false;
            }


            // 2. Check time range based on the *first* timestamp (very approximate)
            if keep {
                if let Some(user_query) = &self.query {
                    // Check if *any* clause could possibly match based on the first timestamp
                    keep = user_query.clauses.iter().any(|and_clause| {
                        // Check if *all* conditions in the AND clause *could* be met
                        and_clause.clauses.iter().all(|col_clause| {
                             if col_clause.position == 0 { // DATE column assumed at position 0
                                 match &col_clause.value {
                                     TreeValue::Timestamp(query_ts) => {
                                         // Can this chunk potentially contain matching timestamps?
                                         match col_clause.operator {
                                             // If query wants < X, keep if chunk starts < X
                                             LT => first_timestamp_in_chunk < *query_ts,
                                             // If query wants <= X, keep if chunk starts <= X
                                             LE => first_timestamp_in_chunk <= *query_ts,
                                             // If query wants == X, keep only if chunk starts == X (strict, needs summary)
                                             // EQ => first_timestamp_in_chunk == *query_ts,
                                             // For EQ/NE/GE/GT, we really need the summary range.
                                             // Keep the chunk optimistically.
                                             EQ | NE | GE | GT => true,
                                         }
                                     },
                                     _ => true, // Non-timestamp query on DATE column? Keep.
                                 }
                             } else {
                                 true // Non-DATE clauses don't filter based on the key here
                             }
                        })
                    });
                }
            }

            res[i] = keep;
        }
    }

    /// Filter branch index entries (summaries of children).
    fn intersecting(
        &self,
        offset: u64, // Offset of the first row in the first child covered by this branch index
        index: &BranchIndex<ColumnarTT>, // Contains VecSeq<ColumnarSummary>
        res: &mut [bool], // Result mask for the child summaries
    ) {
        let summaries = index.summaries.as_ref(); // Vec<ColumnarSummary>
        let mut current_logical_offset = offset;

        for i in 0..summaries.len().min(res.len()) {
            if !res[i] {
                // Update offset even if skipping
                current_logical_offset += summaries[i].count;
                continue;
            }

            let child_summary = &summaries[i];
            let child_row_count = child_summary.count;
            let child_logical_end_offset = current_logical_offset + child_row_count;

            // 1. Check offset range overlap
            let range_overlaps =
                self.range_start.map_or(true, |start| child_logical_end_offset > start) &&
                self.range_end.map_or(true, |end| current_logical_offset < end);

            // 2. Check time range overlap (using summary min/max time)
            let time_overlaps = match &self.query {
                Some(user_query) => user_query.clauses.iter().any(|and_clause| {
                     and_clause.clauses.iter().all(|col_clause| {
                         if col_clause.position == 0 { // DATE column
                             match &col_clause.value {
                                 TreeValue::Timestamp(query_ts) => {
                                     match col_clause.operator {
                                         LT => child_summary.min_time < *query_ts, // Range starts before
                                         LE => child_summary.min_time <= *query_ts, // Range starts before or at
                                         EQ => child_summary.min_time <= *query_ts && child_summary.max_time >= *query_ts, // Range overlaps
                                         NE => true, // Summary likely contains non-equal values unless min==max==query_ts
                                         GE => child_summary.max_time >= *query_ts, // Range ends at or after
                                         GT => child_summary.max_time > *query_ts, // Range ends after
                                     }
                                 }
                                 _ => true, // Non-timestamp query on DATE column? Intersects.
                             }
                         } else {
                             true // Check non-DATE columns using column summaries
                         }
                     })
                }),
                None => true, // No query means time overlaps
            };


            // 3. Check user query against the child's column summaries
            let query_intersects = match &self.query {
                 Some(user_query) => user_query.eval_columnar_summary(child_summary),
                 None => true, // No query means it intersects
            };

            res[i] = range_overlaps && time_overlaps && query_intersects;

            // Update offset for the next child
            current_logical_offset = child_logical_end_offset;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::{Row, SummaryRange, SummaryValue};

    use super::*;

    fn test_keys() -> Vec<TreeKey> {
        vec![
            Row(
                0b1010101010101010_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    TreeValue::Integer(16),
                    TreeValue::Float(3.141592),
                    TreeValue::Timestamp(1234),
                    TreeValue::String("Hi Mom!".into()),
                    TreeValue::Integer(42),
                    TreeValue::Float(2.71828),
                    TreeValue::String("Hello Dad!".into()),
                    TreeValue::Timestamp(5678),
                ],
            ),
            Row(
                0b0101010101010101_0000000000000000_0000000000000000_0000000000000000.into(),
                vec![
                    TreeValue::Integer(17),
                    TreeValue::Float(4.141592),
                    TreeValue::Timestamp(2345),
                    TreeValue::String("Hi Mo!".into()),
                    TreeValue::Integer(43),
                    TreeValue::Float(3.71828),
                    TreeValue::String("Hell Dad!".into()),
                    TreeValue::Timestamp(6789),
                ],
            ),
        ]
    }

    #[test]
    fn test_query_col_eval_scalar() {
        let row = &test_keys()[0];
        let q = |operator, position, value| {
            let query = QueryCol {
                operator,
                position,
                value,
            };

            query.eval_scalar(&row)
        };

        assert!(q(LT, 0, TreeValue::Integer(20)));
        assert!(!q(LT, 0, TreeValue::Integer(10)));
        assert!(!q(LT, 0, TreeValue::Integer(16)));

        assert!(q(LE, 0, TreeValue::Integer(20)));
        assert!(!q(LE, 0, TreeValue::Integer(10)));
        assert!(q(LE, 0, TreeValue::Integer(16)));

        assert!(!q(EQ, 0, TreeValue::Integer(20)));
        assert!(!q(EQ, 0, TreeValue::Integer(10)));
        assert!(q(EQ, 0, TreeValue::Integer(16)));

        assert!(q(NE, 0, TreeValue::Integer(20)));
        assert!(q(NE, 0, TreeValue::Integer(10)));
        assert!(!q(NE, 0, TreeValue::Integer(16)));

        assert!(!q(GE, 0, TreeValue::Integer(20)));
        assert!(q(GE, 0, TreeValue::Integer(10)));
        assert!(q(GE, 0, TreeValue::Integer(16)));

        assert!(!q(GT, 0, TreeValue::Integer(20)));
        assert!(q(GT, 0, TreeValue::Integer(10)));
        assert!(!q(GT, 0, TreeValue::Integer(16)));

        assert!(q(LT, 2, TreeValue::Float(4.2)));
        assert!(!q(LT, 2, TreeValue::Float(2.1)));
        assert!(!q(LT, 2, TreeValue::Float(3.141592)));

        assert!(q(LE, 2, TreeValue::Float(4.2)));
        assert!(!q(LE, 2, TreeValue::Float(2.1)));
        assert!(q(LE, 2, TreeValue::Float(3.141592)));

        assert!(!q(EQ, 2, TreeValue::Float(4.2)));
        assert!(!q(EQ, 2, TreeValue::Float(2.1)));
        assert!(q(EQ, 2, TreeValue::Float(3.141592)));

        assert!(!q(GE, 2, TreeValue::Float(4.2)));
        assert!(q(GE, 2, TreeValue::Float(2.1)));
        assert!(q(GE, 2, TreeValue::Float(3.141592)));

        assert!(!q(GT, 2, TreeValue::Float(4.2)));
        assert!(q(GT, 2, TreeValue::Float(2.1)));
        assert!(!q(GT, 2, TreeValue::Float(3.141592)));

        assert!(q(LT, 4, TreeValue::Timestamp(2000)));
        assert!(!q(LT, 4, TreeValue::Timestamp(1000)));
        assert!(!q(LT, 4, TreeValue::Timestamp(1234)));

        assert!(q(LE, 4, TreeValue::Timestamp(2000)));
        assert!(!q(LE, 4, TreeValue::Timestamp(1000)));
        assert!(q(LE, 4, TreeValue::Timestamp(1234)));

        assert!(!q(EQ, 4, TreeValue::Timestamp(2000)));
        assert!(!q(EQ, 4, TreeValue::Timestamp(1000)));
        assert!(q(EQ, 4, TreeValue::Timestamp(1234)));

        assert!(!q(GE, 4, TreeValue::Timestamp(2000)));
        assert!(q(GE, 4, TreeValue::Timestamp(1000)));
        assert!(q(GE, 4, TreeValue::Timestamp(1234)));

        assert!(!q(GT, 4, TreeValue::Timestamp(2000)));
        assert!(q(GT, 4, TreeValue::Timestamp(1000)));
        assert!(!q(GT, 4, TreeValue::Timestamp(1234)));

        assert!(q(LT, 6, TreeValue::String("Zebra".into())));
        assert!(!q(LT, 6, TreeValue::String("Aardvark".into())));
        assert!(!q(LT, 6, TreeValue::String("Hi Mom!".into())));

        assert!(q(LE, 6, TreeValue::String("Zebra".into())));
        assert!(!q(LE, 6, TreeValue::String("Aardvark".into())));
        assert!(q(LE, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(EQ, 6, TreeValue::String("Zebra".into())));
        assert!(!q(EQ, 6, TreeValue::String("Aardvark".into())));
        assert!(q(EQ, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(GE, 6, TreeValue::String("Zebra".into())));
        assert!(q(GE, 6, TreeValue::String("Aardvark".into())));
        assert!(q(GE, 6, TreeValue::String("Hi Mom!".into())));

        assert!(!q(GT, 6, TreeValue::String("Zebra".into())));
        assert!(q(GT, 6, TreeValue::String("Aardvark".into())));
        assert!(!q(GT, 6, TreeValue::String("Hi Mom!".into())));
    }

    fn test_summaries() -> Vec<TreeSummary> {
        vec![
            TreeSummary {
                cols: 0b1010101010101010_0000000000000000_0000000000000000_0000000000000000.into(),
                values: vec![
                    SummaryValue::Integer(SummaryRange { lhs: 16, rhs: 26 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 3.141592,
                        rhs: 3.541591,
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 1234,
                        rhs: 2234,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hi Mom!".into(),
                        rhs: "Salve Madre".into(),
                    }),
                    SummaryValue::Integer(SummaryRange { lhs: 42, rhs: 42 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 2.71828,
                        rhs: 3.21828,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hello Dad!".into(),
                        rhs: "Salve Padre".into(),
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 5678,
                        rhs: 6678,
                    }),
                ],
                rows: 1,
            },
            TreeSummary {
                cols: 0b0101010101010101_0000000000000000_0000000000000000_0000000000000000.into(),
                values: vec![
                    SummaryValue::Integer(SummaryRange { lhs: 17, rhs: 27 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 4.141592,
                        rhs: 4.641592,
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 2345,
                        rhs: 3345,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hi Mo!".into(),
                        rhs: "Hi Ze!".into(),
                    }),
                    SummaryValue::Integer(SummaryRange { lhs: 43, rhs: 53 }),
                    SummaryValue::Float(SummaryRange {
                        lhs: 3.71828,
                        rhs: 4.21828,
                    }),
                    SummaryValue::String(SummaryRange {
                        lhs: "Hell Dad!".into(),
                        rhs: "Hell Zap!".into(),
                    }),
                    SummaryValue::Timestamp(SummaryRange {
                        lhs: 6789,
                        rhs: 7789,
                    }),
                ],
                rows: 1,
            },
        ]
    }

    #[test]
    fn test_query_col_eval_summary() {
        let summary = &test_summaries()[0];
        let q = |operator, position, value| {
            let query = QueryCol {
                operator,
                position,
                value,
            };
            query.eval_summary(summary)
        };

        // 16 .. 26
        assert!(!q(LT, 0, TreeValue::Integer(10)));
        assert!(!q(LT, 0, TreeValue::Integer(16)));
        assert!(q(LT, 0, TreeValue::Integer(20)));
        assert!(q(LT, 0, TreeValue::Integer(26)));
        assert!(q(LT, 0, TreeValue::Integer(30)));

        assert!(!q(LE, 0, TreeValue::Integer(10)));
        assert!(q(LE, 0, TreeValue::Integer(16)));
        assert!(q(LE, 0, TreeValue::Integer(20)));
        assert!(q(LE, 0, TreeValue::Integer(26)));
        assert!(q(LE, 0, TreeValue::Integer(30)));

        assert!(!q(EQ, 0, TreeValue::Integer(10)));
        assert!(q(EQ, 0, TreeValue::Integer(16)));
        assert!(q(EQ, 0, TreeValue::Integer(20)));
        assert!(q(EQ, 0, TreeValue::Integer(26)));
        assert!(!q(EQ, 0, TreeValue::Integer(30)));

        assert!(q(NE, 0, TreeValue::Integer(10)));
        assert!(q(NE, 0, TreeValue::Integer(16)));
        assert!(q(NE, 0, TreeValue::Integer(20)));
        assert!(q(NE, 0, TreeValue::Integer(26)));
        assert!(q(NE, 0, TreeValue::Integer(30)));
        assert!(!q(NE, 8, TreeValue::Integer(42)));

        assert!(q(GT, 0, TreeValue::Integer(10)));
        assert!(q(GT, 0, TreeValue::Integer(16)));
        assert!(q(GT, 0, TreeValue::Integer(20)));
        assert!(!q(GT, 0, TreeValue::Integer(26)));
        assert!(!q(GT, 0, TreeValue::Integer(30)));

        assert!(q(GE, 0, TreeValue::Integer(10)));
        assert!(q(GE, 0, TreeValue::Integer(16)));
        assert!(q(GE, 0, TreeValue::Integer(20)));
        assert!(q(GE, 0, TreeValue::Integer(26)));
        assert!(!q(GE, 0, TreeValue::Integer(30)));
    }

    #[test]
    fn test_query_algebra() {
        let a = QueryCol {
            position: 0,
            operator: EQ,
            value: TreeValue::Integer(16),
        };
        let b = QueryCol {
            position: 4,
            operator: GT,
            value: TreeValue::Timestamp(1000),
        };
        let c = QueryCol {
            position: 2,
            operator: EQ,
            value: TreeValue::Float(3.141592),
        };
        let d = QueryCol {
            position: 6,
            operator: EQ,
            value: TreeValue::String("Hi Mom!".into()),
        };

        fn q(q: QueryCol) -> Query {
            Query::new(q.position, q.operator, q.value)
        }

        let query = q(a.clone()).and(q(b.clone()));
        let expected = Query {
            clauses: vec![QueryAnd {
                clauses: vec![a.clone(), b.clone()],
            }],
        };
        assert_eq!(query, expected);

        let query = query.or(q(c.clone()));
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), b.clone()],
                },
                QueryAnd {
                    clauses: vec![c.clone()],
                },
            ],
        };
        assert_eq!(query, expected);

        let query = query.and(q(d.clone()));
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), b.clone(), d.clone()],
                },
                QueryAnd {
                    clauses: vec![c.clone(), d.clone()],
                },
            ],
        };
        assert_eq!(query, expected);

        let query1 = q(a.clone()).or(q(b.clone()));
        let query2 = q(c.clone()).or(q(d.clone()));
        let query = query1.and(query2);
        let expected = Query {
            clauses: vec![
                QueryAnd {
                    clauses: vec![a.clone(), c.clone()],
                },
                QueryAnd {
                    clauses: vec![a.clone(), d.clone()],
                },
                QueryAnd {
                    clauses: vec![b.clone(), c.clone()],
                },
                QueryAnd {
                    clauses: vec![b.clone(), d.clone()],
                },
            ],
        };
        assert_eq!(query, expected);
    }

    #[test]
    fn test_query_eval_scalar() {
        let keys = test_keys();
        let query = Query::new(0, EQ, TreeValue::Integer(16));
        assert!(query.eval_scalar(&keys[0]));
        assert!(!query.eval_scalar(&keys[1]));

        let query = query.or(Query::new(1, LT, TreeValue::Integer(100)));
        assert!(query.eval_scalar(&keys[0]));
        assert!(query.eval_scalar(&keys[1]));

        let query = query.and(Query::new(5, GT, TreeValue::Timestamp(1000)));
        assert!(!query.eval_scalar(&keys[0]));
        assert!(query.eval_scalar(&keys[1]));
    }

    #[test]
    fn test_query_eval_summary() {
        let summaries = test_summaries();
        let query = Query::new(0, EQ, TreeValue::Integer(16));
        assert!(query.eval_summary(&summaries[0]));
        assert!(!query.eval_summary(&summaries[1]));

        let query = query.or(Query::new(1, LT, TreeValue::Integer(100)));
        assert!(query.eval_summary(&summaries[0]));
        assert!(query.eval_summary(&summaries[1]));

        let query = query.and(Query::new(5, GT, TreeValue::Timestamp(1000)));
        assert!(!query.eval_summary(&summaries[0]));
        assert!(query.eval_summary(&summaries[1]));
    }
}
