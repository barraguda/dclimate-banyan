use crate::{
    bitmap::Bitmap,
    codec::SummaryValue,
    codec::TreeValue, // Keep TreeValue import
    data_definition::{ColumnType, DataDefinition, Record},
    error::{Result, UninitializedDatastreamError},
    // Use the ColumnarBanyanQuery struct
    query::{ColumnarBanyanQuery, Query as UserQuery},
    value::Value,
    BanyanStore,
};
use banyan::{
    // Public API items:
    index::{Summarizable, VecSeq}, 
    store::{BranchCache},          
    Config, Forest, Secrets, StreamBuilder, Transaction, TreeTypes, FilteredChunk, // Keep FilteredChunk for iter_filtered_chunked
};
use banyan_utils::tags::Sha256Digest;
use cbor_data::codec::{CodecError, ReadCbor, WriteCbor};
use chrono::NaiveDateTime;
use either::Either;
use libipld::{
    IpldCodec, multihash::{Multihash, Code},
    cbor::DagCborCodec,
    prelude::{Decode, Encode},
    Cid, DagCbor,
};
use std::{cmp::min, collections::BTreeMap, iter, sync::Arc};

// --- Banyan TreeTypes Implementation for Columnar Storage ---

#[derive(Debug, Clone)]
pub struct ColumnarTT;

impl TreeTypes for ColumnarTT {
    type Key = i64;
    type Summary = ColumnarSummary;
    type KeySeq = VecSeq<i64>;
    type SummarySeq = VecSeq<ColumnarSummary>;
    type Link = Sha256Digest;
    const NONCE: &'static [u8; 24] = b"dclimate_columnar_nonce!";
}

// --- Columnar Data Structures ---

#[derive(Clone, Debug, PartialEq, ReadCbor, WriteCbor)]
pub struct RowSeq {
    num_rows: u64,
    timestamps: Vec<i64>,
    columns: Vec<Option<ColumnData>>,
}

impl RowSeq {
    // Keep new and push as they were in the base
    fn new(dd: &DataDefinition) -> Self {
        Self {
            num_rows: 0,
            timestamps: Vec::new(),
            columns: vec![None; dd.len()],
        }
    }
    fn push(&mut self, record: &Record<'_>, dd: &DataDefinition) -> Result<()> {
        let timestamp = record.values.get("DATE")
            .ok_or_else(|| anyhow::anyhow!("Record missing 'DATE'"))?.clone().try_into()?;
        self.timestamps.push(timestamp);
        for (idx, col_def) in dd.iter().enumerate() {
            if col_def.name == "DATE" { continue; }
            let current_row_index = self.num_rows;
            if let Some(value) = record.values.get(&col_def.name) {
                let tree_value = TreeValue::from_value(&col_def.kind, value.clone())?;
                match self.columns.get_mut(idx) {
                    Some(Some(col_data)) => col_data.push(current_row_index, tree_value),
                    Some(None) => {
                        let mut col_data = ColumnData::new(col_def.kind.clone());
                        col_data.push(current_row_index, tree_value);
                        self.columns[idx] = Some(col_data);
                    }
                    None => return Err(anyhow::anyhow!("Internal error: Column index {} out of bounds", idx)),
                }
            } else if let Some(Some(col_data)) = self.columns.get_mut(idx) {
                col_data.ensure_length(current_row_index + 1);
            }
        }
        self.num_rows += 1;
        Ok(())
    }

    // *** CHANGE 3: Modify get_record signature ***
    // The returned Record borrows the DataDefinition ('a), but NOT the RowSeq (&self).
    fn get_record<'a>(&self, row_idx: usize, dd: &'a DataDefinition) -> Result<Record<'a>> {
    // *********************************************
        // Implementation remains the same
        if row_idx >= self.num_rows as usize {
             return Err(anyhow::anyhow!("Row index out of bounds: {} >= {}", row_idx, self.num_rows));
        }

        let mut values = BTreeMap::new();
        let ts = *self.timestamps.get(row_idx).ok_or_else(|| anyhow::anyhow!("Timestamp index out of bounds: {} >= {}", row_idx, self.timestamps.len()))?;
        let naive_dt = NaiveDateTime::from_timestamp_opt(ts, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp value {} found in RowSeq", ts))?;
        values.insert("DATE".to_string(), Value::Timestamp(naive_dt));
        for (col_idx, col_def) in dd.iter().enumerate() {
            if col_def.name == "DATE" { continue; }
            if let Some(Some(col_data)) = self.columns.get(col_idx) {
                // Use usize for col_data.get based on original ColumnData impl
                if let Some(tree_value) = col_data.get(row_idx) {
                    values.insert(col_def.name.clone(), Value::from_tree_value(&col_def.kind, tree_value)?);
                }
            }
        }
        Ok(Record { data_definition: dd, values })
    }
}

// --- Keep ColumnData and its impls exactly as in the base ---
#[derive(Clone, Debug, PartialEq, ReadCbor, WriteCbor)]
pub struct ColumnData {
    presence: Bitmap,
    values: Vec<TreeValue>,
    kind: ColumnType,
    num_rows_spanned: u64,
}
impl ReadCbor for ColumnType {
     fn read_cbor_impl(cbor: &cbor_data::Cbor) -> cbor_data::codec::Result<Self> {
         let bytes = cbor.as_slice();
         let mut cursor = std::io::Cursor::new(bytes);
         Decode::decode(DagCborCodec, &mut cursor)
             .map_err(|e| CodecError::Custom(e.into()))
     }
     fn fmt(f: &mut impl std::fmt::Write) -> std::fmt::Result { write!(f, "ColumnType") }
}
impl WriteCbor for ColumnType {
     fn write_cbor<W: cbor_data::Writer>(&self, w: W) -> W::Output {
         let mut buf = Vec::new();
         Encode::encode(self, DagCborCodec, &mut buf).expect("Encoding ColumnType failed");
         w.write_trusting(&buf)
     }
}
impl Encode<DagCborCodec> for ColumnType {
    fn encode<W: std::io::Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        match self {
            ColumnType::Timestamp => 0u8.encode(c, w),
            ColumnType::Integer => 1u8.encode(c, w),
            ColumnType::Float => 2u8.encode(c, w),
            ColumnType::String => 3u8.encode(c, w),
            ColumnType::Enum(choices) => { w.write_all(&[0x82])?; 4u8.encode(c, w)?; choices.encode(c, w) }
        }
    }
}
impl Decode<DagCborCodec> for ColumnType {
    fn decode<R: std::io::Read + std::io::Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        let first_byte = u8::decode(c, r)?;
        match first_byte {
            0 => Ok(ColumnType::Timestamp), 1 => Ok(ColumnType::Integer),
            2 => Ok(ColumnType::Float), 3 => Ok(ColumnType::String),
            0x82 => { // This handling for Enum looks correct based on Encode
                let kind_code = u8::decode(c, r)?;
                if kind_code == 4 { Ok(ColumnType::Enum(Vec::<String>::decode(c, r)?)) }
                else { Err(anyhow::anyhow!("Invalid CBOR for Enum: tag {}", kind_code)) }
            }
            _ => Err(anyhow::anyhow!("Invalid CBOR tag for ColumnType: {}", first_byte)),
        }
    }
}
impl ColumnData {
    fn new(kind: ColumnType) -> Self { Self { presence: Bitmap::new(), values: Vec::new(), kind, num_rows_spanned: 0 } }
    fn push(&mut self, row_idx: u64, value: TreeValue) {
        self.ensure_length(row_idx + 1);
        // Keep original panic check or ensure Bitmap handles large indices
        if row_idx >= 64 { panic!("Bitmap index out of bounds (max 63): {}", row_idx); }
        let idx_usize = row_idx as usize;
        let rank = self.presence.rank(idx_usize);
        self.presence.set(idx_usize, true);
        self.values.insert(rank as usize, value);
    }
    fn get(&self, row_idx: usize) -> Option<TreeValue> { // Keep usize based on base
        if row_idx < 64 && row_idx < self.num_rows_spanned as usize && self.presence.get(row_idx) {
            let rank = self.presence.rank(row_idx + 1);
            if rank > 0 { self.values.get((rank - 1) as usize).cloned() } else { None }
        } else { None }
    }
    fn ensure_length(&mut self, rows: u64) { self.num_rows_spanned = self.num_rows_spanned.max(rows); }
    fn summarize(&self) -> Option<SummaryValue> {
        if self.values.is_empty() { return None; }
        let mut summary = SummaryValue::from(self.values[0].clone());
        for value in self.values.iter().skip(1) {
            summary = crate::codec::SummaryValue::incorporate(&summary, value.clone());
        }
        Some(summary)
    }
}

// --- Keep ColumnarSummary and Summarizable impls exactly as in the base ---
#[derive(Clone, Debug, PartialEq, DagCbor)]
pub struct ColumnarSummary {
    pub min_time: i64, pub max_time: i64, pub count: u64, pub cols: Bitmap, pub values: Vec<SummaryValue>,
}
impl ColumnarSummary {
    fn from_row_seq(row_seq: &RowSeq, dd: &DataDefinition) -> Self {
        let min_time = *row_seq.timestamps.iter().min().unwrap_or(&i64::MAX);
        let max_time = *row_seq.timestamps.iter().max().unwrap_or(&i64::MIN);
        let count = row_seq.num_rows;
        let mut cols = Bitmap::new();
        let mut indexed_values = Vec::new();
        for (idx, col_opt) in row_seq.columns.iter().enumerate() {
            if dd.get(idx).map(|c| c.name == "DATE").unwrap_or(false) { continue; }
            if let Some(col_data) = col_opt {
                if let Some(summary) = col_data.summarize() {
                    if idx < 64 { cols.set(idx, true); indexed_values.push((idx, summary)); }
                    else { eprintln!("Warning: Column index {} exceeds bitmap capacity (64)", idx); }
                }
            }
        }
        indexed_values.sort_by_key(|(idx, _)| *idx);
        let values = indexed_values.into_iter().map(|(_, val)| val).collect();
        Self { min_time, max_time, count, cols, values }
    }
    pub(crate) fn get_column_summary(&self, pos: usize) -> Option<&SummaryValue> {
        if pos < 64 && self.cols.get(pos) {
            let rank = self.cols.rank(pos + 1);
            if rank > 0 { self.values.get((rank - 1) as usize) } else { None }
        } else { None }
    }
}
impl Summarizable<ColumnarSummary> for VecSeq<ColumnarSummary> {
    fn summarize(&self) -> ColumnarSummary {
        if self.as_ref().is_empty() {
            return ColumnarSummary { min_time: i64::MAX, max_time: i64::MIN, count: 0, cols: Bitmap::new(), values: Vec::new() };
        }
        let mut min_time = i64::MAX; let mut max_time = i64::MIN; let mut count = 0;
        let mut cols_bitmap = Bitmap::new();
        let mut combined_summaries: Vec<Option<SummaryValue>> = vec![None; 64]; // Assuming 64 columns max
        for child_summary in self.as_ref().iter() {
            min_time = min_time.min(child_summary.min_time); max_time = max_time.max(child_summary.max_time);
            count += child_summary.count; cols_bitmap |= child_summary.cols;
            let mut child_values_iter = child_summary.values.iter();
            for col_idx in 0..64 { // Iterate up to bitmap capacity
                if child_summary.cols.get(col_idx) {
                    let child_val = child_values_iter.next().expect("Bitmap/values mismatch");
                    combined_summaries[col_idx] = Some(match combined_summaries[col_idx].take() {
                        Some(existing) => crate::codec::SummaryValue::extend(&existing, child_val.clone()).unwrap_or(existing), // Handle extend error
                        None => child_val.clone(),
                    });
                }
            }
        }
        let final_values = (0..64).filter(|&i| cols_bitmap.get(i)).filter_map(|i| combined_summaries[i].take()).collect();
        ColumnarSummary { min_time, max_time, count, cols: cols_bitmap, values: final_values }
    }
}
impl Summarizable<ColumnarSummary> for VecSeq<i64> {
    fn summarize(&self) -> ColumnarSummary {
        let keys = self.as_ref(); let count = keys.len() as u64;
        let min_time = *keys.iter().min().unwrap_or(&i64::MAX);
        let max_time = *keys.iter().max().unwrap_or(&i64::MIN);
        ColumnarSummary { min_time, max_time, count, cols: Bitmap::new(), values: Vec::new() }
    }
}

// --- Columnar Datastream Implementation ---

#[derive(Clone)]
pub struct ColumnarDatastream<S: BanyanStore> {
    pub cid: Option<Cid>,
    pub data_definition: Arc<DataDefinition>,
    store: S,
}

// Add necessary bounds for Forest/Transaction usage if store is cloned
// Make sure S: BanyanStore + Clone + Send + Sync + 'static covers all needs
impl<S: BanyanStore + Clone + Send + Sync + 'static> ColumnarDatastream<S> {
    // Keep new, load as before
    pub fn new(store: S, data_definition: DataDefinition) -> Self {
        Self { cid: None, store, data_definition: Arc::new(data_definition) }
    }
    pub fn load(cid: &Cid, store: S, data_definition: DataDefinition) -> Self {
        Self { cid: Some(cid.clone()), store, data_definition: Arc::new(data_definition) }
    }
    
        // Add the effective_leaf_limit logic back here
        pub fn extend<'dd, I>(mut self, records: I) -> Result<Self>
            where I: IntoIterator<Item = Record<'dd>>, I::IntoIter: Send {
            let forest = Forest::<ColumnarTT, _>::new(self.store.clone(), BranchCache::new(1 << 20));
            let mut txn = Transaction::new(forest.clone(), self.store.clone());
            let config = Config::debug_fast(); // Or Config::default() etc.
            let secrets = Secrets::default();
    
            let mut builder = match self.cid {
                None => StreamBuilder::new(config.clone(), secrets.clone()),
                Some(ref cid) => {
                     let link: Sha256Digest = cid.clone().try_into()
                        .map_err(|e| anyhow::anyhow!("Failed to convert Cid to Sha256Digest: {}", e))?;
                     txn.load_stream_builder(secrets.clone(), config.clone(), link)?
                }
            };
    
            let mut current_row_seq = RowSeq::new(&self.data_definition);
            let mut first_key_in_leaf: Option<i64> = None;
            let mut leaf_buffer: Vec<(i64, RowSeq)> = Vec::new();
            const BUFFER_SIZE: usize = 10; // Banyan transaction buffer size
    
            // *** Define the hard limit imposed by ColumnData's Bitmap ***
            const BITMAP_LIMIT: u64 = 64;
            // *** Determine the effective limit for flushing RowSeq ***
            let effective_leaf_limit = min(BITMAP_LIMIT, config.max_leaf_count as u64);
            // Optional: Log the effective limit being used
            // println!("Using effective leaf limit: {}", effective_leaf_limit);
    
    
            // --- Record processing loop ---
            for record in records { // Consider records.into_iter() if records is owned
                let timestamp: i64 = record.values.get("DATE").ok_or_else(|| anyhow::anyhow!("Record missing 'DATE'"))?.clone().try_into()?;
                if first_key_in_leaf.is_none() { first_key_in_leaf = Some(timestamp); }
    
                // Add the record to the current RowSeq
                // This increments current_row_seq.num_rows internally
                current_row_seq.push(&record, &self.data_definition)?;
    
                // *** Check if the RowSeq has reached the effective limit ***
                // This check now happens *before* num_rows can reach 64 (unless config is smaller)
                if current_row_seq.num_rows >= effective_leaf_limit {
                    // RowSeq is full according to the stricter limit, flush it
                    let key = first_key_in_leaf.expect("Leaf key should be set if num_rows > 0");
                    leaf_buffer.push((key, current_row_seq)); // Push the completed RowSeq
    
                    // Start a new RowSeq
                    current_row_seq = RowSeq::new(&self.data_definition);
                    first_key_in_leaf = None; // Reset key for the new RowSeq
    
                    // Flush the leaf_buffer to the transaction if it's full
                    if leaf_buffer.len() >= BUFFER_SIZE {
                         txn.extend(&mut builder, leaf_buffer.drain(..))?;
                    }
                }
            }
            // --- End of record processing loop ---
    
            // Handle any remaining records in the last RowSeq
            if current_row_seq.num_rows > 0 {
                let key = first_key_in_leaf.expect("Partial leaf key should exist");
                leaf_buffer.push((key, current_row_seq));
            }
            // Flush any remaining leaves in the buffer
            if !leaf_buffer.is_empty() {
                 txn.extend(&mut builder, leaf_buffer)?;
            }
    
    
            let final_tree = builder.snapshot();
            let root_link = final_tree.root().ok_or_else(|| anyhow::anyhow!("No root link found after snapshot"))?;
    
            let new_cid = Cid::from(*root_link);
            self.cid = Some(new_cid);
    
            self.store = txn.into_writer();
            Ok(self)
        }
    
    // --- iter_query and other methods remain the same as the previous successful fix ---
    pub fn iter_query<'a>(
        &'a self,
        query: ColumnarBanyanQuery,
    ) -> Result<impl Iterator<Item = Result<Record<'a>>> + Send + 'a> {
        let cid = match self.cid {
            Some(ref cid) => cid.clone(),
            None => return Err(UninitializedDatastreamError.into()),
        };
        let forest = Forest::<ColumnarTT, _>::new(self.store.clone(), BranchCache::new(1 << 20));
        let secrets = Secrets::default();
        let tree = forest.load_tree::<RowSeq>(secrets.clone(), cid.try_into()?)?;

        let chunk_iterator = forest.iter_filtered_chunked(
            &tree,
            query.clone(),
            &|_| (),
        );

        let dd_ref: &'a DataDefinition = self.data_definition.as_ref();
        let query_clone = query.clone();

        let record_iterator = chunk_iterator.flat_map(move |chunk_result| {
            let inner_query = query_clone.clone();
            match chunk_result {
                Ok(chunk) => {
                    let records_iter = chunk.data.into_iter().flat_map(move |(offset, _key, row_seq)| {
                        let user_query_option_innermost = inner_query.query.clone();
                        (0..row_seq.num_rows).filter_map(move |row_idx_in_chunk| {
                            let logical_row_idx = offset + row_idx_in_chunk;
                            let in_offset_range =
                                inner_query.range_start.map_or(true, |s| logical_row_idx >= s) &&
                                inner_query.range_end.map_or(true, |e| logical_row_idx < e);
                            if !in_offset_range { return None; }
                            match row_seq.get_record(row_idx_in_chunk as usize, dd_ref) {
                                Ok(record) => {
                                    let matches_user_query = match &user_query_option_innermost {
                                        Some(user_q) => {
                                            match dd_ref.key_from_record(record.clone()) {
                                                Ok(tree_key) => crate::query::Query::eval_scalar(user_q, &tree_key),
                                                Err(_) => false,
                                            }
                                        }, None => true,
                                    };
                                    if matches_user_query { Some(Ok(record)) } else { None }
                                }
                                Err(e) => Some(Err(e)),
                            }
                        })
                    });
                    Either::Left(records_iter)
                }
                Err(e) => Either::Right(iter::once(Err(e))),
            }
        });
        Ok(record_iterator)
    }

    pub fn iter<'a>(&'a self) -> Result<impl Iterator<Item = Result<Record<'a>>> + Send + 'a> {
         self.iter_query(ColumnarBanyanQuery { query: None, range_start: None, range_end: None })
    }

     pub fn query<'a>(&'a self, query: &UserQuery) -> Result<impl Iterator<Item = Result<Record<'a>>> + Send + 'a> {
         self.iter_query(ColumnarBanyanQuery { query: Some(query.clone()), range_start: None, range_end: None })
     }

    pub fn slice<'ds>(&'ds self, start: u64, end: u64) -> ColumnarDatastreamView<'ds, S> {
        ColumnarDatastreamView { datastream: self, start: Some(start), end: Some(end) }
    }
}

// --- DatastreamView and its impl remain the same ---
pub struct ColumnarDatastreamView<'ds, S: BanyanStore + Clone + Send + Sync + 'static> {
    datastream: &'ds ColumnarDatastream<S>,
    start: Option<u64>,
    end: Option<u64>,
}
impl<'ds, S: BanyanStore + Clone + Send + Sync + 'static> ColumnarDatastreamView<'ds, S> {
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<Record<'ds>>> + Send + 'ds> {
        self.datastream.iter_query(ColumnarBanyanQuery { query: None, range_start: self.start, range_end: self.end })
    }
    pub fn query(&self, query: &UserQuery) -> Result<impl Iterator<Item = Result<Record<'ds>>> + Send + 'ds> {
        self.datastream.iter_query(ColumnarBanyanQuery { query: Some(query.clone()), range_start: self.start, range_end: self.end })
    }
}

