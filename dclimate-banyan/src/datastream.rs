use banyan::{store::BranchCache, Config, Forest, Secrets, StreamBuilder, Transaction};
use libipld::Cid;

use crate::{
    codec::{Row, TreeKey, TreeType},
    data_definition::{DataDefinition, Record},
    error::{Result, UninitializedDatastreamError},
    query::BanyanQuery,
    BanyanStore, Query,
};

#[derive(Clone)]
pub struct Datastream<S: BanyanStore> {
    pub cid: Option<Cid>,
    pub data_definition: DataDefinition,

    store: S,
}

impl<S: BanyanStore> Datastream<S> {
    pub fn new(store: S, data_definition: DataDefinition) -> Self {
        Self {
            cid: None,
            store,
            data_definition,
        }
    }

    pub fn load(cid: &Cid, store: S, data_definition: DataDefinition) -> Self {
        Self {
            cid: Some(cid.clone()),
            store,
            data_definition,
        }
    }

    fn update(self, cid: Cid) -> Self {
        Self {
            cid: Some(cid),
            store: self.store,
            data_definition: self.data_definition,
        }
    }

    pub fn nuke(self) -> Self {
        self
    }

    pub fn extend<'dd, I: IntoIterator<Item = Record<'dd>>>(self, records: I) -> Result<Self> {
        // Copying the example and hoping for the best. Not sure which of these intermediate objects
        // would be safe or useful to retain between calls so for now we start from scratch for each
        // call.
        let forest = Forest::<TreeType, _>::new(self.store.clone(), BranchCache::new(1024));
        let mut txn = Transaction::new(forest, self.store.clone());
        let mut builder = match self.cid {
            None => StreamBuilder::new(Config::debug_fast(), Secrets::default()),
            Some(cid) => {
                txn.load_stream_builder(Secrets::default(), Config::debug_fast(), cid.try_into()?)?
            }
        };
        let rows: Result<Vec<(TreeKey, Row)>> = records
            .into_iter()
            .map(|r| {
                Ok((
                    self.data_definition.key_from_record(r.clone())?,
                    self.data_definition.row_from_record(r)?,
                ))
            })
            .collect();
        txn.extend(&mut builder, rows?)?;
        let tree = builder.snapshot();
        let root = tree.root().expect("there should be a root");

        Ok(self.update(root.clone().into()))
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Result<Record>>> {
        // TBD: Should this be allowed? Might be prudent to force user to narrow bounds.
        self.iter_query(BanyanQuery {
            query: None,
            range_start: None,
            range_end: None,
        })
    }

    pub fn query(&self, query: &Query) -> Result<impl Iterator<Item = Result<Record>>> {
        self.iter_query(BanyanQuery {
            query: Some(query.clone()),
            range_start: None,
            range_end: None,
        })
    }

    fn iter_query(&self, query: BanyanQuery) -> Result<impl Iterator<Item = Result<Record>>> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => Err(UninitializedDatastreamError)?,
        };
        let forest = Forest::<TreeType, _>::new(self.store.clone(), BranchCache::new(1024));
        let txn = Transaction::new(forest, self.store.clone());
        let tree = txn.load_tree::<Row>(Secrets::default(), cid.try_into()?)?;
        let records = txn.iter_filtered(&tree, query).map(|item| {
            let (_i, _k, row) = item?;
            self.data_definition.record_from_row(row)
        });

        Ok(records)
    }

    pub fn slice<'ds>(&'ds self, start: u64, end: u64) -> DatastreamView<'ds, S> {
        DatastreamView {
            datastream: self,
            start: Some(start),
            end: Some(end),
        }
    }

    pub fn slice_from<'ds>(&'ds self, index: u64) -> DatastreamView<'ds, S> {
        DatastreamView {
            datastream: self,
            start: Some(index),
            end: None,
        }
    }

    pub fn slice_to<'ds>(&'ds self, index: u64) -> DatastreamView<'ds, S> {
        DatastreamView {
            datastream: self,
            start: None,
            end: Some(index),
        }
    }
}

pub struct DatastreamView<'ds, S: BanyanStore> {
    datastream: &'ds Datastream<S>,
    start: Option<u64>,
    end: Option<u64>,
}

impl<'ds, S: BanyanStore> DatastreamView<'ds, S> {
    pub fn slice(&self, start: u64, end: u64) -> Self {
        let (start, end) = match self.start {
            Some(view_start) => (Some(view_start + start), Some(view_start + end)),
            None => (Some(start), Some(end)),
        };

        Self {
            datastream: self.datastream,
            start,
            end,
        }
    }

    pub fn slice_to(&self, index: u64) -> Self {
        let end = Some(match self.start {
            Some(view_start) => view_start + index,
            None => index,
        });

        Self {
            datastream: self.datastream,
            start: self.start,
            end,
        }
    }

    pub fn slice_from(&self, index: u64) -> Self {
        let start = Some(match self.start {
            Some(view_start) => view_start + index,
            None => index,
        });

        Self {
            datastream: self.datastream,
            start,
            end: self.end,
        }
    }
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<Record>>> {
        self.datastream.iter_query(BanyanQuery {
            query: None,
            range_start: self.start,
            range_end: self.end,
        })
    }

    pub fn query(&self, query: &Query) -> Result<impl Iterator<Item = Result<Record>>> {
        self.datastream.iter_query(BanyanQuery {
            query: Some(query.clone()),
            range_start: self.start,
            range_end: self.end,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{memory_store, ColumnType};

    use super::*;

    const SIZE_64_MB: usize = 1 << 26;

    fn make_data_definition() -> DataDefinition {
        DataDefinition::from_iter(vec![("one", ColumnType::Integer, true)])
    }

    #[test]
    fn test_datastream_slice() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);
        let sliced = datastream.slice(2, 4);
        assert_eq!(sliced.start.unwrap(), 2);
        assert_eq!(sliced.end.unwrap(), 4);
    }

    #[test]
    fn test_datastream_slice_to() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);
        let sliced = datastream.slice_to(4);
        assert!(sliced.start.is_none());
        assert_eq!(sliced.end.unwrap(), 4);
    }

    #[test]
    fn test_datastream_slice_from() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);
        let sliced = datastream.slice_from(2);
        assert_eq!(sliced.start.unwrap(), 2);
        assert!(sliced.end.is_none());
    }

    #[test]
    fn test_datastream_view_slice() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: None,
            end: None,
        };
        let sliced = to_slice.slice(2, 4);
        assert_eq!(sliced.start.unwrap(), 2);
        assert_eq!(sliced.end.unwrap(), 4);

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: Some(100),
            end: Some(200),
        };
        let sliced = to_slice.slice(2, 4);
        assert_eq!(sliced.start.unwrap(), 102);
        assert_eq!(sliced.end.unwrap(), 104);
    }

    #[test]
    fn test_datastream_view_slice_to() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: None,
            end: Some(100),
        };
        let sliced = to_slice.slice_to(4);
        assert!(sliced.start.is_none());
        assert_eq!(sliced.end.unwrap(), 4);

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: Some(100),
            end: Some(200),
        };
        let sliced = to_slice.slice_to(4);
        assert_eq!(sliced.start.unwrap(), 100);
        assert_eq!(sliced.end.unwrap(), 104);
    }

    #[test]
    fn test_datastream_view_slice_from() {
        let data_definition = make_data_definition();
        let datastream = Datastream::new(memory_store(SIZE_64_MB), data_definition);

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: None,
            end: None,
        };
        let sliced = to_slice.slice_from(2);
        assert_eq!(sliced.start.unwrap(), 2);
        assert!(sliced.end.is_none());

        let to_slice = DatastreamView {
            datastream: &datastream,
            start: Some(100),
            end: Some(200),
        };
        let sliced = to_slice.slice_from(2);
        assert_eq!(sliced.start.unwrap(), 102);
        assert_eq!(sliced.end.unwrap(), 200);
    }
}
