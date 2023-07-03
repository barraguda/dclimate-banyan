use banyan::{store::BranchCache, Config, Forest, Secrets, StreamBuilder, Transaction};
use libipld::Cid;

use crate::{
    codec::{Row, TreeKey, TreeType},
    data_definition::{DataDefinition, Record},
    error::Result,
    resolver::Resolver,
    BanyanStore,
};

pub struct Datastream<'ds, S: BanyanStore> {
    pub cid: Option<Cid>,

    resolver: &'ds Resolver<S>,
    data_definition: &'ds DataDefinition,
}

impl<'ds, S: BanyanStore> Datastream<'ds, S> {
    pub(crate) fn new(resolver: &'ds Resolver<S>, data_definition: &'ds DataDefinition) -> Self {
        Self {
            cid: None,
            resolver,
            data_definition,
        }
    }

    pub(crate) fn load(
        cid: Cid,
        resolver: &'ds Resolver<S>,
        data_definition: &'ds DataDefinition,
    ) -> Self {
        Self {
            cid: Some(cid),
            resolver,
            data_definition,
        }
    }

    fn update(self, cid: Cid) -> Self {
        Self {
            cid: Some(cid),
            resolver: self.resolver,
            data_definition: self.data_definition,
        }
    }

    pub fn extend<I: IntoIterator<Item = Record<'ds>>>(self, records: I) -> Result<Self> {
        // Copying the example and hoping for the best. Not sure which of these intermediate objects
        // would be safe or useful to retain between calls so for now we start from scratch for each
        // call.
        let forest =
            Forest::<TreeType, _>::new(self.resolver.store.clone(), BranchCache::new(1024));
        let mut builder = StreamBuilder::new(Config::debug_fast(), Secrets::default());
        let mut txn = Transaction::new(forest, self.resolver.store.clone());
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
        let forest =
            Forest::<TreeType, _>::new(self.resolver.store.clone(), BranchCache::new(1024));
        let txn = Transaction::new(forest, self.resolver.store.clone());
        let tree = txn.load_tree::<Row>(Secrets::default(), self.cid.unwrap().try_into()?)?;
        let records = txn.iter_from(&tree).map(|item| {
            let (_i, _k, row) = item?;
            self.data_definition.record_from_row(row)
        });

        Ok(records)
    }
}
