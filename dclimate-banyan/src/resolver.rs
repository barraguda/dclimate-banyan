use banyan::store::{BlockWriter, MemStore};
use banyan_utils::{ipfs::IpfsStore, tags::Sha256Digest};
use libipld::Cid;

use crate::{datastream::Datastream, BanyanStore, DataDefinition};

#[derive(Clone)]
pub struct Resolver<S: BanyanStore> {
    pub(crate) store: S,
}

impl<S: BanyanStore> Resolver<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn new_datastream<'ds>(
        &'ds self,
        data_definition: &'ds DataDefinition,
    ) -> Datastream<'ds, S> {
        Datastream::new(self, data_definition)
    }

    pub fn load_datastream<'ds>(
        &'ds self,
        cid: &Cid,
        data_definition: &'ds DataDefinition,
    ) -> Datastream<'ds, S> {
        Datastream::load(cid.clone(), self, data_definition)
    }
}

pub fn memory_store(max_size: usize) -> MemStore<Sha256Digest> {
    MemStore::new(max_size, Sha256Digest::digest)
}

pub fn ipfs_available() -> bool {
    IpfsStore.put(vec![]).is_ok()
}
