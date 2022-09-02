use anyhow::Result;
use cid::Cid;
use fvm_ipld_blockstore::Blockstore;
use parking_lot::RwLock;
use std::collections::HashMap;

/// A thread-safe `HashMap` wrapper.
#[derive(Debug, Default)]
pub struct MemoryDB {
    db: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryDB {
    pub fn bytes_stored(&self) -> u64 {
        let map = self.db.read().clone();
        let mut count: u64 = 0;
        for value in map.values() {
            count += value.len() as u64;
        }
        count
    }
}

impl Clone for MemoryDB {
    fn clone(&self) -> Self {
        Self {
            db: RwLock::new(self.db.read().clone()),
        }
    }
}

impl Blockstore for MemoryDB {
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.db.read().contains_key(&k.to_bytes()))
    }

    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.db.read().get(&k.to_bytes()).cloned())
    }

    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        self.db.write().insert(k.to_bytes(), block.into());
        Ok(())
    }
}
