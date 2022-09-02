use anyhow::Result;
use cid::Cid;
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_hamt::Hamt;
use parking_lot::RwLock;
use std::collections::HashMap;

fn main() {
    ExperimentResult::print_csv_header();

    let n = 100_000;
    let step_size = 100;
    let max_steps = 100;
    let mut ms = vec![];

    for x in 0..max_steps {
        ms.push((x + 1) * step_size);
    }

    for bit_width in [1, 2, 3, 4, 5, 6, 7, 8] {
        for m in ms.iter() {
            experiment(bit_width, n, *m).print_csv();
        }
    }
}

struct ExperimentResult {
    n: usize,
    m: usize,
    bit_width: u32,
    total_bytes: u64,
    byte_difference: u64,
}

impl ExperimentResult {
    fn print_csv_header() {
        println!("\n\nn;m;bit_width;total_bytes;byte_diff");
    }

    fn print_csv(&self) {
        println!(
            "{};{};{};{};{}",
            self.n, self.m, self.bit_width, self.total_bytes, self.byte_difference
        )
    }
}

fn experiment(bit_width: u32, n: usize, m: usize) -> ExperimentResult {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize> = Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }

    let _cid = map.flush().unwrap();
    let total_bytes = store.bytes_stored();

    let value_after = ".";

    for key in 0..m {
        map.set(key, value_after.to_string()).unwrap();
    }

    let _cid_after = map.flush().unwrap();
    let bytes_after = store.bytes_stored();
    let byte_difference = bytes_after - total_bytes;

    let result = ExperimentResult {
        n,
        m,
        bit_width,
        total_bytes,
        byte_difference,
    };

    result
}

/// A thread-safe `HashMap` wrapper.
#[derive(Debug, Default)]
pub struct MemoryDB {
    db: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemoryDB {
    fn bytes_stored(&self) -> u64 {
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
