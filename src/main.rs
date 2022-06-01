use forest_db::{Error, Store};
use ipld_blockstore::BlockStore;
use ipld_hamt::Hamt;
use parking_lot::RwLock;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};

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

    let cid = map.flush().unwrap();
    let total_bytes = store.bytes_stored();

    let value_after = ".";

    for key in 0..m {
        map.set(key, value_after.to_string()).unwrap();
    }

    let cid_after = map.flush().unwrap();
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
    db: RwLock<HashMap<u64, Vec<u8>>>,
}

impl MemoryDB {
    fn db_index<K>(key: K) -> u64
    where
        K: AsRef<[u8]>,
    {
        let mut hasher = DefaultHasher::new();
        key.as_ref().hash::<DefaultHasher>(&mut hasher);
        hasher.finish()
    }

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

impl Store for MemoryDB {
    fn write<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .write()
            .insert(Self::db_index(key), value.as_ref().to_vec());
        Ok(())
    }

    fn delete<K>(&self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.db.write().remove(&Self::db_index(key));
        Ok(())
    }

    fn read<K>(&self, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.read().get(&Self::db_index(key)).cloned())
    }

    fn exists<K>(&self, key: K) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.read().contains_key(&Self::db_index(key)))
    }
}

impl BlockStore for MemoryDB {}
