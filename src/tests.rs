use anyhow::Result;
use fvm_ipld_hamt::Hash;
use std::fmt::Debug;

use crate::memorydb::MemoryDB;
use fvm_ipld_hamt::Hamt;
use fvm_ipld_hamt::Sha256;
use proptest::collection::*;
use proptest::prelude::*;
use proptest::strategy::Shuffleable;
use serde::de::DeserializeOwned;
use serde::Serialize;
use test_strategy::proptest;

#[derive(Debug, Clone)]
enum Operation<K, V> {
    Insert(K, V),
    Remove(K),
}

impl<K, V> Operation<K, V> {
    pub fn can_be_swapped_with(&self, other: &Operation<K, V>) -> bool
    where
        K: PartialEq,
        V: PartialEq,
    {
        match (self, other) {
            (Operation::Insert(key_a, val_a), Operation::Insert(key_b, val_b)) => {
                // We can't swap if the keys are the same and values different.
                // Because in those cases operation order matters.
                // E.g. insert "a" 10, insert "a" 11 != insert "a" 11, insert "a" 10
                // But insert "a" 10, insert "b" 11 == insert "b" 11, insert "a" 10
                // Or insert "a" 10, insert "a" 10 == insert "a" 10, insert "a" 10 ('swapped')
                key_a != key_b || val_a == val_b
            }
            (Operation::Insert(key_i, _), Operation::Remove(key_r)) => {
                // We can only swap if these two operations are unrelated.
                // Otherwise order matters.
                // E.g. insert "a" 10, remove "a" != remove "a", insert "a" 10
                key_i != key_r
            }
            (Operation::Remove(key_r), Operation::Insert(key_i, _)) => {
                // same as above
                key_i != key_r
            }
            (Operation::Remove(_), Operation::Remove(_)) => {
                // Removes can always be swapped
                true
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Operations<K, V>(Vec<Operation<K, V>>);

impl<K: PartialEq, V: PartialEq> Shuffleable for Operations<K, V> {
    fn shuffle_len(&self) -> usize {
        self.0.len()
    }

    /// Swaps the values if that wouldn't change the semantics.
    /// Otherwise it's a no-op.
    fn shuffle_swap(&mut self, a: usize, b: usize) {
        use std::cmp;
        if a == b {
            return;
        }
        let min = cmp::min(a, b);
        let max = cmp::max(a, b);
        let left = &self.0[min];
        let right = &self.0[max];

        for i in min..=max {
            let neighbor = &self.0[i];
            if !left.can_be_swapped_with(neighbor) {
                return;
            }
            if !right.can_be_swapped_with(neighbor) {
                return;
            }
        }

        // The reasoning for why this works now, is following:
        // Let's look at an example. We checked that we can do all of these swaps:
        // a x y z b
        // x a y z b
        // x y a z b
        // x y z a b
        // x y z b a
        // x y b z a
        // x b y z a
        // b x y z a
        // Observe how a moves to the right
        // and b moves to the left.
        // The end result is the same as
        // just swapping a and b.
        // With all calls to `can_be_swapped_with` above
        // we've made sure that this operation is now safe.

        self.0.swap(a, b);
    }
}

fn node_from_operations<K, V>(
    operations: Operations<K, V>,
    store: &MemoryDB,
) -> Result<Hamt<&MemoryDB, V, K, Sha256, 3>>
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned + Eq,
{
    let mut map: Hamt<&MemoryDB, V, K, Sha256, 3> = Hamt::new_with_bit_width(&store, 4);

    for op in operations.0 {
        match op {
            Operation::Insert(key, value) => {
                map.set(key, value)?;
            }
            Operation::Remove(key) => {
                map.delete(&key)?;
            }
        };
    }

    Ok(map)
}

fn small_key() -> impl Strategy<Value = String> {
    (0..1000).prop_map(|i| format!("key {i}"))
}

fn operation<K: Debug, V: Debug>(
    key: impl Strategy<Value = K>,
    value: impl Strategy<Value = V>,
) -> impl Strategy<Value = Operation<K, V>> {
    (any::<bool>(), key, value).prop_map(|(is_insert, key, value)| {
        if is_insert {
            Operation::Insert(key, value)
        } else {
            Operation::Remove(key)
        }
    })
}

fn operations<K: Debug, V: Debug>(
    key: impl Strategy<Value = K>,
    value: impl Strategy<Value = V>,
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = Operations<K, V>> {
    vec(operation(key, value), size).prop_map(|vec| Operations(vec))
}

fn operations_and_shuffled<K: PartialEq + Clone + Debug, V: PartialEq + Clone + Debug>(
    key: impl Strategy<Value = K>,
    value: impl Strategy<Value = V>,
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = (Operations<K, V>, Operations<K, V>)> {
    operations(key, value, size)
        .prop_flat_map(|operations| (Just(operations.clone()), Just(operations).prop_shuffle()))
}

#[proptest(cases = 1000, max_shrink_iters = 10_000)]
fn node_operations_are_history_independent(
    #[strategy(operations_and_shuffled(small_key(), 0u64..1000, 0..1000))] pair: (
        Operations<String, u64>,
        Operations<String, u64>,
    ),
) {
    let (original, shuffled) = pair;

    let store = &mut MemoryDB::default();

    let mut node1 = node_from_operations(original, store).unwrap();
    let mut node2 = node_from_operations(shuffled, store).unwrap();

    let cid1 = node1.flush().unwrap();
    let cid2 = node2.flush().unwrap();

    assert_eq!(cid1, cid2);
}
