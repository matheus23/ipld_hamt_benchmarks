pub mod memorydb;

#[cfg(test)]
mod tests;

use std::{cmp, ops::AddAssign};

use cid::Cid;
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::{de::DeserializeOwned, serde_bytes::Deserialize, CborStore};
use fvm_ipld_hamt::{
    bitfield::Bitfield, node::Node, pointer::Pointer, Hamt, Hash, HashAlgorithm, KeyValuePair,
    Sha256,
};
use memorydb::MemoryDB;
use once_cell::unsync::OnceCell;
use serde::Serialize;

const BUCKET_SIZE: usize = 1;

fn main() {
    test_hamt_dot();
    // println!("Bucket Size {BUCKET_SIZE}");
    // ExperimentResult::print_csv_header();

    // let n = 100_000;
    // let step_size = 1;
    // let window_start = 0;
    // let window_end = 100;
    // let max_steps = (window_end - window_start) / step_size;
    // let mut ms = vec![];

    // for x in 0..max_steps {
    //     ms.push((x + 1) * step_size + window_start);
    // }

    // for m in ms.iter() {
    //     experiment::<BUCKET_SIZE>(4, n, *m).print_csv();
    // }
}

struct ExperimentResult {
    n: usize,
    m: usize,
    bucket_size: usize,
    bit_width: u32,
    total_bytes: u64,
    byte_difference: u64,
}

impl ExperimentResult {
    fn print_csv_header() {
        println!("\n\nn;m;bucket_size;bit_width;total_bytes;byte_diff");
    }

    fn print_csv(&self) {
        println!(
            "{};{};{};{};{};{}",
            self.n,
            self.m,
            self.bucket_size,
            self.bit_width,
            self.total_bytes,
            self.byte_difference
        )
    }
}

fn experiment<const BUCKET_SIZE: usize>(bit_width: u32, n: usize, m: usize) -> ExperimentResult {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, BUCKET_SIZE> =
        Hamt::new_with_bit_width(&store, bit_width);
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
        bucket_size: BUCKET_SIZE,
        bit_width,
        total_bytes,
        byte_difference,
    };

    result
}

#[test]
fn experiment_avg_node_degree() {
    let avg = total_avg_node_degree::<BUCKET_SIZE>(4, 100_000);
    println!("{:#?}", avg);
    println!("{}", avg.links_per_node());
    println!("{}", avg.values_per_node());
}

#[derive(Clone, Debug)]
struct Averages {
    nodes: u64,
    links: u64,
    min_degree: u64,
    max_degree: u64,
    values: u64,
}

impl Averages {
    fn links_per_node(&self) -> f64 {
        self.links as f64 / self.nodes as f64
    }

    fn values_per_node(&self) -> f64 {
        self.values as f64 / self.nodes as f64
    }
}

impl AddAssign for Averages {
    fn add_assign(&mut self, rhs: Self) {
        self.nodes += rhs.nodes;
        self.links += rhs.nodes;
        self.values += rhs.values;
        self.min_degree = cmp::min(self.min_degree, rhs.min_degree);
        self.max_degree = cmp::max(self.max_degree, rhs.max_degree);
    }
}

fn total_avg_node_degree<const BUCKET_SIZE: usize>(bit_width: u32, n: usize) -> Averages {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, BUCKET_SIZE> =
        Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }

    avg_node_degree(&map.root, &store)
}

fn avg_node_degree<S, K, V, H, const BUCKET_SIZE: usize>(
    node: &Node<K, V, H, BUCKET_SIZE>,
    store: &S,
) -> Averages
where
    K: Hash + Eq + PartialOrd + DeserializeOwned,
    H: HashAlgorithm,
    V: DeserializeOwned,
    S: Blockstore,
{
    let mut avg = Averages {
        nodes: 1,
        links: 0,
        values: 0,
        min_degree: 0,
        max_degree: 0,
    };

    let mut degree: u64 = 0;

    for pointer in node.pointers.iter() {
        match pointer {
            Pointer::Values(v) => {
                avg.values += v.len() as u64;
            }
            Pointer::Link { cid, cache } => {
                degree += 1;
                if let Some(child_node) = resolve_link(cid, cache, store) {
                    avg += avg_node_degree(child_node, store);
                }
            }
            Pointer::Dirty(child_node) => {
                degree += 1;
                avg += avg_node_degree(child_node, store);
            }
        }
    }

    avg.links += degree;
    avg.min_degree = cmp::min(avg.min_degree, degree);
    avg.max_degree = cmp::max(avg.max_degree, degree);

    avg
}

fn resolve_link<'a, S, K, V, H, const BUCKET_SIZE: usize>(
    cid: &Cid,
    cache: &'a OnceCell<Box<Node<K, V, H, BUCKET_SIZE>>>,
    store: &'a S,
) -> Option<&'a Node<K, V, H, BUCKET_SIZE>>
where
    K: Hash + Eq + PartialOrd + DeserializeOwned,
    H: HashAlgorithm,
    V: DeserializeOwned,
    S: Blockstore,
{
    if let Some(cached_node) = cache.get() {
        return Some(cached_node);
    } else {
        let node = if let Some(node) = store.get_cbor(cid).unwrap() {
            node
        } else {
            return None;
        };

        // Ignore error intentionally, the cache value will always be the same
        let cache_node = cache.get_or_init(|| node);
        return Some(cache_node);
    }
}

enum Resolved<'a, K, V, H, const BUCKET_SIZE: usize> {
    Link(&'a Node<K, V, H, BUCKET_SIZE>),
    Bucket(&'a Vec<KeyValuePair<K, V>>),
}

fn resolved<'a, S, K, V, H, const BUCKET_SIZE: usize>(
    pointer: &'a Pointer<K, V, H, BUCKET_SIZE>,
    store: &'a S,
) -> Resolved<'a, K, V, H, BUCKET_SIZE>
where
    K: Hash + Eq + PartialOrd + DeserializeOwned,
    H: HashAlgorithm,
    V: DeserializeOwned,
    S: Blockstore,
{
    match pointer {
        Pointer::Values(v) => Resolved::Bucket(v),
        Pointer::Link { cid, cache } => {
            if let Some(node) = resolve_link(cid, cache, store) {
                Resolved::Link(node)
            } else {
                unreachable!()
            }
        }
        Pointer::Dirty(node) => Resolved::Link(node),
    }
}

struct Dot {
    nodes: Vec<String>,
    vertices: Vec<(String, String)>,
}

impl Dot {
    fn new() -> Self {
        Dot {
            nodes: Vec::new(),
            vertices: Vec::new(),
        }
    }

    fn extend(&mut self, other: Self) {
        self.nodes.extend(other.nodes);
        self.vertices.extend(other.vertices);
    }
}

fn cidstr(cid: &Cid) -> String {
    let str = cid.to_string();
    str[str.len() - 8..str.len()].to_string()
}

fn bitfieldstr(bitfield: Bitfield, len: u32) -> String {
    let mut str = String::new();
    for i in 0..len {
        if bitfield.test_bit(i) {
            str += "1";
        } else {
            str += "0";
        }
    }
    str
}

fn hamt_to_dot<S, K, V, H, const BUCKET_SIZE: usize>(hamt: &Hamt<S, K, V, H, BUCKET_SIZE>) -> Dot
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned + ToString,
    H: HashAlgorithm,
    V: Serialize + DeserializeOwned + Hash + Eq + PartialOrd + ToString,
    S: Blockstore + Clone,
{
    node_to_dot(&hamt.root, &mut hamt.store().clone(), hamt.bit_width).0
}

fn node_to_dot<S, K, V, H, const BUCKET_SIZE: usize>(
    node: &Node<K, V, H, BUCKET_SIZE>,
    store: &mut S,
    bit_width: u32,
) -> (Dot, Cid)
where
    K: Hash + Eq + PartialOrd + Serialize + DeserializeOwned + ToString,
    H: HashAlgorithm,
    V: Serialize + DeserializeOwned + ToString,
    S: Blockstore + Clone,
{
    let mut dot = Dot::new();

    use cid::multihash::Code;

    let node_cid = store.put_cbor(&node, Code::Blake2b256).unwrap();
    let from = cidstr(&node_cid);

    let mut node_str = format!(
        "\"{from}\" [
    label=<
        <table border=\"0\" cellborder=\"1\" cellspacing=\"0\">
            <tr><td colspan=\"{BUCKET_SIZE}\">{}</td></tr>
            <tr><td colspan=\"{BUCKET_SIZE}\">{}</td></tr>\n",
        from.clone(),
        bitfieldstr(node.bitfield, 1 << bit_width)
    );

    for pointer in node.pointers.iter() {
        match resolved(pointer, &store.clone()) {
            Resolved::Bucket(bucket) => {
                node_str += format!(
                    "<tr>{}</tr>",
                    bucket
                        .iter()
                        .map(|kv| format!(
                            "<td align=\"left\"><font face=\"mono\">{}:</font> {}</td>",
                            hex::encode(H::hash(kv.key()))[..8].to_string(),
                            kv.key().to_string()
                        ))
                        .collect::<Vec<String>>()
                        .join(", ")
                )
                .as_str();
            }
            Resolved::Link(child_node) => {
                let (child_dot, child_cid) = node_to_dot(child_node, store, bit_width);
                dot.extend(child_dot);
                let to = cidstr(&child_cid);
                dot.vertices.push((from.clone(), to));
            }
        }
    }

    node_str += "        </table>
    >
]";

    dot.nodes.push(node_str);

    (dot, node_cid)
}

fn test_hamt_dot() {
    let bit_width = 4;
    let n = 300;
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, 3> = Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }
    map.flush().unwrap();

    println!("digraph G {{");
    println!(
        "\n  compound = true
  fontname = \"Helvetica\"

  edge [
    colorscheme = \"piyg11\"
    fontname = \"Helvetica\"
  ];

  node [
    shape = plaintext
    style = filled
    colorscheme = \"piyg11\"
    fontname = \"Helvetica\"

    color = 2
    fontcolor = 2
    fillcolor = 5
  ];

  graph [
    colorscheme = \"piyg11\"
    color = 10
    style = \"rounded,filled\"
    fontcolor = 7
  ];\n"
    );
    let dot = hamt_to_dot(&map);
    for node in dot.nodes {
        println!("{node}");
    }
    for (from, to) in dot.vertices {
        println!("  \"{from}\" -> \"{to}\"");
    }
    println!("}}");
}

#[test]
fn test_avg_node_bytes() {
    for i in 1..=1000 {
        let n = 100 * i;
        println!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}",
            n,
            avg_node_bytes_experiment::<1>(4, n) as u32,
            avg_node_bytes_experiment::<2>(4, n) as u32,
            avg_node_bytes_experiment::<3>(4, n) as u32,
            avg_node_bytes_experiment::<5>(4, n) as u32,
            avg_node_bytes_experiment::<8>(4, n) as u32,
            avg_node_bytes_experiment::<12>(4, n) as u32,
            avg_node_bytes_experiment::<16>(4, n) as u32,
            avg_node_bytes_experiment::<32>(4, n) as u32,
            avg_node_bytes_experiment::<64>(4, n) as u32,
            avg_node_bytes_experiment::<128>(4, n) as u32,
        );
    }
}

#[cfg(test)]
fn avg_node_bytes_experiment<const BUCKET_SIZE: usize>(bit_width: u32, n: usize) -> f64 {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, BUCKET_SIZE> =
        Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }
    map.flush().unwrap();

    store.bytes_average()
}

#[test]
fn test_max_node_bytes() {
    for i in 1..=1000 {
        let n = 100 * i;
        println!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}",
            n,
            max_node_bytes_experiment::<1>(4, n),
            max_node_bytes_experiment::<2>(4, n),
            max_node_bytes_experiment::<3>(4, n),
            max_node_bytes_experiment::<5>(4, n),
            max_node_bytes_experiment::<8>(4, n),
            max_node_bytes_experiment::<12>(4, n),
            max_node_bytes_experiment::<16>(4, n),
            max_node_bytes_experiment::<32>(4, n),
            max_node_bytes_experiment::<64>(4, n),
            max_node_bytes_experiment::<128>(4, n),
        );
    }
}

#[cfg(test)]
fn max_node_bytes_experiment<const BUCKET_SIZE: usize>(bit_width: u32, n: usize) -> usize {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, BUCKET_SIZE> =
        Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }
    map.flush().unwrap();

    store.bytes_max()
}

#[test]
fn test_merkle_proof_bytes() {
    for i in 1..=10 {
        let n = 10_000 * i;
        println!(
            "{}; {}; {}; {}; {}; {}; {}; {}; {}; {}; {}",
            n,
            merkle_proof_bytes_experiment::<1>(4, n),
            merkle_proof_bytes_experiment::<2>(4, n),
            merkle_proof_bytes_experiment::<3>(4, n),
            merkle_proof_bytes_experiment::<5>(4, n),
            merkle_proof_bytes_experiment::<8>(4, n),
            merkle_proof_bytes_experiment::<12>(4, n),
            merkle_proof_bytes_experiment::<16>(4, n),
            merkle_proof_bytes_experiment::<32>(4, n),
            merkle_proof_bytes_experiment::<64>(4, n),
            merkle_proof_bytes_experiment::<128>(4, n)
        );
    }
}

#[cfg(test)]
fn merkle_proof_bytes_experiment<const BUCKET_SIZE: usize>(bit_width: u32, n: usize) -> u64 {
    let store = MemoryDB::default();
    let mut map: Hamt<_, _, usize, Sha256, BUCKET_SIZE> =
        Hamt::new_with_bit_width(&store, bit_width);
    let value = "F";

    for key in 0..n {
        map.set(key, value.to_string()).unwrap();
    }
    map.flush().unwrap();

    let bytes_before = store.bytes_stored();

    map.set(0, "N".to_string()).unwrap();
    map.flush().unwrap();

    let bytes_after = store.bytes_stored();
    bytes_after - bytes_before
}
