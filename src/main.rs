pub mod memorydb;

#[cfg(test)]
mod tests;

use fvm_ipld_hamt::{Hamt, Sha256};
use memorydb::MemoryDB;

const BUCKET_SIZE: usize = 32;

fn main() {
    println!("Bucket Size {BUCKET_SIZE}");
    ExperimentResult::print_csv_header();

    let n = 100_000;
    let step_size = 1;
    let window_start = 0;
    let window_end = 100;
    let max_steps = (window_end - window_start) / step_size;
    let mut ms = vec![];

    for x in 0..max_steps {
        ms.push((x + 1) * step_size + window_start);
    }

    for m in ms.iter() {
        println!("{}", experiment::<BUCKET_SIZE>(4, n, *m).byte_difference);
        // experiment::<BUCKET_SIZE>(4, n, *m).print_csv();
    }
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
