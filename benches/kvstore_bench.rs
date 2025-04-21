use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{fs, time::Duration};
use tempfile::NamedTempFile;

use once_cell::sync::Lazy;
use patterns_of_distributed_systems::{KVStore, WriteBatch};

const READ_WAL_PATH: &str = "/tmp/wal-read.log";

fn criterion_config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_secs(10)) // or whatever
        .configure_from_args()
}

/* ---------------------------------------------------------------------
Benchmark 1: 1 000 individual puts
------------------------------------------------------------------ */
fn bench_put_400(c: &mut Criterion) {
    c.bench_function("put_400", |b| {
        b.iter(|| {
            let tmp = NamedTempFile::new().expect("");
            let mut store =
                KVStore::new(true, tmp.path().to_str().expect("")).expect("err with store");

            for i in 0..400 {
                store.put(&format!("k{i}"), "value");
            }

            black_box(store);
        })
    });
}

/* ---------------------------------------------------------------------
Benchmark 2: 200 batches × 3 elements
------------------------------------------------------------------ */
fn bench_batch_200x3(c: &mut Criterion) {
    c.bench_function("batch_200x3", |b| {
        b.iter(|| {
            let tmp = NamedTempFile::new().expect("");
            let mut store =
                KVStore::new(true, tmp.path().to_str().expect("")).expect("err with store");

            for batch_idx in 0..200 {
                let mut batch = WriteBatch::default();
                for item_idx in 0..3 {
                    batch.put(&format!("k{}_{}", batch_idx, item_idx), "value");
                }
                store.put_batch(batch);
            }

            black_box(store);
        })
    });
}

/* ---------------------------------------------------------------------
Benchmark 3: replaying an *existing* WAL
------------------------------------------------------------------ */
static PREPARED: Lazy<()> = Lazy::new(|| {
    // Build a WAL once (same size as benchmark 1) so each iteration only
    // measures the *read* path.
    let _ = fs::remove_file(READ_WAL_PATH); // ignore error if missing
    let mut store =
        KVStore::new(true /* truncate */, READ_WAL_PATH).expect("Error with opening store");
    for i in 0..500 {
        store.put(&format!("k{i}"), "value");
    }
});

fn bench_read_existing(c: &mut Criterion) {
    c.bench_function("read_existing", |b| {
        // Ensure WAL exists exactly once
        Lazy::force(&PREPARED);

        b.iter(|| {
            let store = KVStore::open(READ_WAL_PATH).expect("");
            black_box(store);
        })
    });
}

/* --------------------------------------------------------------------- */
criterion_group! {
    name = kvstore_benches;
    config = criterion_config();
    targets = bench_put_400, bench_batch_200x3, bench_read_existing
}

criterion_main!(kvstore_benches);
