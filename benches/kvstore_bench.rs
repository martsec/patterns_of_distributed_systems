use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{fs, time::Duration};

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
fn bench_put_1000(c: &mut Criterion) {
    c.bench_function("put_1000", |b| {
        b.iter(|| {
            // truncate=true gives a clean file for every iteration
            let mut store = KVStore::new(true).unwrap();

            for i in 0..1000 {
                store.put(&format!("k{i}"), "value");
            }

            black_box(store); // make sure work isn't optimized away
        })
    });
}

/* ---------------------------------------------------------------------
Benchmark 2: 500 batches × 3 elements
------------------------------------------------------------------ */
fn bench_batch_500x3(c: &mut Criterion) {
    c.bench_function("batch_500x3", |b| {
        b.iter(|| {
            let mut store = KVStore::new(true).unwrap();

            for batch_idx in 0..500 {
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
    let mut store = KVStore::new(true /* truncate */).unwrap();
    for i in 0..1000 {
        store.put(&format!("k{i}"), "value");
    }
    // store dropped here → WAL closed / flushed
});

fn bench_read_existing(c: &mut Criterion) {
    c.bench_function("read_existing", |b| {
        // Ensure WAL exists exactly once
        Lazy::force(&PREPARED);

        b.iter(|| {
            // open(false) replays the log into memory
            let store = KVStore::open().unwrap();
            black_box(store);
        })
    });
}

/* --------------------------------------------------------------------- */
criterion_group! {
    name = kvstore_benches;
    config = criterion_config();
    targets = bench_put_1000, bench_batch_500x3, bench_read_existing
}

criterion_main!(kvstore_benches);
