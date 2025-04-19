use std::error::Error;

use kv_store::{KVStore, WriteBatch};

mod kv_store;
mod wal;
fn main() -> Result<(), Box<dyn Error>> {
    let mut kvstore = KVStore::new(true)?;

    kvstore.put("Hello", "World");

    let mut batch = WriteBatch::default();
    batch.put("b1", "plai");
    batch.put("b2", "cards");
    kvstore.put_batch(batch);

    let kv2 = KVStore::open()?;

    println!("{:?}", kv2);

    Ok(())
}
