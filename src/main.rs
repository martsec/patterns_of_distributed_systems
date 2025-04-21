use std::error::Error;

use patterns_of_distributed_systems::{KVStore, WriteBatch};

fn main() -> Result<(), Box<dyn Error>> {
    let path = "/tmp/kv-wal.log";
    {
        let mut kvstore = KVStore::new(true, path)?;

        kvstore.put("Hello", "World");

        let mut batch = WriteBatch::default();
        batch.put("b1", "plai");
        batch.put("b2", "cards");
        kvstore.put_batch(batch);
    }
    let kv2 = KVStore::open(path)?;

    println!("{:?}", kv2);

    Ok(())
}
