use std::collections::HashMap;

use crate::wal::{ArchivedWalEntry, WALConfig, WalEntry, WriteAheadLog};

#[derive(Debug)]
pub struct KVStore {
    kv: HashMap<String, String>,
    wal: WriteAheadLog,
}

impl KVStore {
    pub fn new(truncate: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = WALConfig {
            path: "/tmp/wal.log".into(),
            truncate,
        };
        let wal = WriteAheadLog::open(cfg)?;
        let mut store = Self {
            wal,
            kv: HashMap::default(),
        };

        store.apply_log()?;
        Ok(store)
    }

    pub fn open() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(false)
    }
}

impl Drop for KVStore {
    fn drop(&mut self) {
        // TODO: try to do a snapshot before closing if we are gracefully closing the store.
        // If not, just do nothing. WAL should handle the ir drop.
    }
}

impl KVStore {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.kv.get(key)
    }

    pub fn put(&mut self, key: &str, value: &str) {
        self.append_log(WalEntry::Set(key.into(), value.into()));
        self.apply_put(key, value);
    }

    pub fn put_batch(&mut self, batch: WriteBatch) {
        self.append_log(WalEntry::Batch(batch.elements.clone()));
        self.apply_batch(batch.elements);
    }

    fn append_log(&mut self, entry: WalEntry) {
        let _ = self.wal.write(entry);
    }

    fn apply_put(&mut self, key: &str, value: &str) {
        self.kv.insert(key.into(), value.into());
    }

    fn apply_batch(&mut self, kv: HashMap<String, String>) {
        self.kv.extend(kv);
    }
    /// Reads content from WAL and applies it to the state
    fn apply_log(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(wal_entry) = self.wal.read_next()? {
            match wal_entry.zero_copy()? {
                ArchivedWalEntry::Set(k, v) => self.apply_put(k, v),
                ArchivedWalEntry::Batch(kv) => {
                    // TODO: I should ideally use `self.apply_batch()` ?
                    self.kv.extend(
                        kv.iter()
                            .map(|(k, v)| (k.as_str().to_owned(), v.as_str().to_owned())),
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct WriteBatch {
    elements: HashMap<String, String>,
}

impl WriteBatch {
    pub fn put(&mut self, key: &str, value: &str) {
        self.elements.insert(key.into(), value.into());
    }
}
