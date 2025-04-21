use std::collections::HashMap;

use crate::wal::{ArchivedWalEntry, WALConfig, WalEntry, WriteAheadLog};

#[derive(Debug)]
pub struct KVStore {
    kv: HashMap<String, String>,
    wal: WriteAheadLog,
}

impl KVStore {
    pub fn new(truncate: bool, file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = WALConfig {
            path: file.into(),
            truncate,
        };
        Self::from_walcfg(cfg)
    }

    pub fn open(file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = WALConfig {
            path: file.into(),
            truncate: false,
        };
        Self::from_walcfg(cfg)
    }

    fn from_walcfg(cfg: WALConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let wal = WriteAheadLog::open(cfg)?;
        let mut store = Self {
            wal,
            kv: HashMap::default(),
        };

        store.apply_log()?;
        Ok(store)
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

#[cfg(test)]
mod tests {

    use super::{KVStore, WriteBatch};
    use tempfile::NamedTempFile;

    fn get_store(file: &NamedTempFile) -> KVStore {
        KVStore::open(file.path().to_str().expect("")).expect("")
    }

    #[test]
    fn empty_store_returns_none() {
        let tmp = NamedTempFile::new().expect("");
        let store = KVStore::new(true, tmp.path().to_str().expect("")).expect("");
        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn put_and_get_roundtrip() {
        let tmp = NamedTempFile::new().expect("");
        let mut store = get_store(&tmp);

        store.put("foo", "bar");
        assert_eq!(store.get("foo"), Some(&"bar".to_string()));

        // Overwrite
        store.put("foo", "baz");
        assert_eq!(store.get("foo"), Some(&"baz".to_string()));
    }

    #[test]
    fn batch_put_extend_store() {
        let tmp = NamedTempFile::new().expect("");
        let mut store = get_store(&tmp);

        let mut batch = WriteBatch::default();
        batch.put("k1", "v1");
        batch.put("k2", "v2");
        batch.put("k3", "v3");

        store.put_batch(batch);

        assert_eq!(store.get("k1"), Some(&"v1".to_string()));
        assert_eq!(store.get("k2"), Some(&"v2".to_string()));
        assert_eq!(store.get("k3"), Some(&"v3".to_string()));
    }

    #[test]
    fn wal_persists_between_sessions() {
        let tmp = NamedTempFile::new().expect("");
        {
            let mut store = get_store(&tmp);
            store.put("a", "1");

            let mut batch = WriteBatch::default();
            batch.put("b", "2");
            batch.put("c", "3");
            store.put_batch(batch);
        }

        let store = get_store(&tmp);

        assert_eq!(store.get("a"), Some(&"1".to_string()));
        assert_eq!(store.get("b"), Some(&"2".to_string()));
        assert_eq!(store.get("c"), Some(&"3".to_string()));
    }

    #[test]
    fn batch_and_single_put_mix_order() {
        let tmp = NamedTempFile::new().expect("");
        let mut store = get_store(&tmp);
        // start with batch
        let mut batch = WriteBatch::default();
        batch.put("b1", "x");
        batch.put("b2", "y");
        store.put_batch(batch);

        // then single
        store.put("single", "z");

        assert_eq!(store.get("b1"), Some(&"x".to_string()));
        assert_eq!(store.get("b2"), Some(&"y".to_string()));
        assert_eq!(store.get("single"), Some(&"z".to_string()));
    }

    #[test]
    fn overwrite_after_reopen() {
        let tmp = NamedTempFile::new().expect("");
        {
            let mut store = get_store(&tmp);
            store.put("dup", "old");
        }
        {
            let mut store = get_store(&tmp);
            assert_eq!(store.get("dup"), Some(&"old".to_string()));
            store.put("dup", "new");
            assert_eq!(store.get("dup"), Some(&"new".to_string()));
        }
        {
            let store = get_store(&tmp);
            assert_eq!(store.get("dup"), Some(&"new".to_string()));
        }
    }

    #[test]
    fn batch_put_empty_is_noop() {
        let tmp = NamedTempFile::new().expect("");
        let mut store = get_store(&tmp);

        let batch = WriteBatch::default(); // empty
        store.put_batch(batch);

        assert!(store.get("anything").is_none());
    }
    #[test]
    fn overrides_existing_file_if_new() {
        let tmp = NamedTempFile::new().expect("");
        {
            let mut store =
                KVStore::new(true, tmp.path().to_str().expect("")).expect("err with store");
            store.put("hello", "world");
        }
        let store2 = KVStore::new(true, tmp.path().to_str().expect("")).expect("err with store");

        assert!(store2.get("hello").is_none());
    }
}
