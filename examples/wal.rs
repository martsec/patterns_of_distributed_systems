#![allow(dead_code)]
use std::{collections::HashMap, fs::File};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello World");

    let mut kv = KVStore::new()?;
    kv.put("hello", "world");

    println!("{:?}", kv);
    Ok(())
}

#[derive(Debug)]
struct KVStore {
    kv: HashMap<String, String>,
    wal: WriteAheadLog,
}

impl KVStore {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = WALConfig {
            path: "/tmp/wal.log".into(),
        };
        let wal = WriteAheadLog::new(cfg)?;
        Ok(Self {
            wal,
            kv: HashMap::default(),
        })
    }
}

impl KVStore {
    pub fn get(&self, key: &str) -> Option<&String> {
        self.kv.get(key)
    }

    pub fn put(&mut self, key: &str, value: &str) {
        self.append_log(key, value);
        self.kv.insert(key.into(), value.into());
    }

    fn append_log(&mut self, key: &str, value: &str) {
        self.wal.write();
        println!("Appending log");
    }
}

#[derive(Default, Debug)]
struct WALConfig {
    path: String,
}

#[derive(Debug)]
struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    pub fn new(cfg: WALConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let f = File::create_new(cfg.path)?;
        Ok(Self { file: f })
    }
}

impl WriteAheadLog {
    pub fn write(&mut self) {}
}
