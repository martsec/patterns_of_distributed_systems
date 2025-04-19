#![allow(dead_code, unused, unused_imports)]
use rkyv::{access, rancor::Failure};
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::io::{self, Write};
use std::io::{Read, Seek, SeekFrom};
use std::process::abort;
use std::{collections::HashMap, fs::File};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut kv = KVStore::new(true)?;
    let mut batch = WriteBatch::default();
    batch.put("hello", "world");
    batch.put("PLAI", "is a sarcastic card game for developers.");

    kv.put_batch(batch);
    kv.put("nyx", "is a small female cat, godess of the night");
    std::mem::drop(kv);

    println!("\n\nInitializing a new store, reading from WAL");
    let mut kv2 = KVStore::open()?;
    println!("\n\n{:?}", kv2);
    Ok(())
}

#[derive(Debug)]
struct KVStore {
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

        store.apply_log();
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
        self.append_log(WalEntry::Set(SetValueCommand {
            key: key.into(),
            value: value.into(),
        }));
        self.apply_put(key, value);
    }

    pub fn put_batch(&mut self, batch: WriteBatch) {
        self.append_log(WalEntry::Batch(WriteBatchCommand {
            kv: batch.elements.clone(),
        }));
    }

    fn append_log(&mut self, entry: WalEntry) {
        self.wal.write(entry);
    }

    fn apply_put(&mut self, key: &str, value: &str) {
        self.kv.insert(key.into(), value.into());
    }

    fn apply_batch(&mut self, kv: HashMap<String, String>) {
        self.kv.extend(kv);
    }
    /// Reads content from WAL and applies it to the state
    fn apply_log(&mut self) {
        let wal_entries = self.wal.read();
        for result in wal_entries {
            match result {
                Err(e) => println!("Error reading: {:?}", e),
                Ok(cmd) => match cmd {
                    WalEntry::Set(c) => self.apply_put(&c.key, &c.value),
                    WalEntry::Batch(b) => self.apply_batch(b.kv),
                },
            }
        }
    }
}

#[derive(Default, Debug)]
struct WriteBatch {
    elements: HashMap<String, String>,
}

impl WriteBatch {
    fn put(&mut self, key: &str, value: &str) {
        self.elements.insert(key.into(), value.into());
    }
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct SetValueCommand {
    pub key: String,
    pub value: String,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct WriteBatchCommand {
    pub kv: HashMap<String, String>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum WalEntry {
    Set(SetValueCommand),
    Batch(WriteBatchCommand),
}

impl WalEntry {
    fn serialize(&self) -> rkyv::util::AlignedVec {
        rkyv::to_bytes::<Error>(self).expect("serialize WalEntry")
    }

    fn deserialize<'a>(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // This is not too efficient since we are deserializing and thus copying data
        // We Should pass around the archived reference
        let archived = rkyv::access::<ArchivedWalEntry, Failure>(bytes)?;
        Ok(rkyv::deserialize::<WalEntry, Error>(archived)?)
    }
    fn zero_copy(bytes: &[u8]) -> Result<&ArchivedWalEntry, Box<dyn std::error::Error>> {
        Ok(rkyv::access::<ArchivedWalEntry, Failure>(bytes)?)
    }
}

#[derive(Default, Debug)]
struct WALConfig {
    path: String,
    truncate: bool,
}

#[derive(Debug)]
struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    /// Opens a new R/W WAL
    ///
    pub fn open(cfg: WALConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut f_opts = File::options();
        f_opts.read(true).write(true).create(true);
        match cfg.truncate {
            true => f_opts.truncate(true),
            false => f_opts.append(true),
        };

        let f = f_opts.open(&cfg.path)?;
        Ok(Self { file: f })
    }
}

impl WriteAheadLog {
    /// Writes to a log file with the following structure
    ///
    ///┌──────────────┬───────────┐┌──────────────┬───────────┐┌──────────────┬───────────┐
    ///│ 4‑byte len = │  N bytes  ││ 4‑byte len = │  M bytes  ││ 4‑byte len = │  K bytes  │ …
    ///│  first blob  │〈archive〉││ second blob  │〈archive〉││ third blob   │〈archive〉│
    ///└──────────────┴───────────┘└──────────────┴───────────┘└──────────────┴───────────┘
    ///
    /// It's not calling flush() constantly since we are not using a BufWriter as of now.
    pub fn write(&mut self, cmd: WalEntry) -> Result<(), std::io::Error> {
        let blob = cmd.serialize();
        let blob_len = blob.len() as u32;

        self.file.write_all(&blob_len.to_le_bytes())?;
        self.file.write_all(&blob)?;
        println!("\tAppending log {:?}", cmd);
        Ok(())
    }

    pub fn read(&mut self) -> Vec<Result<WalEntry, Box<dyn std::error::Error + 'static>>> {
        // Of course, reading the entire file and sending a Vec is not optimal.
        // We should just have a generator

        // Make sure we start at the beginning.
        if let Err(e) = self.file.rewind() {
            return vec![Err(Box::new(e))];
        }

        let mut out: Vec<Result<WalEntry, Box<dyn std::error::Error>>> = Vec::new();
        let mut len_buf = [0u8; 4];

        loop {
            // 1. Read the length prefix
            match self.file.read_exact(&mut len_buf) {
                Ok(()) => {}
                // Clean EOF – we’re done.
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    out.push(Err(Box::new(e)));
                    break;
                }
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // 2. Pull the archive itself
            let mut buf = vec![0u8; len];
            if let Err(e) = self.file.read_exact(&mut buf) {
                out.push(Err(Box::new(e)));
                break;
            }

            // 3. Validate + deserialize
            let deser = WalEntry::deserialize(&buf);
            out.push(deser);
        }

        out
    }
}

impl Drop for WriteAheadLog {
    /// Safeguard against "safe" exits.
    ///
    /// Does not work in external kill signals like sigkill, oom, power loss, segfault...
    fn drop(&mut self) {
        println!("  --> Flushing");
        if let Err(e) = self.file.flush() {
            eprintln!("WAL: failed to flush on drop: {e}");
            // TODO: return this as a critical error in the error stack
        }
    }
}
