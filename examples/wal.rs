#![allow(dead_code, unused, unused_imports)]
use rkyv::{access, rancor::Failure};
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::io::{self, Write};
use std::io::{Read, Seek, SeekFrom};
use std::{collections::HashMap, fs::File};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut kv = KVStore::new(true)?;
    kv.put("hello", "world");
    //kv.put(
    //    "PLAI",
    //    "Is a card game for developers and people working in tech. It's sarcastic.",
    //);
    //kv.put("nyx", "is a small female cat, godess of the night");
    std::mem::drop(kv);

    println!("\n\n\nInitializing a new store, reading from WAL");
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
        self.append_log(key, value);
        self.put_no_log(key, value);
    }

    fn put_no_log(&mut self, key: &str, value: &str) {
        self.kv.insert(key.into(), value.into());
    }

    fn append_log(&mut self, key: &str, value: &str) {
        self.wal.write(SetValueCommand {
            key: key.into(),
            value: value.into(),
        });
    }

    /// Reads content from WAL and applies it to the state
    fn apply_log(&mut self) {
        let wal_entries = self.wal.read();
        for result in wal_entries {
            match result {
                Err(e) => println!("Error reading: {:?}", e),
                Ok(cmd) => self.put_no_log(&cmd.key, &cmd.value),
            }
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug)]
struct SetValueCommand {
    key: String,
    value: String,
}

impl SetValueCommand {
    fn serialize(&self) -> rkyv::util::AlignedVec {
        rkyv::to_bytes::<Error>(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let a = access::<ArchivedSetValueCommand, Failure>(bytes)?;
        Ok(deserialize::<Self, Error>(a)?)
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
    pub fn write(&mut self, cmd: SetValueCommand) -> Result<(), std::io::Error> {
        let blob = cmd.serialize();
        let blob_len = blob.len() as u32;

        self.file.write_all(&blob_len.to_le_bytes())?;
        self.file.write_all(&blob)?;
        // TODO: this is not optimal since we are reading constantly & is not even a guarantee
        //self.file.flush()?;
        println!("\tAppending log {:?}", cmd);
        Ok(())
    }

    pub fn read(&mut self) -> Vec<Result<SetValueCommand, Box<dyn std::error::Error + 'static>>> {
        // Make sure we start at the beginning.
        if let Err(e) = self.file.rewind() {
            return vec![Err(Box::new(e))];
        }

        let mut out: Vec<Result<SetValueCommand, Box<dyn std::error::Error>>> = Vec::new();
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
            let deser = SetValueCommand::deserialize(&buf);
            out.push(deser);
        }

        out
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        println!("  --> Flushing");
        if let Err(e) = self.file.flush() {
            eprintln!("WAL: failed to flush on drop: {e}");
            // TODO: return this as a critical error in the error stack
        }
    }
}
