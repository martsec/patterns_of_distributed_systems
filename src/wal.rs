#![allow(dead_code, unused, unused_imports)]
use rkyv::{access, rancor::Failure};
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::io::{self, Write};
use std::io::{Read, Seek, SeekFrom};
use std::process::abort;
use std::{collections::HashMap, fs::File};

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum WalEntry {
    Set(String, String),
    Batch(HashMap<String, String>),
}

impl WalEntry {
    fn serialize(&self) -> rkyv::util::AlignedVec {
        rkyv::to_bytes::<Error>(self).expect("serialize WalEntry")
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
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
pub struct WALConfig {
    pub path: String,
    pub truncate: bool,
}

#[derive(Debug)]
pub struct WriteAheadLog {
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
        let mut archive_lenght_marker = [0u8; 4];

        loop {
            let buf = self.read_next();
            match buf {
                Err(e) => out.push(Err(e)),
                Ok(Some(wf)) => {
                    let deser = WalEntry::deserialize(&wf.buf);
                    out.push(deser);
                }
                Ok(None) => break,
            }
        }

        out
    }

    /// This reads the individual bytes from file but returns a wrapper around the zero copy data
    pub fn read_next(&mut self) -> Result<Option<WalFrame>, Box<dyn std::error::Error>> {
        let mut archive_lenght_marker = [0u8; 4];
        // 1. Read the length prefix
        match self.file.read_exact(&mut archive_lenght_marker) {
            Ok(()) => {} // Read ok
            Err(e) => return Ok(None),
        }
        let lenght_of_archive = u32::from_le_bytes(archive_lenght_marker) as usize;

        // 2. Pull the archive itself
        let mut buf = vec![0u8; lenght_of_archive];
        if let Err(e) = self.file.read_exact(&mut buf) {
            return Err(Box::new(e));
        }
        Ok(Some(WalFrame { buf }))
    }
}

impl Drop for WriteAheadLog {
    /// Safeguard against "safe" exits.
    ///
    /// Does not work in external kill signals like sigkill, oom, power loss, segfault...
    fn drop(&mut self) {
        if let Err(e) = self.file.flush() {
            eprintln!("WAL: failed to flush on drop: {e}");
            // TODO: return this as a critical error in the error stack
        }
    }
}

// Contains the entry bytes and it's zero copy
pub struct WalFrame {
    pub buf: Vec<u8>,
}

impl WalFrame {
    pub fn zero_copy(&self) -> Result<&ArchivedWalEntry, Box<dyn std::error::Error>> {
        WalEntry::zero_copy(&self.buf)
    }
}
