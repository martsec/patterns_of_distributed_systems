#![allow(dead_code, unused, unused_imports)]
use rkyv::{access, rancor::Failure};
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::io::{self, IoSlice, Write};
use std::io::{Read, Seek, SeekFrom};
use std::process::abort;
use std::{collections::HashMap, fs::File};

const GENERATION: u64 = 0;
const HEADER_LEN: usize = 8 /*index*/ + 8 /*generation*/ + 4 /*blob len*/;

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

// Contains the binary file data and some useful metadata.
pub struct WalFrame {
    pub index: u64,
    pub generation: u64,
    pub buf: Vec<u8>,
}

impl WalFrame {
    pub fn zero_copy(&self) -> Result<&ArchivedWalEntry, Box<dyn std::error::Error>> {
        WalEntry::zero_copy(&self.buf)
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
    last_log_index: u64,
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
        // TODO: read from last log index???
        Ok(Self {
            file: f,
            last_log_index: 0,
        })
    }
}

impl WriteAheadLog {
    /// Writes to a log file with the following structure
    ///
    ///┌───────────┬────────────┬───────────┬───────────┐
    ///│ 8-byte =  │ 8-byte =   │ 4-byte =  │ N bytes   │ …
    ///│ log index │ generation │ blob size │ 〈blob〉  │
    ///└───────────┴────────────┴───────────┴───────────┘
    ///
    /// It's not calling flush() constantly since we are not using a BufWriter as of now.
    pub fn write(&mut self, cmd: WalEntry) -> Result<(), std::io::Error> {
        let blob = cmd.serialize();

        let blob_len = blob.len() as u32;
        let new_index = self.last_log_index + 1;
        let generation = GENERATION;
        let mut header = [0u8; 20];
        header[0..8].copy_from_slice(&new_index.to_le_bytes());
        header[8..16].copy_from_slice(&generation.to_le_bytes());
        header[16..20].copy_from_slice(&blob_len.to_le_bytes());

        // Ideally we should just write once, but `write_vectored` might not write the  entire last
        // buffer!!! So better be slow as of now
        self.file.write_all(&header)?;
        self.file.write_all(&blob)?;

        self.last_log_index = new_index;
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
        let mut hdr = [0u8; HEADER_LEN];

        let mut read = 0;
        while read < HEADER_LEN {
            let n = self.file.read(&mut hdr[read..])?;
            if n == 0 {
                // EOF before *any* header byte ⇒ log exhausted
                if read == 0 {
                    return Ok(None);
                }
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated WAL header",
                )));
            }
            read += n;
        }

        let index = u64::from_le_bytes(hdr[0..8].try_into().expect("Issue with index"));
        let generation = u64::from_le_bytes(hdr[8..16].try_into().expect("Issue with generation"));
        let blob_len =
            u32::from_le_bytes(hdr[16..20].try_into().expect("Issue with blob lenght")) as usize;

        let mut buf = vec![0u8; blob_len];
        if let Err(e) = self.file.read_exact(&mut buf) {
            return Err(Box::new(e));
        }

        if index > self.last_log_index {
            self.last_log_index = index;
        }
        Ok(Some(WalFrame {
            buf,
            generation,
            index,
        }))
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
