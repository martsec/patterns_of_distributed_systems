#![allow(dead_code, unused, unused_imports)]
use glob::glob;
use rkyv::{access, rancor::Failure};
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::io::{self, IoSlice, Write};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::abort;
use std::{collections::HashMap, fs::File};
use std::{fs, mem};

use super::simple_wal::WriteAheadLog;
use super::{WalEntry, WalEntryWithHeader, WalError, WalFrame, WalResult};

const GENERATION: u64 = 0;
const HEADER_LEN: usize = 8 /*index*/ + 8 /*generation*/ + 4 /*blob len*/;

#[derive(Debug)]
struct WalSegment {
    start_index: u64,
    file: File,
}

impl WalSegment {
    fn new(prefix: &str, start_index: u64) -> WalResult<Self> {
        let path = Self::file_name(prefix, start_index);
        Ok(Self {
            file: open_file(&path, false)?,
            start_index,
        })
    }

    fn open(path: &str) -> WalResult<Self> {
        let start_index = Self::start_offset_from_file_name(path)?;
        Ok(Self {
            start_index,
            file: open_file(path, false)?,
        })
    }

    fn file_name(prefix: &str, start_index: u64) -> String {
        format!("{}_{}{}", prefix, start_index, ".log")
    }

    fn start_offset_from_file_name(file_name: &str) -> WalResult<u64> {
        let s = file_name.split("_").last();
        match s {
            None => Err(WalError::ShouldNotHappen),
            Some(idx) => idx.parse().map_err(|_| WalError::ShouldNotHappen),
        }
    }

    fn size(&self) -> WalResult<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn flush(&mut self) -> WalResult<()> {
        Ok(self.file.flush()?)
    }

    /// Writes to a log file with the following structure
    ///
    ///┌───────────┬────────────┬───────────┬───────────┐
    ///│ 8-byte =  │ 8-byte =   │ 4-byte =  │ N bytes   │ …
    ///│ log index │ generation │ blob size │ 〈blob〉  │
    ///└───────────┴────────────┴───────────┴───────────┘
    ///
    /// It's not calling flush() constantly since we are not using a BufWriter as of now.
    fn write_entry(&mut self, entry: WalEntryWithHeader) -> WalResult<()> {
        let bytes = entry.to_le_bytes()?;
        self.file.write_all(&bytes)?;
        Ok(())
    }

    /// This reads the individual bytes from file but returns a wrapper around the zero copy data
    fn read_next(&mut self) -> WalResult<Option<WalFrame>> {
        let mut hdr = [0u8; HEADER_LEN];

        let mut read = 0;
        while read < HEADER_LEN {
            let n = self.file.read(&mut hdr[read..])?;
            if n == 0 {
                // EOF before *any* header byte ⇒ log exhausted
                if read == 0 {
                    return Ok(None);
                }
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated WAL header").into(),
                );
            }
            read += n;
        }

        let index = u64::from_le_bytes(hdr[0..8].try_into().expect("Issue with index"));
        let generation = u64::from_le_bytes(hdr[8..16].try_into().expect("Issue with generation"));
        let blob_len =
            u32::from_le_bytes(hdr[16..20].try_into().expect("Issue with blob lenght")) as usize;

        let mut buf = vec![0u8; blob_len];
        if let Err(e) = self.file.read_exact(&mut buf) {
            return Err(e.into());
        }
        Ok(Some(WalFrame {
            buf,
            generation,
            index,
        }))
    }
}

impl Drop for WalSegment {
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

fn open_file(path: &str, truncate: bool) -> WalResult<File> {
    let mut f_opts = File::options();
    f_opts.read(true).write(true).create(true);
    match truncate {
        true => f_opts.truncate(true),
        false => f_opts.append(true),
    };

    Ok(f_opts.open(path)?)
}
struct WalEntryIterator {
    segments: Vec<WalSegment>,
}

impl WalEntryIterator {
    pub fn new(path: &str) -> Self {
        Self {
            segments: Self::open_segments(path).expect("Err"),
        }
    }

    fn open_segments(path: &str) -> WalResult<Vec<WalSegment>> {
        // TODO: accept a starting index
        let open_segment =
            |p: PathBuf| WalSegment::open(p.to_str().ok_or(WalError::ShouldNotHappen)?);
        let mut segments = glob(&format!("{}_*.log", path))?
            .into_iter()
            .map(|p| p.map(open_segment)?)
            .collect::<WalResult<Vec<WalSegment>>>()?;
        // Sort descending so we will pop from the end
        segments.sort_by(|a, b| b.start_index.cmp(&a.start_index));
    }

    pub fn read_next(&mut self) -> WalResult<Option<WalFrame>> {
        match self.segments.last_mut() {
            None => return Ok(None),
            Some(segment) => {
                let next = segment.read_next()?;
                if next.is_some() {
                    return Ok(next);
                }
            }
        }
        // If we arrive here it means we have reached the end of current segment. We should
        // roll to the next one
        let _ = self.segments.pop();
        self.read_next()
    }
}

#[derive(Default, Debug)]
pub struct WALConfig {
    pub path: String,
    pub truncate: bool,
    pub start_index: u64,

    pub max_log_size: u64,
}

#[derive(Debug)]
pub struct SegmentedWal {
    open_segment: WalSegment,
    segments: Vec<WalSegment>,
    last_log_index: u64,
    cfg: WALConfig,
}

impl SegmentedWal {
    /// Opens a new R/W WAL
    ///
    pub fn open(cfg: WALConfig) -> WalResult<Self> {
        if cfg.truncate {
            let path = Path::new(&cfg.path);
            for log in glob(&format!("{}_*.log", &cfg.path))? {
                log.map(|p| fs::remove_file(p))?;
            }
        }
        let mut segments = SegmentedWal::open_segments(&cfg)?;
        let open_segment = segments.pop().ok_or(WalError::ShouldNotHappen)?;
        // TODO: read from last log index???
        Ok(Self {
            last_log_index: 0,
            segments,
            open_segment,
            cfg,
        })
    }

    fn open_segments(cfg: &WALConfig) -> WalResult<Vec<WalSegment>> {
        let open_segment =
            |p: PathBuf| WalSegment::open(p.to_str().ok_or(WalError::ShouldNotHappen)?);
        let segments: Vec<WalSegment> = glob(&format!("{}_*.log", &cfg.path))?
            .into_iter()
            .map(|p| p.map(open_segment)?)
            .collect::<WalResult<Vec<WalSegment>>>()?;

        Ok(match segments.is_empty() {
            true => vec![WalSegment::new(&cfg.path, cfg.start_index)?],
            false => segments,
        })
    }
}

impl SegmentedWal {
    pub fn write(&mut self, cmd: WalEntry) -> WalResult<()> {
        self.maybe_roll()?;

        let index = self.last_log_index + 1;
        let generation = GENERATION;

        let entry = WalEntryWithHeader {
            index,
            generation,
            entry: cmd,
        };
        self.open_segment.write_entry(entry)?;
        self.last_log_index = index;
        Ok(())
    }

    fn maybe_roll(&mut self) -> WalResult<()> {
        if self.open_segment.size()? >= self.cfg.max_log_size {
            // Should we add a message to roll the wal?
            self.open_segment.flush()?;
            // In place replacement
            let replacement = WalSegment::new(&self.cfg.path, self.last_log_index + 1)?;
            let old = mem::replace(&mut self.open_segment, replacement);
            self.segments.push(old);
        }
        Ok(())
    }

    pub fn read_from() {
        todo!()
    }

    pub fn read_next(&mut self) -> WalResult<Option<WalFrame>> {
        // TODO: Iterate since start index
        let wf = self.open_segment.read_next()?;

        if let Some(f) = &wf {
            if f.index > self.last_log_index {
                self.last_log_index = f.index;
            }
        }
        Ok(wf)
    }
}
