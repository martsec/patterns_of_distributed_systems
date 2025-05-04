pub mod segmented_log;
pub mod simple_wal;
pub mod wal;

use rkyv::rancor::Failure;
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("failed to serialize WAL entry: {0}")]
    Serialization(#[from] rkyv::rancor::Error),
    #[error("failed to deserialize WAL entry: {0}")]
    Deserialization(#[from] rkyv::rancor::Failure),
    #[error("failure in log file: {0}")]
    IO(#[from] std::io::Error),
    #[error("failure truncating old wal files: {0}")]
    Truncate(#[from] glob::PatternError),
    #[error("failure truncating old wal files: {0}")]
    Glob(#[from] glob::GlobError),
    #[error("This should not happen")]
    ShouldNotHappen,
}

pub type WalResult<T> = std::result::Result<T, WalError>;

struct WalEntryWithHeader {
    index: u64,
    generation: u64,
    entry: WalEntry,
}

impl WalEntryWithHeader {
    fn to_le_bytes(self) -> WalResult<Vec<u8>> {
        // TODO: use arenas for more efficient memory management
        // https://docs.rs/rkyv/latest/rkyv/api/high/fn.to_bytes_with_alloc.html
        let mut buf = Vec::new();
        // header placeholder:
        buf.reserve(20);
        buf.extend_from_slice(&[0u8; 20]);
        {
            buf.extend_from_slice(&self.entry.serialize()?);
        }
        let blob_len = (buf.len() - 20) as u32;
        buf[0..8].copy_from_slice(&self.index.to_le_bytes());
        buf[8..16].copy_from_slice(&self.generation.to_le_bytes());
        buf[16..20].copy_from_slice(&blob_len.to_le_bytes());
        Ok(buf)
    }
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum WalEntry {
    Set(String, String),
    Batch(HashMap<String, String>),
}

impl WalEntry {
    fn serialize(&self) -> WalResult<rkyv::util::AlignedVec> {
        Ok(rkyv::to_bytes::<Error>(self)?)
    }

    fn deserialize(bytes: &[u8]) -> WalResult<Self> {
        // This is not too efficient since we are deserializing and thus copying data
        // We Should pass around the archived reference
        let archived = rkyv::access::<ArchivedWalEntry, Failure>(bytes)?;
        Ok(rkyv::deserialize::<WalEntry, Error>(archived)?)
    }
    fn zero_copy(bytes: &[u8]) -> WalResult<&ArchivedWalEntry> {
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
    pub fn zero_copy(&self) -> WalResult<&ArchivedWalEntry> {
        WalEntry::zero_copy(&self.buf)
    }
}
