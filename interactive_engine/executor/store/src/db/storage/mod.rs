use crate::db::api::{GraphResult, BackupId};

pub mod rocksdb;
use self::rocksdb::RocksDBIter;
use std::ptr::null;

pub trait ExternalStorage: Send + Sync {
    fn get(&self, key: &[u8]) -> GraphResult<Option<StorageRes>>;
    fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()>;
    fn delete(&self, key: &[u8]) -> GraphResult<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter>;
    fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter>;
    fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter>;
    fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()>;
    fn load(&self, files: &[&str]) -> GraphResult<()>;
    fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<dyn ExternalStorageBackup>>;
    fn new_scan(&self, prefix: &[u8]) -> GraphResult<Box<dyn Iterator<Item=KvPair> + Send>>;
}

pub trait ExternalStorageBackup {
    fn create_new_backup(&mut self) -> GraphResult<BackupId>;
    fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()>;
    fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()>;
    fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()>;
    fn get_backup_list(&self) -> Vec<BackupId>;
}

pub enum StorageRes {
    RocksDB(Vec<u8>),
}

impl StorageRes {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StorageRes::RocksDB(v) => v,
        }
    }
}

pub enum StorageIter<'a> {
    RocksDB(RocksDBIter<'a>),
}

impl<'a> StorageIter<'a> {
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        match *self {
            StorageIter::RocksDB(ref mut iter) => iter.next(),
        }
    }
}

pub type KvPair = (RawBytes, RawBytes);

pub struct RawBytes {
    ptr: *const u8,
    len: usize,
}

impl RawBytes {
    pub fn new(slice: &[u8]) -> Self {
        RawBytes {
            ptr: slice.as_ptr(),
            len: slice.len(),
        }
    }

    pub fn empty() -> Self {
        RawBytes {
            ptr: null(),
            len: 0,
        }
    }

    pub unsafe fn to_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.ptr, self.len)
    }
}

impl<'a> Iterator for StorageIter<'a> {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StorageIter::RocksDB(ref mut iter) => iter.next().map(|(k, v)| {
                (RawBytes::new(k), RawBytes::new(v))
            }),
        }
    }
}
