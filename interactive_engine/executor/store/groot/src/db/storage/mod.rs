use crate::db::api::{BackupId, GraphResult};

pub mod rocksdb;
use std::ptr::null;

use self::rocksdb::RocksDBIter;

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
    fn new_scan(&self, prefix: &[u8]) -> GraphResult<Box<dyn Iterator<Item = KvPair> + Send>>;
    fn try_catch_up_with_primary(&self) -> GraphResult<()>;
    fn reopen(&self) -> GraphResult<()>;
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
        let tmp = slice.to_vec();
        let ptr = tmp.as_ptr();
        let len = tmp.len();
        ::std::mem::forget(tmp);
        RawBytes { ptr, len }
    }

    pub fn empty() -> Self {
        RawBytes { ptr: null(), len: 0 }
    }

    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for RawBytes {
    fn drop(&mut self) {
        if self.len > 0 {
            unsafe {
                Vec::from_raw_parts(self.ptr as *mut u8, self.len, self.len);
            }
        }
    }
}

impl<'a> Iterator for StorageIter<'a> {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StorageIter::RocksDB(ref mut iter) => iter
                .next()
                .map(|(k, v)| (RawBytes::new(k), RawBytes::new(v))),
        }
    }
}
