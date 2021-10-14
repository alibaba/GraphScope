use crate::db::api::GraphResult;

pub mod rocksdb;
use self::rocksdb::RocksDBIter;


pub trait ExternalStorage {
    fn get(&self, key: &[u8]) -> GraphResult<Option<StorageRes>>;
    fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()>;
    fn delete(&self, key: &[u8]) -> GraphResult<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter>;
    fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter>;
    fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter>;
    fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()>;
    fn load(&self, files: &[&str]) -> GraphResult<()>;
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
