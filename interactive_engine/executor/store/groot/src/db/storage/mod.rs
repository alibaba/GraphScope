pub mod rocksdb;
use std::ptr::null;

use self::rocksdb::RocksDBIter;

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
