use ::rocksdb::{DB, ReadOptions, SeekKey, Writable, IngestExternalFileOptions, DBIterator, DBOptions};
use std::collections::HashMap;

use crate::db::api::*;
use super::{StorageIter, StorageRes, ExternalStorage};

pub struct RocksDB {
    db: DB,
}

impl RocksDB {
    pub fn open(options: &HashMap<String, String>, path: &str) -> GraphResult<Self> {
        let opts = init_options(options);
        let db = DB::open(opts, path).map_err(|e| {
            let msg = format!("open rocksdb at {} failed, because {}", path, e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg, open, options, path)
        })?;
        let ret = RocksDB {
            db,
        };
        Ok(ret)
    }
}

impl ExternalStorage for RocksDB {
    fn get(&self, key: &[u8]) -> GraphResult<Option<StorageRes>> {
        match self.db.get(key) {
            Ok(Some(v)) => Ok(Some(StorageRes::RocksDB(v))),
            Ok(None) => Ok(None),
            Err(e) => {
                let msg = format!("rocksdb.get failed because {}", e);
                let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
                Err(err)
            }
        }
    }

    fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()> {
        self.db.put(key, val).map_err(|e| {
            let msg = format!("rocksdb.put failed because {}", e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.db.delete(key).map_err(|e| {
            let msg = format!("rocksdb.delete failed because {}", e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter> {
        let mut iter = match bytes_upper_bound(prefix) {
            Some(upper) => {
                let mut option = ReadOptions::new();
                option.set_iterate_upper_bound(upper);
                self.db.iter_opt(option)
            }
            None => self.db.iter(),
        };
        iter.seek(SeekKey::Key(prefix));
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter> {
        let mut iter = self.db.iter();
        iter.seek(SeekKey::Key(start));
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter> {
        let mut option = ReadOptions::new();
        option.set_iterate_upper_bound(end.to_vec());
        let mut iter = self.db.iter_opt(option);
        iter.seek(SeekKey::Key(start));
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()> {
        self.db.delete_range(start, end).map_err(|e| {
            let msg = format!("rocksdb.delete_range failed because {}", e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn load(&self, files: &[&str]) -> GraphResult<()> {
        let mut options = IngestExternalFileOptions::new();
        options.move_files(true);
        self.db.ingest_external_file(&options, files).map_err(|e| {
            let msg = format!("rocksdb.ingest_sst failed because {}", e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }
}

#[allow(unused_variables)]
fn init_options(options: &HashMap<String, String>) -> DBOptions {
    let mut ret = DBOptions::new();
    ret.create_if_missing(true);
    ret
}


pub struct RocksDBIter<'a> {
    first_item: bool,
    inner: DBIterator<&'a DB>,
}

impl<'a> RocksDBIter<'a> {
    fn new(iter: DBIterator<&'a DB>) -> Self {
        RocksDBIter {
            first_item: true,
            inner: iter,
        }
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if !self.first_item {
            if self.inner.next() {
                return Some((self.inner.key(), self.inner.value()));
            }
        } else if self.inner.valid() {
            self.first_item = false;
            return Some((self.inner.key(), self.inner.value()))
        }
        None
    }
}

fn bytes_upper_bound(bytes: &[u8]) -> Option<Vec<u8>> {
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::max_value() {
            let mut ret = bytes.to_vec();
            ret[i] += 1;
            for j in i+1..bytes.len() {
                ret[j] = 0;
            }
            return Some(ret);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::common::bytes::transform;
    use crate::db::util::fs;

    #[test]
    fn test_rocksdb_iter() {
        let path = "test_rocksdb_iter";
        {
            let db = RocksDB::open(&HashMap::new(), path).unwrap();
            let mut ans = Vec::new();
            for i in 1..=10 {
                let key = format!("aaa#{:010}", i);
                db.put(key.as_bytes(), i.to_string().as_bytes()).unwrap();
                ans.push((key, i));
            }
            let mut iter = db.scan_prefix(b"aaa").unwrap();
            for (key, i) in ans {
                let (k, v) = iter.next().unwrap();
                assert_eq!(key, String::from_utf8(k.to_vec()).unwrap());
                assert_eq!(i, String::from_utf8(v.to_vec()).unwrap().parse::<i32>().unwrap());
            }
            assert!(iter.next().is_none());

            let mut iter = db.scan_prefix(b"zzz").unwrap();
            assert!(iter.next().is_none());
        }
        fs::rmr(path).unwrap();
    }

    #[test]
    fn test_rocksdb_scan_from() {
        let path = "test_rocksdb_scan_from";
        fs::rmr(path).unwrap();
        {
            let db = RocksDB::open(&HashMap::new(), path).unwrap();
            for i in 1..=20 {
                if i % 2 == 0 {
                    let key = format!("aaa#{:010}", i);
                    db.put(key.as_bytes(), transform::i64_to_vec(i).as_slice()).unwrap();
                }
            }

            for i in 1..=20 {
                let key = format!("aaa#{:010}", i);
                let ans = format!("aaa#{:010}", (i + 1) / 2 * 2);
                let mut iter = db.scan_from(key.as_bytes()).unwrap();
                let (k, v) = iter.next().unwrap();
                assert_eq!(k, ans.as_bytes());
                assert_eq!(transform::bytes_to_i64(v).unwrap(), (i + 1) / 2 * 2);
            }
        }
        fs::rmr(path).unwrap();
    }

}
