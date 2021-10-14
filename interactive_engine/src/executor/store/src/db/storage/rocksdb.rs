use ::rocksdb::{DB, Options, ReadOptions, DBRawIterator, IngestExternalFileOptions};
use std::collections::HashMap;
use std::path::Path;

use crate::db::api::*;
use super::{StorageIter, StorageRes, ExternalStorage};

pub struct RocksDB {
    db: DB,
}

impl RocksDB {
    pub fn open(options: &HashMap<String, String>, path: &str) -> GraphResult<Self> {
        let opts = init_options(options);
        let db = DB::open(&opts, path).map_err(|e| {
            let msg = format!("open rocksdb at {} failed, because {}", path, e.into_string());
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
                let msg = format!("rocksdb.get failed because {}", e.into_string());
                let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
                Err(err)
            }
        }
    }

    fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()> {
        self.db.put(key, val).map_err(|e| {
            let msg = format!("rocksdb.put failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.db.delete(key).map_err(|e| {
            let msg = format!("rocksdb.delete failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter> {
        let mut iter = match bytes_upper_bound(prefix) {
            Some(upper) => {
                let mut option = ReadOptions::default();
                option.set_iterate_upper_bound(upper);
                self.db.raw_iterator_opt(option)
            }
            None => self.db.raw_iterator(),
        };
        iter.seek(prefix);
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter> {
        let mut iter = self.db.raw_iterator();
        iter.seek(start);
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter> {
        let mut option = ReadOptions::default();
        option.set_iterate_upper_bound(end.to_vec());
        let mut iter = self.db.raw_iterator_opt(option);
        iter.seek(start);
        Ok(StorageIter::RocksDB(RocksDBIter::new(iter)))
    }

    fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()> {
        let handle = self.db.cf_handle("default").unwrap();
        self.db.delete_range_cf(handle, start, end).map_err(|e| {
            let msg = format!("rocksdb.delete_range failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn load(&self, files: &[&str]) -> GraphResult<()> {
        let mut options = IngestExternalFileOptions::default();
        options.set_move_files(true);
        let paths : Vec<&Path> = files.to_vec().into_iter().map(|f| Path::new(f)).collect();
        self.db.ingest_external_file_opts(&options, paths).map_err(|e| {
            let msg = format!("rocksdb.ingest_sst failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }
}

#[allow(unused_variables)]
fn init_options(options: &HashMap<String, String>) -> Options {
    let mut ret = Options::default();
    ret.create_if_missing(true);
    // TODO: Add other customized db options.
    ret
}


pub struct RocksDBIter<'a> {
    inner: DBRawIterator<'a>,
    just_seeked: bool,
}

impl<'a> RocksDBIter<'a> {
    fn new(iter: DBRawIterator<'a>) -> Self {
        RocksDBIter {
            inner: iter,
            just_seeked: true,
        }
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if !self.inner.valid() {
            return None;
        }
        if self.just_seeked {
            self.just_seeked = false;
        } else {
            self.inner.next();
        }
        Some((self.inner.key().unwrap(), self.inner.value().unwrap()))
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
