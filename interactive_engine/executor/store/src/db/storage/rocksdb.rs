use ::rocksdb::{DB, Options, ReadOptions, DBRawIterator, IngestExternalFileOptions};
use ::rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use std::collections::HashMap;
use std::sync::Arc;

use crate::db::api::*;
use super::{StorageIter, StorageRes, ExternalStorage, ExternalStorageBackup};
use crate::db::storage::{KvPair, RawBytes};

pub struct RocksDB {
    db: Arc<DB>,
}

pub struct RocksDBBackupEngine {
    db: Arc<DB>,
    backup_engine: BackupEngine,
}

impl RocksDB {
    pub fn open(options: &HashMap<String, String>, path: &str) -> GraphResult<Self> {
        let opts = init_options(options);
        let db = DB::open(&opts, path).map_err(|e| {
            let msg = format!("open rocksdb at {} failed, because {}", path, e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg, open, options, path)
        })?;
        let ret = RocksDB {
            db: Arc::new(db),
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
        self.db.ingest_external_file_opts(&options, files.to_vec()).map_err(|e| {
            let msg = format!("rocksdb.ingest_sst failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })
    }

    fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<dyn ExternalStorageBackup>> {
        let backup_opts = BackupEngineOptions::default();
        let backup_engine = BackupEngine::open(&backup_opts, backup_path).map_err(|e| {
            let msg = format!("open rocksdb backup engine at {} failed, because {}", backup_path.to_string(), e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        let ret = RocksDBBackupEngine {
            db: self.db.clone(),
            backup_engine,
        };
        Ok(Box::from(ret))
    }

    fn new_scan(&self, prefix: &[u8]) -> GraphResult<Box<dyn Iterator<Item=KvPair> + Send>> {
        let mut iter = match bytes_upper_bound(prefix) {
            Some(upper) => {
                let mut option = ReadOptions::default();
                option.set_iterate_upper_bound(upper);
                self.db.raw_iterator_opt(option)
            }
            None => self.db.raw_iterator(),
        };
        iter.seek(prefix);
        Ok(Box::new(Scan::new(iter)))
    }
}

pub struct Scan {
    inner_iter: RocksDBIter<'static>,
}

impl Scan {
    pub fn new(iter: DBRawIterator) -> Self {
        Scan {
            inner_iter: unsafe { std::mem::transmute(RocksDBIter::new(iter)) },
        }
    }
}

impl Iterator for Scan {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner_iter.next().map(|(k, v)| (RawBytes::new(k), RawBytes::new(v)))
    }
}


impl ExternalStorageBackup for RocksDBBackupEngine {
    /// Optimize this method after a new rust-rocksdb version.
    fn create_new_backup(&mut self) -> GraphResult<BackupId> {
        let before = self.get_backup_list();
        self.backup_engine.create_new_backup(&self.db).map_err(|e| {
            let msg = format!("create new rocksdb backup failed, because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        let after = self.get_backup_list();
        if after.len() != before.len() + 1 {
            let msg = "get new created rocksdb backup id failed".to_string();
            return Err(gen_graph_err!(GraphErrorCode::ExternalStorageError, msg));
        }
        let new_backup_id = *after.iter().max().unwrap();
        Ok(new_backup_id)
    }

    /// Do nothing now.
    /// Implement this method after a new rust-rocksdb version.
    #[allow(unused_variables)]
    fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()> {
        Ok(())
    }

    fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()> {
        let mut restore_option = RestoreOptions::default();
        restore_option.set_keep_log_files(false);
        self.backup_engine.restore_from_backup(restore_path, restore_path, &restore_option, backup_id as u32).map_err(|e| {
            let msg = format!("restore from rocksdb backup {} failed, because {}", backup_id, e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        Ok(())
    }

    fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()> {
        self.backup_engine.verify_backup(backup_id as u32).map_err(|e| {
            let msg = format!("rocksdb backup {} verify failed, because {}", backup_id, e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        Ok(())
    }

    fn get_backup_list(&self) -> Vec<BackupId> {
        self.backup_engine.get_backup_info().into_iter().map(|info| info.backup_id as BackupId).collect()
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

        if self.inner.valid() {
            Some((
                self.inner.key().unwrap(),
                self.inner.value().unwrap()
            ))
        } else {
            None
        }

    }
}

fn bytes_upper_bound(bytes: &[u8]) -> Option<Vec<u8>> {
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::MAX {
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
