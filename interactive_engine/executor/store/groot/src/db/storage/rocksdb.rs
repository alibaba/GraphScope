use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use ::rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use ::rocksdb::{DBRawIterator, Env, IngestExternalFileOptions, Options, ReadOptions, DB};
use crossbeam_epoch::{self as epoch, Atomic, Owned};
use rocksdb::WriteBatch;

use super::{StorageIter, StorageRes};
use crate::db::api::*;
use crate::db::storage::{KvPair, RawBytes};

pub struct RocksDB {
    db: Atomic<Arc<DB>>,
    options: HashMap<String, String>,
    is_secondary: bool,
}

pub struct RocksDBBackupEngine {
    db: Arc<DB>,
    backup_engine: BackupEngine,
}

impl RocksDB {
    pub fn open(options: &HashMap<String, String>) -> GraphResult<Self> {
        let opts = init_options(options);
        let path = options
            .get("store.data.path")
            .expect("invalid config, missing store.data.path");
        let db = DB::open(&opts, path).map_err(|e| {
            let msg = format!("open rocksdb at {} failed: {}", path, e.into_string());
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg, open, options, path)
        })?;
        let ret = RocksDB { db: Atomic::new(Arc::new(db)), options: options.clone(), is_secondary: false };
        Ok(ret)
    }

    pub fn open_as_secondary(options: &HashMap<String, String>) -> GraphResult<Self> {
        let db = RocksDB::open_helper(options, false).map_err(|e| {
            let msg = format!("open rocksdb at {:?}, error: {:?}", options, e);
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg, open_as_secondary)
        })?;

        let ret = RocksDB { db: Atomic::new(Arc::new(db)), options: options.clone(), is_secondary: true };
        Ok(ret)
    }

    pub fn open_helper(options: &HashMap<String, String>, reopen: bool) -> Result<DB, ::rocksdb::Error> {
        let path = options
            .get("store.data.path")
            .expect("invalid config, missing store.data.path");
        let mut sec_path = options
            .get("store.data.secondary.path")
            .expect("invalid config, missing store.data.secondary.path")
            .clone();
        if reopen {
            while Path::new(&sec_path).exists() {
                sec_path = increment_path_string(&sec_path);
            }
        }
        let opts = init_secondary_options(options);
        info!("Opening secondary at {}, {}", path, sec_path);
        DB::open_as_secondary(&opts, path, &sec_path)
    }

    pub fn get_db(&self) -> GraphResult<Arc<DB>> {
        let guard = &epoch::pin();
        let db_shared = self.db.load(Ordering::SeqCst, guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            Ok(db.clone())
        } else {
            let msg = format!("rocksdb.scan_from failed because the acquired db is `None`");
            let err = gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg);
            Err(err)
        }
    }

    fn replace_db(&self, db: DB) {
        let guard = &epoch::pin();
        let new_db = Arc::new(db);
        let cur = Owned::new(new_db).into_shared(guard);

        let prev = self.db.swap(cur, Ordering::Release, guard);
        unsafe {
            drop(prev.into_owned());
        }
        info!("RocksDB replaced");
    }

    pub fn get(&self, key: &[u8]) -> GraphResult<Option<StorageRes>> {
        let db = self.get_db()?;
        match db.get(key) {
            Ok(Some(v)) => Ok(Some(StorageRes::RocksDB(v))),
            Ok(None) => Ok(None),
            Err(e) => {
                let msg = format!("rocksdb.get failed because {}", e.into_string());
                let err = gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg);
                Err(err)
            }
        }
    }

    pub fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot put in secondary instance");
            return Ok(());
        }
        let db = self.get_db()?;
        db.put(key, val).map_err(|e| {
            let msg = format!("rocksdb.put failed because {}", e.into_string());
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })
    }

    pub fn delete(&self, key: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot delete in secondary instance");
            return Ok(());
        }
        let db = self.get_db()?;
        db.delete(key).map_err(|e| {
            let msg = format!("rocksdb.delete failed because {}", e.into_string());
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter> {
        let db = self.get_db()?;
        Ok(StorageIter::RocksDB(RocksDBIter::new_prefix(db, prefix)))
    }

    pub fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter> {
        let db = self.get_db()?;
        Ok(StorageIter::RocksDB(RocksDBIter::new_start(db, start)))
    }

    pub fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter> {
        let db = self.get_db()?;
        Ok(StorageIter::RocksDB(RocksDBIter::new_range(db, start, end)))
    }

    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot delete_range in secondary instance");
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let db = self.get_db()?;
        let _ = db.delete_file_in_range(start, end);
        batch.delete_range(start, end);
        db.write(batch).map_err(|e| {
            let msg = format!("rocksdb.delete_range failed because {}", e.into_string());
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })?;
        let mut val = false;
        if let Some(conf_str) = self
            .options
            .get("store.rocksdb.disable.auto.compactions")
        {
            val = conf_str.parse::<bool>().unwrap();
        }
        if !val {
            db.compact_range(Option::Some(start), Option::Some(end))
        }
        Ok(())
    }

    pub fn compact(&self) -> GraphResult<()> {
        info!("begin to compact rocksdb");
        if self.is_secondary {
            info!("Cannot compact in secondary instance");
            return Ok(());
        }
        let db = self.get_db()?;
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        info!("compacted rocksdb");
        Ok(())
    }

    pub fn load(&self, files: &[&str]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot ingest in secondary instance");
            return Ok(());
        }
        let mut options = IngestExternalFileOptions::default();
        options.set_move_files(true);
        let db = self.get_db()?;
        db.ingest_external_file_opts(&options, files.to_vec())
            .map_err(|e| {
                let msg = format!("rocksdb.load file {:?} failed because {}", files, e.into_string());
                gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
            })
    }

    pub fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<RocksDBBackupEngine>> {
        let backup_opts = BackupEngineOptions::new(backup_path).map_err(|e| {
            let msg = format!(
                "Gen BackupEngineOptions error for path {}, because {}",
                backup_path.to_string(),
                e.into_string()
            );
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })?;
        let env = Env::new().map_err(|e| {
            let msg = format!("Gen rocksdb Env failed because {}", e.into_string());
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })?;
        let backup_engine = BackupEngine::open(&backup_opts, &env).map_err(|e| {
            let msg = format!(
                "open rocksdb backup engine at {} failed, because {}",
                backup_path.to_string(),
                e.into_string()
            );
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })?;
        let db = self.get_db()?;
        Ok(Box::from(RocksDBBackupEngine { db, backup_engine }))
    }

    pub fn new_scan(&self, prefix: &[u8]) -> GraphResult<Box<dyn Iterator<Item = KvPair> + Send>> {
        let db = self.get_db()?;
        Ok(Box::new(Scan::new(db, prefix)))
    }

    pub fn try_catch_up_with_primary(&self) -> GraphResult<()> {
        if !self.is_secondary {
            return Ok(());
        }
        let db = self.get_db()?;
        db.try_catch_up_with_primary().map_err(|e| {
            let msg = format!("rocksdb.try_catch_up_with_primary failed because {:?}", e);
            gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
        })
    }

    pub fn reopen(&self, wait_sec: u64) -> GraphResult<()> {
        if !self.is_secondary {
            return Ok(());
        }
        loop {
            std::thread::sleep(Duration::from_secs(wait_sec));
            let db = RocksDB::open_helper(&self.options, true).map_err(|e| {
                let msg = format!("open rocksdb at {:?}, error: {:?}", self.options, e);
                gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg, open_as_secondary)
            })?;
            let ret = db.try_catch_up_with_primary();
            if ret.is_err() {
                error!("New secondary catch up failed: {:?}", ret);
            } else {
                info!("RocksDB secondary instance reopened");
                self.replace_db(db);
                break;
            }
        }
        Ok(())
    }
}

fn increment_path_string(string: &str) -> String {
    let file_parts: Vec<&str> = string.rsplitn(2, "/").collect();
    let mut parts: Vec<&str> = file_parts[0].rsplitn(2, '_').collect();
    parts.reverse();
    let last = parts[parts.len() - 1];
    let number: i64 = if parts.len() == 1 || !last.chars().all(|c| c.is_digit(10)) {
        0
    } else {
        last.parse::<i64>().unwrap() + 1
    };
    format!("{}/{}_{}", file_parts[1], parts[0], number)
}

pub struct Scan<'a> {
    inner_iter: RocksDBIter<'a>,
}

impl<'a> Scan<'a> {
    pub fn new(db: Arc<DB>, prefix: &[u8]) -> Self {
        Scan { inner_iter: RocksDBIter::new_prefix(db, prefix) }
    }
}

impl<'a> Iterator for Scan<'a> {
    type Item = KvPair;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner_iter
            .next()
            .map(|(k, v)| (RawBytes::new(k), RawBytes::new(v)))
    }
}

impl RocksDBBackupEngine {
    /// Optimize this method after a new rust-rocksdb version.
    pub fn create_new_backup(&mut self) -> GraphResult<BackupId> {
        let before = self.get_backup_list();
        self.backup_engine
            .create_new_backup(&self.db)
            .map_err(|e| {
                let msg = format!("create new rocksdb backup failed, because {}", e.into_string());
                gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
            })?;
        let after = self.get_backup_list();
        if after.len() != before.len() + 1 {
            let msg = "get new created rocksdb backup id failed".to_string();
            return Err(gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg));
        }
        let new_backup_id = *after.iter().max().unwrap();
        Ok(new_backup_id)
    }

    /// Do nothing now.
    /// Implement this method after a new rust-rocksdb version.
    #[allow(unused_variables)]
    pub fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()> {
        Ok(())
    }

    pub fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()> {
        let mut restore_option = RestoreOptions::default();
        restore_option.set_keep_log_files(false);
        self.backup_engine
            .restore_from_backup(restore_path, restore_path, &restore_option, backup_id as u32)
            .map_err(|e| {
                let msg = format!(
                    "restore from rocksdb backup {} failed, because {}",
                    backup_id,
                    e.into_string()
                );
                gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
            })?;
        Ok(())
    }

    pub fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()> {
        self.backup_engine
            .verify_backup(backup_id as u32)
            .map_err(|e| {
                let msg =
                    format!("rocksdb backup {} verify failed, because {}", backup_id, e.into_string());
                gen_graph_err!(ErrorCode::EXTERNAL_STORAGE_ERROR, msg)
            })?;
        Ok(())
    }

    pub fn get_backup_list(&self) -> Vec<BackupId> {
        self.backup_engine
            .get_backup_info()
            .into_iter()
            .map(|info| info.backup_id as BackupId)
            .collect()
    }
}

fn init_secondary_options(options: &HashMap<String, String>) -> Options {
    let mut opts = Options::default();
    opts.set_max_open_files(-1);
    opts.set_max_write_buffer_number(4);

    if let Some(conf_str) = options.get("store.rocksdb.wal.dir") {
        opts.set_wal_dir(Path::new(conf_str));
    }

    // opts.set_use_direct_reads(true);
    // opts.set_use_direct_io_for_flush_and_compaction(true);

    opts
}

#[allow(unused_variables)]
fn init_options(options: &HashMap<String, String>) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_max_background_jobs(6);
    opts.set_write_buffer_size(256 << 20);
    opts.set_max_open_files(-1);
    // opts.set_max_log_file_size(1024 << 10);
    opts.set_keep_log_file_num(10);
    // https://github.com/facebook/rocksdb/wiki/Basic-Operations#non-sync-writes
    opts.set_use_fsync(true);
    opts.set_max_write_buffer_number(4);

    opts.set_bytes_per_sync(1048576);

    if let Some(conf_str) = options.get("store.rocksdb.disable.auto.compactions") {
        let val = conf_str.parse().unwrap();
        opts.set_disable_auto_compactions(val);
    }

    if let Some(conf_str) = options.get("store.rocksdb.wal.dir") {
        opts.set_wal_dir(Path::new(conf_str));
    }

    if let Some(conf_str) = options.get("store.rocksdb.write.buffer.mb") {
        let size_mb: usize = conf_str.parse().unwrap();
        let size_bytes = size_mb * 1024 * 1024;
        opts.set_write_buffer_size(size_bytes);
    }
    if let Some(conf_str) = options.get("store.rocksdb.max.write.buffer.num") {
        opts.set_max_write_buffer_number(conf_str.parse().unwrap());
    }
    if let Some(conf_str) = options.get("store.rocksdb.level0.compaction.trigger") {
        opts.set_level_zero_file_num_compaction_trigger(conf_str.parse().unwrap());
    }
    if let Some(conf_str) = options.get("store.rocksdb.max.level.base.mb") {
        let size_mb: u64 = conf_str.parse().unwrap();
        opts.set_max_bytes_for_level_base(size_mb * 1024 * 1024);
    }
    if let Some(conf_str) = options.get("store.rocksdb.background.jobs") {
        let background_jobs = conf_str.parse().unwrap();
        opts.set_max_background_jobs(background_jobs);
    }
    if let Some(conf_str) = options.get("store.rocksdb.paranoid.checks") {
        let check = conf_str.parse().unwrap();
        opts.set_paranoid_checks(check);
    }
    opts
}

pub struct RocksDBIter<'a> {
    _db: *const DB,
    inner: DBRawIterator<'a>,
    just_seeked: bool,
}

unsafe impl Send for RocksDBIter<'_> {}

impl<'a> RocksDBIter<'a> {
    fn new_prefix(db: Arc<DB>, prefix: &[u8]) -> Self {
        let opt = match bytes_upper_bound(prefix) {
            Some(upper) => {
                let mut option = ReadOptions::default();
                option.set_iterate_upper_bound(upper);
                option
            }
            None => ReadOptions::default(),
        };
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let db_ref = unsafe { &*db_ptr };
        let mut iter = db_ref.raw_iterator_opt(opt);
        iter.seek(prefix);
        let db_iter = Self { _db: db_ptr, inner: iter, just_seeked: true };
        db_iter
    }

    fn new_start(db: Arc<DB>, start: &[u8]) -> Self {
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let db_ref = unsafe { &*db_ptr };
        let mut iter = db_ref.raw_iterator();
        iter.seek(start);
        let db_iter = Self { _db: db_ptr, inner: iter, just_seeked: true };
        db_iter
    }

    fn new_range(db: Arc<DB>, start: &[u8], end: &[u8]) -> Self {
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let db_ref = unsafe { &*db_ptr };
        let mut option = ReadOptions::default();
        option.set_iterate_upper_bound(end.to_vec());
        let mut iter = db_ref.raw_iterator_opt(option);
        iter.seek(start);
        let db_iter = Self { _db: db_ptr, inner: iter, just_seeked: true };
        db_iter
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
            Some((self.inner.key().unwrap(), self.inner.value().unwrap()))
        } else {
            None
        }
    }
}

impl<'a> Drop for RocksDBIter<'a> {
    fn drop(&mut self) {
        unsafe {
            Arc::from_raw(self._db);
        }
    }
}

fn bytes_upper_bound(bytes: &[u8]) -> Option<Vec<u8>> {
    for i in (0..bytes.len()).rev() {
        if bytes[i] != u8::MAX {
            let mut ret = bytes.to_vec();
            ret[i] += 1;
            for j in i + 1..bytes.len() {
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
            let mut config = HashMap::new();
            config.insert("store.data.path".to_owned(), path.to_owned());
            let db = RocksDB::open(&config).unwrap();
            let mut ans = Vec::new();
            for i in 1..=10 {
                let key = format!("aaa#{:010}", i);
                db.put(key.as_bytes(), i.to_string().as_bytes())
                    .unwrap();
                ans.push((key, i));
            }
            let mut iter = db.scan_prefix(b"aaa").unwrap();
            for (key, i) in ans {
                let (k, v) = iter.next().unwrap();
                assert_eq!(key, String::from_utf8(k.to_vec()).unwrap());
                assert_eq!(
                    i,
                    String::from_utf8(v.to_vec())
                        .unwrap()
                        .parse::<i32>()
                        .unwrap()
                );
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
            let mut config = HashMap::new();
            config.insert("store.data.path".to_owned(), path.to_owned());
            let db = RocksDB::open(&config).unwrap();
            for i in 1..=20 {
                if i % 2 == 0 {
                    let key = format!("aaa#{:010}", i);
                    db.put(key.as_bytes(), transform::i64_to_vec(i).as_slice())
                        .unwrap();
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
