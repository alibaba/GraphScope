use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ::rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use ::rocksdb::{DBRawIterator, Env, IngestExternalFileOptions, Options, ReadOptions, DB};
use crossbeam_epoch::{self as epoch, Atomic, Guard, Shared, Owned};
use libc::THREAD_BACKGROUND_POLICY_DARWIN_BG;
use rocksdb::{DBCompactionStyle, DBCompressionType, WriteBatch};

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
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg, open, options, path)
        })?;
        let ret = RocksDB { db: Atomic::new(Arc::new(db)), options: options.clone(), is_secondary: false };
        Ok(ret)
    }

    pub fn open_as_secondary(options: &HashMap<String, String>) -> GraphResult<Self> {
        let db = RocksDB::open_helper(options).map_err(|e| {
            let msg = format!("open rocksdb at {:?}, error: {:?}", options, e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg, open_as_secondary)
        })?;

        let ret = RocksDB { db: Atomic::new(Arc::new(db)), options: options.clone(), is_secondary: true };
        Ok(ret)
    }

    pub fn open_helper(options: &HashMap<String, String>) -> Result<DB, ::rocksdb::Error> {
        let path = options
            .get("store.data.path")
            .expect("invalid config, missing store.data.path");
        let sec_path = options
            .get("store.data.secondary.path")
            .expect("invalid config, missing store.data.secondary.path");

        let mut opts = Options::default();
        opts.set_max_open_files(-1);
        opts.set_paranoid_checks(false);
        DB::open_as_secondary(&opts, path, sec_path)
    }

    fn get_db<'g>(&self, guard: &'g Guard) -> Shared<'g, Arc<DB>> {
        self.db.load(Ordering::Acquire, guard)
    }

    fn replace_db(&self, db: DB) {
        let guard = epoch::pin();
        let new_db = Arc::new(db);
        let new_db_shared = Owned::new(new_db).into_shared(&guard);
        let old_db_shared = self.db.swap(new_db_shared, Ordering::Release, &guard);

        // Use Crossbeam's 'defer' mechanism to safely drop the old Arc
        unsafe {
            // Convert 'Shared' back to 'Arc' for deferred dropping
            let old_db_arc = old_db_shared.into_owned();
            // guard.defer_destroy(old_db_shared)
            guard.defer(|| drop(old_db_arc))
        }
    }

    pub fn get(&self, key: &[u8]) -> GraphResult<Option<StorageRes>> {
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            match db.get(key) {
                Ok(Some(v)) => Ok(Some(StorageRes::RocksDB(v))),
                Ok(None) => Ok(None),
                Err(e) => {
                    let msg = format!("rocksdb.get failed because {}", e.into_string());
                    let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
                    Err(err)
                }
            }
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn put(&self, key: &[u8], val: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot put in secondary instance");
            return Ok(());
        }
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            db.put(key, val).map_err(|e| {
                let msg = format!("rocksdb.put failed because {}", e.into_string());
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
            })
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn delete(&self, key: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot delete in secondary instance");
            return Ok(());
        }
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            db.delete(key).map_err(|e| {
                let msg = format!("rocksdb.delete failed because {}", e.into_string());
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
            })
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> GraphResult<StorageIter> {
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            Ok(StorageIter::RocksDB(RocksDBIter::new_prefix(db.clone(), prefix)))
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn scan_from(&self, start: &[u8]) -> GraphResult<StorageIter> {
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            Ok(StorageIter::RocksDB(RocksDBIter::new_start(db.clone(), start)))
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn scan_range(&self, start: &[u8], end: &[u8]) -> GraphResult<StorageIter> {
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            Ok(StorageIter::RocksDB(RocksDBIter::new_range(db.clone(), start, end)))
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn delete_range(&self, start: &[u8], end: &[u8]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot delete_range in secondary instance");
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            db.delete_file_in_range(start, end)
                .map_err(|e| {
                    let msg = format!("rocksdb.delete_files_in_range failed because {}", e.into_string());
                    gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
                })?;
            batch.delete_range(start, end);
            db.write(batch).map_err(|e| {
                let msg = format!("rocksdb.delete_range failed because {}", e.into_string());
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
            })?;
            db.compact_range(Option::Some(start), Option::Some(end));
            Ok(())
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn load(&self, files: &[&str]) -> GraphResult<()> {
        if self.is_secondary {
            info!("Cannot ingest in secondary instance");
            return Ok(());
        }
        let mut options = IngestExternalFileOptions::default();
        options.set_move_files(true);
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            db.ingest_external_file_opts(&options, files.to_vec())
                .map_err(|e| {
                    let msg =
                        format!("rocksdb.ingest_sst file {:?} failed because {}", files, e.into_string());
                    gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
                })
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<RocksDBBackupEngine>> {
        let backup_opts = BackupEngineOptions::new(backup_path).map_err(|e| {
            let msg = format!(
                "Gen BackupEngineOptions error for path {}, because {}",
                backup_path.to_string(),
                e.into_string()
            );
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        let env = Env::new().map_err(|e| {
            let msg = format!("Gen rocksdb Env failed because {}", e.into_string());
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        let backup_engine = BackupEngine::open(&backup_opts, &env).map_err(|e| {
            let msg = format!(
                "open rocksdb backup engine at {} failed, because {}",
                backup_path.to_string(),
                e.into_string()
            );
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
        })?;
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            let ret = RocksDBBackupEngine { db: db.clone(), backup_engine };
            Ok(Box::from(ret))
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn new_scan(&self, prefix: &[u8]) -> GraphResult<Box<dyn Iterator<Item = KvPair> + Send>> {
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            Ok(Box::new(Scan::new(db.clone(), prefix)))
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn try_catch_up_with_primary(&self) -> GraphResult<()> {
        if !self.is_secondary {
            return Ok(());
        }
        let guard = epoch::pin();
        let db_shared = self.get_db(&guard);
        if let Some(db) = unsafe { db_shared.as_ref() } {
            db.try_catch_up_with_primary().map_err(|e| {
                let msg = format!("try to catch up with primary failed because {:?}", e);
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
            })
        } else {
            let msg = format!("rocksdb.get failed because the acquired db is `None`");
            let err = gen_graph_err!(GraphErrorCode::ExternalStorageError, msg);
            Err(err)
        }
    }

    pub fn reopen(&self) -> GraphResult<()> {
        let db = RocksDB::open_helper(&self.options).map_err(|e| {
            let msg = format!("open rocksdb at {:?}, error: {:?}", self.options, e);
            gen_graph_err!(GraphErrorCode::ExternalStorageError, msg, open_as_secondary)
        })?;
        self.replace_db(db);
        Ok(())
    }
}

pub struct Scan {
    inner_iter: RocksDBIter<'static>,
}

impl Scan {
    pub fn new(db: Arc<DB>, prefix: &[u8]) -> Self {
        Scan { inner_iter: RocksDBIter::new_prefix(db, prefix) }
    }
}

impl Iterator for Scan {
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
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
            })?;
        Ok(())
    }

    pub fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()> {
        self.backup_engine
            .verify_backup(backup_id as u32)
            .map_err(|e| {
                let msg =
                    format!("rocksdb backup {} verify failed, because {}", backup_id, e.into_string());
                gen_graph_err!(GraphErrorCode::ExternalStorageError, msg)
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

#[allow(unused_variables)]
fn init_options(options: &HashMap<String, String>) -> Options {
    let mut ret = Options::default();
    ret.create_if_missing(true);
    ret.set_max_background_jobs(6);
    ret.set_write_buffer_size(256 << 20);
    ret.set_max_open_files(-1);
    ret.set_max_log_file_size(1024 << 10);
    ret.set_keep_log_file_num(10);
    // https://github.com/facebook/rocksdb/wiki/Basic-Operations#non-sync-writes
    ret.set_use_fsync(true);

    if let Some(conf_str) = options.get("store.rocksdb.compression.type") {
        match conf_str.as_str() {
            "none" => ret.set_compression_type(DBCompressionType::None),
            "snappy" => ret.set_compression_type(DBCompressionType::Snappy),
            "zlib" => ret.set_compression_type(DBCompressionType::Zlib),
            "bz2" => ret.set_compression_type(DBCompressionType::Bz2),
            "lz4" => ret.set_compression_type(DBCompressionType::Lz4),
            "lz4hc" => ret.set_compression_type(DBCompressionType::Lz4hc),
            "zstd" => ret.set_compression_type(DBCompressionType::Zstd),
            _ => panic!("invalid compression_type config"),
        }
    }
    if let Some(conf_str) = options.get("store.rocksdb.stats.dump.period.sec") {
        ret.set_stats_dump_period_sec(conf_str.parse().unwrap());
    }
    if let Some(conf_str) = options.get("store.rocksdb.compaction.style") {
        match conf_str.as_str() {
            "universal" => ret.set_compaction_style(DBCompactionStyle::Universal),
            "level" => ret.set_compaction_style(DBCompactionStyle::Level),
            _ => panic!("invalid compaction_style config"),
        }
    }
    if let Some(conf_str) = options.get("store.rocksdb.write.buffer.mb") {
        let size_mb: usize = conf_str.parse().unwrap();
        let size_bytes = size_mb * 1024 * 1024;
        ret.set_write_buffer_size(size_bytes);
    }
    if let Some(conf_str) = options.get("store.rocksdb.max.write.buffer.num") {
        ret.set_max_write_buffer_number(conf_str.parse().unwrap());
    }
    if let Some(conf_str) = options.get("store.rocksdb.level0.compaction.trigger") {
        ret.set_level_zero_file_num_compaction_trigger(conf_str.parse().unwrap());
    }
    if let Some(conf_str) = options.get("store.rocksdb.max.level.base.mb") {
        let size_mb: u64 = conf_str.parse().unwrap();
        ret.set_max_bytes_for_level_base(size_mb * 1024 * 1024);
    }
    if let Some(conf_str) = options.get("store.rocksdb.background.jobs") {
        let background_jobs = conf_str.parse().unwrap();
        ret.set_max_background_jobs(background_jobs);
    }
    ret
}

pub struct RocksDBIter<'a> {
    _db: Arc<DB>,
    inner: Option<DBRawIterator<'a>>,
    just_seeked: bool,
}

impl<'a> RocksDBIter<'a> {
    fn new_prefix(db: Arc<DB>, prefix: &[u8]) -> Self {
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let mut db_iter = Self { _db: db, inner: None, just_seeked: true };
        let db_ref = unsafe { &*db_ptr };
        let mut iter = match bytes_upper_bound(prefix) {
            Some(upper) => {
                let mut option = ReadOptions::default();
                option.set_iterate_upper_bound(upper);
                db_ref.raw_iterator_opt(option)
            }
            None => db_ref.raw_iterator(),
        };
        iter.seek(prefix);

        db_iter.inner = Some(iter);

        db_iter
    }

    fn new_start(db: Arc<DB>, start: &[u8]) -> Self {
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let mut db_iter = Self { _db: db, inner: None, just_seeked: true };
        let db_ref = unsafe { &*db_ptr };
        let mut iter = db_ref.raw_iterator();
        iter.seek(start);
        db_iter.inner = Some(iter);

        db_iter
    }

    fn new_range(db: Arc<DB>, start: &[u8], end: &[u8]) -> Self {
        let db_ptr = Arc::into_raw(db.clone()) as *const DB;
        let mut db_iter = Self { _db: db, inner: None, just_seeked: true };
        let db_ref = unsafe { &*db_ptr };
        let mut option = ReadOptions::default();
        option.set_iterate_upper_bound(end.to_vec());
        let mut iter = db_ref.raw_iterator_opt(option);
        iter.seek(start);

        db_iter.inner = Some(iter);

        db_iter
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if let Some(inner) = &mut self.inner {
            if !inner.valid() {
                return None;
            }

            if self.just_seeked {
                self.just_seeked = false;
            } else {
                inner.next();
            }

            if inner.valid() {
                Some((inner.key().unwrap(), inner.value().unwrap()))
            } else {
                None
            }
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
            let db = RocksDB::open(&HashMap::new(), path).unwrap();
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
            let db = RocksDB::open(&HashMap::new(), path).unwrap();
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
