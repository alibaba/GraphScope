#![allow(dead_code)]
use crate::db::api::*;

use super::version::*;
use crate::db::common::bytes::util::{UnsafeBytesWriter, UnsafeBytesReader};

pub type TableId = i64;

/// Every type has a table manager to maintain the multi versions of bulk loading data
/// It's based on a ring buffer and using lock-free tech. It can hold at most `MAX_SIZE` versions.
pub struct TableManager {
    versions: VersionManager,
}

impl TableManager {
    pub fn new() -> Self {
        TableManager {
            versions: VersionManager::new(),
        }
    }

    pub fn get(&self, si: SnapshotId) -> Option<Table> {
        self.versions.get(si).map(|v| {
            Table::new(v.start_si, v.data)
        })
    }

    pub fn add(&self, si: SnapshotId, table_id: TableId) -> GraphResult<()> {
        res_unwrap!(self.versions.add(si, table_id), add, si, table_id)
    }

    pub fn add_tombstone(&self, si: SnapshotId) -> GraphResult<()> {
        res_unwrap!(self.versions.add_tombstone(si), add_tombstone, si)
    }

    pub fn gc(&self, si: SnapshotId) -> GraphResult<Vec<TableId>> {
        res_unwrap!(self.versions.gc(si), gc, si)
    }

    pub fn size(&self) -> usize {
        self.versions.size()
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
pub struct Table {
    pub id: TableId,
    pub start_si: SnapshotId,
}

impl Table {
    pub fn new(si: SnapshotId, id: TableId) -> Self {
        Table {
            id,
            start_si: si,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match Self::encoding_version() {
            1 => {
                let mut buf = vec![0; 20];
                let mut writer = UnsafeBytesWriter::new(&mut buf);
                writer.write_i32(0, Self::encoding_version().to_be());
                writer.write_i64(4, self.id.to_be());
                writer.write_i64(12, self.start_si.to_be());
                buf
            },
            _ => unreachable!(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> GraphResult<Self> {
        if bytes.len() < 4 {
            let msg = format!("invalid bytes");
            let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, from_bytes);
            return Err(err);
        }
        let reader = UnsafeBytesReader::new(bytes);
        let version = reader.read_i32(0).to_be();
        match version {
            1 => {
                if bytes.len() == 20 {
                    let id = reader.read_i64(4).to_be();
                    let si = reader.read_i64(12).to_be();
                    let ret = Table::new(si, id);
                    return Ok(ret);
                }
                let msg = format!("invalid bytes");
                let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, from_bytes);
                Err(err)
            },
            x => {
                let msg = format!("unknown encoding version {}", x);
                let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, from_bytes);
                return Err(err);
            }
        }
    }

    fn encoding_version() -> i32 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc};
    use std::thread;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::collections::HashSet;
    use crate::db::util::time;
    const MAX_SIZE: usize = 128;

    #[test]
    fn test_simple_table_manager() {
        let manager = TableManager::new();
        assert!(manager.add(-1, 100).is_err());
        assert!(manager.add(0, 100).is_err());
        for i in 1..=100 {
            manager.add(i, i as TableId).unwrap();
        }
        assert_eq!(manager.size(), 100);
        for i in 1..=100 {
            assert_eq!(manager.get(i).unwrap(), Table::new(i, i as TableId));
        }
        for i in 100..110 {
            assert_eq!(manager.get(i).unwrap(), Table::new(100, 100));
        }
        assert!(manager.add(50, 100).is_err());
        let mut ans = Vec::new();
        for i in 1..50 {
            ans.push(i);
        }
        let table_ids = manager.gc(50).unwrap();
        assert_eq!(table_ids, ans);

        assert!(manager.gc(50).unwrap().is_empty());
        assert!(manager.get(49).is_none());
        ans.clear();
        for i in 50..100 {
            ans.push(i);
        }
        assert_eq!(manager.gc(100).unwrap(), ans);
        assert_eq!(manager.size(), 1);
        assert_eq!(manager.get(100).unwrap(), Table::new(100, 100));
    }

    #[test]
    fn test_table_manager_size_limit() {
        let manager = TableManager::new();
        for i in 1..=MAX_SIZE {
            manager.add(i as SnapshotId, i as TableId).unwrap();
        }
        assert!(manager.add(10000, 10000).is_err());
        manager.gc(10).unwrap();
        for i in MAX_SIZE+1..MAX_SIZE+10 {
            manager.add(i as SnapshotId, i as TableId).unwrap();
        }
        assert!(manager.add(10000, 10000).is_err());
        for i in 1..10 {
            assert!(manager.get(i).is_none());
        }
        for i in 10..MAX_SIZE+10 {
            assert_eq!(manager.get(i as SnapshotId).unwrap(), Table::new(i as SnapshotId, i as TableId));
        }
        assert_eq!(manager.size(), MAX_SIZE);
    }

    #[test]
    fn test_table_manager_concurrency() {
        let manager = Arc::new(TableManager::new());
        let stop = Arc::new(AtomicBool::new(false));
        let mut adders = Vec::new();
        for _ in 0..4 {
            let manager_clone = manager.clone();
            let t = thread::spawn(move || {
                for i in 1..=MAX_SIZE {
                    let si = i as SnapshotId;
                    let table_id = i as TableId;
                    let _ = manager_clone.add(si, table_id);
                    time::sleep_ms(10);
                }
            });
            adders.push(t);
        }

        let mut getters = Vec::new();
        for _ in 0..4 {
            let manager_clone = manager.clone();
            let stop_clone = stop.clone();
            let t = thread::spawn(move || {
                let mut last_si = 0;
                while !stop_clone.load(Ordering::Relaxed) {
                    let mut tmp = 0;
                    for i in 1..=MAX_SIZE {
                        let si = i as SnapshotId;
                        if let Some(table) = manager_clone.get(si) {
                            assert!(si as TableId >= table.id, "{} {}", si, table.id);
                            if si as TableId > table.id {
                                break;
                            }
                            tmp = si;
                        }
                    }
                    assert!(tmp>=last_si);
                    last_si = tmp;
                }
            });
            getters.push(t);
        }

        let manager_clone = manager.clone();
        let stop_clone = stop.clone();
        let gc = thread::spawn(move || {
            let mut ans = HashSet::new();
            for i in 1..MAX_SIZE {
                ans.insert(i as TableId);
            }

            let mut do_gc = || {
                for i in 1..=MAX_SIZE {
                    let si = i as SnapshotId;
                    let ids = manager_clone.gc(si).unwrap();
                    for id in ids {
                        assert!(ans.remove(&id));
                    }
                }
            };
            while !stop_clone.load(Ordering::Relaxed) {
                do_gc();
                time::sleep_ms(10);
            }
            do_gc();
            assert!(ans.is_empty());
        });

        for t in adders {
            t.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        for t in getters {
            t.join().unwrap();
        }
        gc.join().unwrap();
        assert_eq!(manager.size(), 1);
        assert_eq!(manager.get(MAX_SIZE as SnapshotId).unwrap(), Table::new(MAX_SIZE as SnapshotId, MAX_SIZE as TableId));
    }

    #[test]
    fn test_tombstone() {
        let manager = TableManager::new();
        for i in 1..=10 {
            manager.add(i, i as TableId).unwrap();
        }
        assert!(manager.add_tombstone(8).is_err());
        manager.add_tombstone(11).unwrap();
        for i in 1..=10 {
            assert_eq!(manager.get(i).unwrap(), Table::new(i, i as TableId));
        }
        for i in 11..20 {
            assert!(manager.get(i).is_none());
        }
        assert!(manager.add(10, 1).is_err());
        for i in 12..=20 {
            manager.add(i, i as TableId).unwrap();
        }

        for i in 12..=20 {
            assert_eq!(manager.get(i).unwrap(), Table::new(i, i as TableId));
        }
        manager.gc(11).unwrap();
        for i in 1..12 {
            assert!(manager.get(i).is_none());
        }
        for i in 12..=20 {
            assert_eq!(manager.get(i).unwrap(), Table::new(i, i as TableId));
        }
    }

    #[test]
    fn test_table_encoding() {
        let table = Table::new(1, 2);
        let bytes =table.to_bytes();
        let table2 = Table::from_bytes(&bytes).unwrap();
        assert_eq!(table, table2);
    }
}
