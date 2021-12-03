use std::sync::atomic::{AtomicUsize, AtomicIsize, Ordering};
use std::mem::MaybeUninit;

use crate::db::api::*;
use crate::db::util::lock::GraphMutexLock;
use crate::db::common::concurrency::volatile::Volatile;

const MAX_SIZE: usize = 128;
const SIZE_MASK: usize = MAX_SIZE - 1;
const TOMBSTONE: i64 = i64::min_value();
const EMPTY_SI: SnapshotId = 0;

/// It is a lock-free data structure for multi version control based on the idea of ring buffer. It
/// can contain at most `MAX_SIZE` slots. Each slot denotes an individual version and maintains a
/// i64 number, which can be regard as a table id, a codec version or something else which can be stored
/// in 8B. It ensures all versions are arranged in increasing order in the ring buffer, so you can fast
/// find the correspond version of a specific snapshot id. For simplicity, it supports multi-thread
/// read and one thread write (every write operator should hold the lock so there is exactly one thread
/// during writing).
pub struct VersionManager {
    slots: [Slot; MAX_SIZE],
    head: AtomicUsize,
    tail: AtomicUsize,
    lock: GraphMutexLock<()>,
}

impl VersionManager {
    pub fn new() -> Self {
        let slots = unsafe {
            let mut tmp: [MaybeUninit<Slot>; MAX_SIZE] = MaybeUninit::uninit().assume_init();
            for v in &mut tmp[..] {
                std::ptr::write(v.as_mut_ptr(), Default::default());
            }
            std::mem::transmute(tmp)
        };

        VersionManager {
            slots,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            lock: GraphMutexLock::new(()),
        }
    }

    pub fn get(&self, si: SnapshotId) -> Option<Version> {
        let mut tail = self.get_tail();
        let head = self.get_head();
        while tail > head {
            let idx = (tail - 1) & SIZE_MASK;
            let x = self.slots[idx].get_si();
            if x == EMPTY_SI {
                return None;
            }
            if si >= x {
                let data = self.slots[idx].get_data();
                let x2 = self.slots[idx].get_si();
                // double check, if x2 != x means during get data, this slot has been modified
                if data == TOMBSTONE || x != x2 {
                    return None;
                }
                return Some(Version::new(x, data));
            }
            tail -= 1;
        }
        None
    }

    pub fn add(&self, si: SnapshotId, data: i64) -> GraphResult<()> {
        if data == TOMBSTONE {
            let msg = format!("cannot using this interface to add tombstone");
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add, si, data);
            return Err(err);
        }
        let res = self.do_add_data(si, data);
        res_unwrap!(res, add, si, data)
    }

    pub fn add_tombstone(&self, si: SnapshotId) -> GraphResult<()> {
        let res = self.do_add_data(si, TOMBSTONE);
        res_unwrap!(res, add_tombstone, si)
    }

    pub fn get_latest_version(&self) -> SnapshotId {
        let tail = self.get_tail();
        let head = self.get_head();
        if tail > head {
            let idx = (tail - 1) & SIZE_MASK;
            return self.slots[idx].get_si();
        }
        0
    }

    pub fn gc(&self, si: SnapshotId) -> GraphResult<Vec<i64>> {
        let _guard = res_unwrap!(self.lock.lock(), gc, si)?;
        let tail = self.get_tail();
        let mut head = self.get_head();
        let mut ret = Vec::new();
        while tail - head > 1 {
            let idx = (head + 1) & SIZE_MASK;
            let cur_si = self.slots[idx].get_si();
            let _data = self.slots[idx].get_data();
            if cur_si == 0 {
                let msg = format!("A bug found! Something wrong with the VersionManager. Maybe thread unsafe");
                let err = gen_graph_err!(GraphErrorCode::GraphStoreBug, msg, gc, si);
                return Err(err);
            }
            if cur_si <= si {
                let idx = head & SIZE_MASK;
                ret.push(self.slots[idx].get_data());
                self.slots[idx].set_si(0);
                self.slots[idx].set_data(TOMBSTONE);
                head += 1;
            } else {
                break;
            }
        }
        self.head.store(head, Ordering::Relaxed);
        Ok(ret)
    }

    pub fn size(&self) -> usize {
        self.get_tail() - self.get_head()
    }

    fn do_add_data(&self, si: SnapshotId, data: i64) -> GraphResult<()> {
        if si <= 0 {
            let msg = format!("cannot add data with version less equal than 0");
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, do_add_data, si, data);
            return Err(err);
        }
        let _guard = res_unwrap!(self.lock.lock(), add, si, data)?;
        if self.size() >= MAX_SIZE {
            let msg = format!("version count exceed limit {}", MAX_SIZE);
            let err = gen_graph_err!(GraphErrorCode::TooManyVersions, msg, do_add_data, si, data);
            return Err(err);
        }
        let tail = self.get_tail();
        let head = self.get_head();
        if tail - head > 0 {
            // check version in increasing order
            let idx = (tail - 1) & SIZE_MASK;
            let last_si = self.slots[idx].get_si();
            if last_si >= si {
                let msg = format!("version {} is less equal than last version {}, it's invalid", si, last_si);
                let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, do_add_data, si, data);
                return Err(err);
            }
        }
        let idx = tail & SIZE_MASK;
        self.slots[idx].set_data(data);
        self.slots[idx].set_si(si);
        self.tail.store(tail + 1, Ordering::Release);
        Ok(())
    }

    fn get_tail(&self) -> usize {
        self.tail.load(Ordering::Acquire)
    }

    fn get_head(&self) -> usize {
        self.head.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
pub struct Version {
    pub start_si: SnapshotId,
    pub data: i64,
}

impl Version {
    fn new(si: SnapshotId, data: i64) -> Self {
        Version {
            start_si: si,
            data,
        }
    }
}

struct Slot {
    si: Volatile<SnapshotId>,
    data: AtomicIsize,
}

impl Slot {
    pub fn get_si(&self) -> SnapshotId {
        self.si.get()
    }

    pub fn set_si(&self, si: SnapshotId) {
        self.si.set(si)
    }

    fn get_data(&self) -> i64 {
        self.data.load(Ordering::Relaxed) as i64
    }

    pub fn set_data(&self, data: i64) {
        self.data.store(data as isize, Ordering::Relaxed);
    }
}

impl Default for Slot {
    fn default() -> Self {
        Slot {
            si: Volatile::new(EMPTY_SI),
            data: AtomicIsize::new(TOMBSTONE as isize),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::sync::atomic::AtomicBool;
    use std::collections::HashSet;
    use crate::db::util::time;

    #[test]
    fn test_simple_version_manager() {
        let manager = VersionManager::new();
        assert!(manager.add(-1, 100).is_err());
        assert!(manager.add(0, 100).is_err());
        for i in 1..=100 {
            manager.add(i, i as i64).unwrap();
        }
        assert_eq!(manager.size(), 100);
        for i in 1..=100 {
            assert_eq!(manager.get(i).unwrap(), Version::new(i, i as i64));
        }
        for i in 100..110 {
            assert_eq!(manager.get(i).unwrap(), Version::new(100, 100));
        }
        assert!(manager.add(50, 100).is_err());
        let mut ans = Vec::new();
        for i in 1..50 {
            ans.push(i);
        }
        let datas = manager.gc(50).unwrap();
        assert_eq!(datas, ans);

        assert!(manager.gc(50).unwrap().is_empty());
        assert!(manager.get(49).is_none());
        ans.clear();
        for i in 50..100 {
            ans.push(i);
        }
        assert_eq!(manager.gc(100).unwrap(), ans);
        assert_eq!(manager.size(), 1);
        assert_eq!(manager.get(100).unwrap(), Version::new(100, 100));
    }

    #[test]
    fn test_version_manager_size_limit() {
        let manager = VersionManager::new();
        for i in 1..=MAX_SIZE {
            manager.add(i as SnapshotId, i as i64).unwrap();
        }
        assert!(manager.add(10000, 10000).is_err());
        manager.gc(10).unwrap();
        for i in MAX_SIZE+1..MAX_SIZE+10 {
            manager.add(i as SnapshotId, i as i64).unwrap();
        }
        assert!(manager.add(10000, 10000).is_err());
        for i in 1..10 {
            assert!(manager.get(i).is_none());
        }
        for i in 10..MAX_SIZE+10 {
            assert_eq!(manager.get(i as SnapshotId).unwrap(), Version::new(i as SnapshotId, i as i64));
        }
        assert_eq!(manager.size(), MAX_SIZE);
    }

    #[test]
    fn test_version_manager_concurrency() {
        let manager = Arc::new(VersionManager::new());
        let stop = Arc::new(AtomicBool::new(false));
        let mut adders = Vec::new();
        for _ in 0..4 {
            let manager_clone = manager.clone();
            let t = thread::spawn(move || {
                for i in 1..=MAX_SIZE {
                    let si = i as SnapshotId;
                    let data = i as i64;
                    let _ = manager_clone.add(si, data);
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
                        if let Some(version) = manager_clone.get(si) {
                            assert!(si as i64 >= version.data, "{} {}", si, version.data);
                            if si as i64 > version.data {
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
                ans.insert(i as i64);
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
        assert_eq!(manager.get(MAX_SIZE as SnapshotId).unwrap(), Version::new(MAX_SIZE as SnapshotId, MAX_SIZE as i64));
    }

    #[test]
    fn test_tombstone() {
        let manager = VersionManager::new();
        for i in 1..=10 {
            manager.add(i, i as i64).unwrap();
        }
        assert!(manager.add_tombstone(8).is_err());
        manager.add_tombstone(11).unwrap();
        for i in 1..=10 {
            assert_eq!(manager.get(i).unwrap(), Version::new(i, i as i64));
        }
        for i in 11..20 {
            assert!(manager.get(i).is_none());
        }
        assert!(manager.add(10, 1).is_err());
        for i in 12..=20 {
            manager.add(i, i as i64).unwrap();
        }

        for i in 12..=20 {
            assert_eq!(manager.get(i).unwrap(), Version::new(i, i as i64));
        }
        manager.gc(11).unwrap();
        for i in 1..12 {
            assert!(manager.get(i).is_none());
        }
        for i in 12..=20 {
            assert_eq!(manager.get(i).unwrap(), Version::new(i, i as i64));
        }
    }

    #[test]
    fn test_add_tombstone_err() {
        let manager = VersionManager::new();
        assert!(manager.add(10, TOMBSTONE).is_err());
    }
}

