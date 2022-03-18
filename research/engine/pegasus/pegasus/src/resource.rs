use std::any::{Any, TypeId};
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::sync::ShardedLock;

use crate::{JobConf, ServerConf};

pub type ResourceMap = HashMap<TypeId, Box<dyn Any + Send>>;
pub type KeyedResources = HashMap<String, Box<dyn Any + Send>>;
pub type SharedResourceMap = HashMap<TypeId, Box<dyn Any + Send + Sync>>;
pub type SharedKeyedResourceMap = HashMap<String, Box<dyn Any + Send + Sync>>;

struct REntry {
    ref_cnt: Arc<AtomicUsize>,
    entry: Box<dyn Any + Send + Sync>,
    freezed: AtomicBool,
}

impl REntry {
    fn new<T: Any + Send + Sync>(e: T) -> Self {
        REntry {
            ref_cnt: Arc::new(AtomicUsize::new(0)),
            entry: Box::new(e) as Box<dyn Any + Send + Sync>,
            freezed: AtomicBool::new(false),
        }
    }

    fn get_ref<T: Any>(&self) -> Option<ArcRef<T>> {
        if self.freezed.load(Ordering::SeqCst) {
            return None;
        }
        let r = self.entry.downcast_ref::<T>()?;
        let ref_cnt = self.ref_cnt.clone();
        ref_cnt.fetch_add(1, Ordering::SeqCst);
        Some(ArcRef { cnt: ref_cnt, inner: r })
    }

    fn get_ref_cnt(&self) -> usize {
        self.ref_cnt.load(Ordering::SeqCst)
    }

    fn freeze(&self) {
        self.freezed.store(true, Ordering::SeqCst);
    }

    fn is_idle(&self) -> bool {
        self.ref_cnt.load(Ordering::SeqCst) == 0 && self.freezed.load(Ordering::SeqCst)
    }
}

struct KeyedResourceMap {
    index: ShardedLock<HashMap<String, usize>>,
    values: UnsafeCell<Vec<Option<REntry>>>,
}

unsafe impl Sync for KeyedResourceMap {}

pub struct ArcRef<'a, T> {
    inner: &'a T,
    cnt: Arc<AtomicUsize>,
}

impl<'a, T> Deref for ArcRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, T> Drop for ArcRef<'a, T> {
    fn drop(&mut self) {
        self.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl KeyedResourceMap {
    fn new() -> Self {
        KeyedResourceMap { index: ShardedLock::new(HashMap::new()), values: UnsafeCell::new(Vec::new()) }
    }

    pub fn add_resource<T: Send + Sync + 'static>(&self, key: String, res: T) -> Option<(String, T)> {
        let mut locked = self.index.write().expect("lock write poisoned");
        if locked.contains_key(&key) {
            return Some((key, res));
        }
        let offset = unsafe {
            let v = &mut *self.values.get();
            if v.len() < 1024 {
                v.push(Some(REntry::new(res)));
                v.len() - 1
            } else {
                let mut found = None;
                for (i, slot) in v.iter_mut().enumerate() {
                    match slot {
                        Some(e) => {
                            if e.is_idle() {
                                found = Some(i);
                                break;
                            }
                        }
                        None => {
                            found = Some(i);
                            break;
                        }
                    }
                }
                if let Some(idle) = found {
                    v[idle] = Some(REntry::new(res));
                    idle
                } else {
                    v.push(Some(REntry::new(res)));
                    v.len() - 1
                }
            }
        };
        locked.insert(key, offset);
        None
    }

    pub fn get_resource<T: Any>(&self, key: &str) -> Option<ArcRef<T>> {
        let locked = self.index.read().expect("lock read poisioned");
        if let Some(offset) = locked.get(key) {
            let v = unsafe { &*self.values.get() };
            if *offset >= v.len() {
                return None;
            }
            if let Some(entry) = &v[*offset] {
                entry.get_ref()
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn remove_resource(&self, key: &str) {
        let mut locked = self.index.write().expect("lock read poisioned");
        if let Some(offset) = locked.remove(key) {
            let v = unsafe { &mut *self.values.get() };
            if offset < v.len() {
                if let Some(entry) = v[offset].take() {
                    if entry.get_ref_cnt() > 0 {
                        entry.freeze();
                        v[offset].replace(entry);
                    }
                }
            }
        }
    }
}

lazy_static! {
    static ref GLOBAL_RESOURCE_MAP: ShardedLock<SharedResourceMap> = ShardedLock::new(Default::default());
    static ref GLOBAL_KEYED_RESOURCES: KeyedResourceMap = KeyedResourceMap::new();
}

thread_local! {
    static RESOURCES : RefCell<ResourceMap> = RefCell::new(ResourceMap::default());
    static KEYED_RESOURCES: RefCell<KeyedResources> = RefCell::new(KeyedResources::default());
}

pub struct Resource<T> {
    ptr: *const T,
}

impl<T> Deref for Resource<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().expect("resource lost") }
    }
}

unsafe impl<T: Send> Send for Resource<T> {}

unsafe impl<T: Send + Sync> Sync for Resource<T> {}

pub struct ResourceMut<T> {
    ptr: *mut T,
}

impl<T> Deref for ResourceMut<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().expect("resource lost") }
    }
}

impl<T> DerefMut for ResourceMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut().expect("resource lost") }
    }
}

pub fn get_resource<T: Any>() -> Option<Resource<T>> {
    RESOURCES.with(|res| {
        let borrow = res.borrow();
        let type_id = TypeId::of::<T>();
        if let Some(r) = borrow.get(&type_id) {
            let resource: &T = r.downcast_ref::<T>().expect("type id error;");
            let ptr = resource as *const T;
            Some(Resource { ptr })
        } else {
            None
        }
    })
}

pub fn get_resource_mut<T: Any>() -> Option<ResourceMut<T>> {
    RESOURCES.with(|res| {
        let mut borrow = res.borrow_mut();
        let type_id = TypeId::of::<T>();
        if let Some(r) = borrow.get_mut(&type_id) {
            let resource: &mut T = r.downcast_mut::<T>().expect("type id error;");
            let ptr = resource as *mut T;
            Some(ResourceMut { ptr })
        } else {
            None
        }
    })
}

pub fn get_resource_by_key<T: Any>(key: &str) -> Option<Resource<T>> {
    KEYED_RESOURCES.with(|store| {
        let borrow = store.borrow();
        if let Some(r) = borrow.get(key) {
            if let Some(res) = r.downcast_ref::<T>() {
                let ptr = res as *const T;
                Some(Resource { ptr })
            } else {
                None
            }
        } else {
            None
        }
    })
}

pub fn get_resource_mut_by_key<T: Any>(key: &str) -> Option<ResourceMut<T>> {
    KEYED_RESOURCES.with(|store| {
        let mut borrow = store.borrow_mut();
        if let Some(r) = borrow.get_mut(key) {
            if let Some(res) = r.downcast_mut::<T>() {
                let ptr = res as *mut T;
                Some(ResourceMut { ptr })
            } else {
                None
            }
        } else {
            None
        }
    })
}

pub(crate) fn replace_resource(resource: ResourceMap) -> ResourceMap {
    RESOURCES.with(|store| store.replace(resource))
}

pub(crate) fn replace_keyed_resources(resource: KeyedResources) -> KeyedResources {
    KEYED_RESOURCES.with(|store| store.replace(resource))
}

pub fn set_global_resource<T: Any + Send + Sync>(res: T) {
    let mut store = GLOBAL_RESOURCE_MAP
        .write()
        .expect("global resource write lock poisoned");
    let type_id = TypeId::of::<T>();
    store.insert(type_id, Box::new(res));
}

pub fn add_global_resource<T: Any + Send + Sync>(key: String, res: T) -> Option<(String, T)> {
    GLOBAL_KEYED_RESOURCES.add_resource(key, res)
}

pub fn get_global_resource<T: Any + Send + Sync>(key: &str) -> Option<ArcRef<'static, T>> {
    GLOBAL_KEYED_RESOURCES.get_resource(key)
}

pub fn remove_global_resource(key: &str) {
    GLOBAL_KEYED_RESOURCES.remove_resource(key)
}

pub trait PartitionedResource {
    type Res: Send + 'static;

    fn get_resource(&self, par: usize) -> Option<&Self::Res>;

    fn take_resource(&mut self, par: usize) -> Option<Self::Res>;
}

impl<T: ?Sized + Send + Sync + 'static> PartitionedResource for std::sync::Arc<T> {
    type Res = std::sync::Arc<T>;

    fn get_resource(&self, _par: usize) -> Option<&Self::Res> {
        Some(&self)
    }

    fn take_resource(&mut self, _par: usize) -> Option<Self::Res> {
        Some(self.clone())
    }
}

pub struct DefaultParResource<T> {
    partitions: Vec<Option<T>>,
}

impl<T> DefaultParResource<T> {
    pub fn new(conf: &JobConf, res: Vec<T>) -> Result<Self, Vec<T>> {
        if res.len() as u32 != conf.workers {
            Err(res)
        } else {
            let mut partitions = Vec::with_capacity(res.len());
            for r in res {
                partitions.push(Some(r));
            }
            let pr = DefaultParResource { partitions };
            Ok(pr)
        }
    }
}

impl<T: Send + Sync + 'static> PartitionedResource for DefaultParResource<T> {
    type Res = T;

    fn get_resource(&self, par: usize) -> Option<&Self::Res> {
        if par < self.partitions.len() {
            self.partitions[par].as_ref()
        } else {
            None
        }
    }

    fn take_resource(&mut self, par: usize) -> Option<Self::Res> {
        if par < self.partitions.len() {
            self.partitions[par].take()
        } else {
            None
        }
    }
}

pub struct DistributedParResource<T> {
    partitions: Vec<Option<T>>,
    start_index: usize,
}

impl<T> DistributedParResource<T> {
    pub fn new(conf: &JobConf, res: Vec<T>) -> Result<Self, Vec<T>> {
        if res.len() as u32 != conf.workers {
            Err(res)
        } else {
            let mut partitions = Vec::with_capacity(res.len());
            for r in res {
                partitions.push(Some(r));
            }
            let server_conf = conf.servers();
            let servers = match server_conf {
                ServerConf::Local => vec![0],
                ServerConf::Partial(ids) => ids.clone(),
                ServerConf::All => crate::get_servers(),
            };
            let mut start_index = 0 as usize;
            if !servers.is_empty() && (servers.len() > 1) {
                if let Some(my_id) = crate::server_id() {
                    let mut my_index = -1;
                    for (index, id) in servers.iter().enumerate() {
                        if *id == my_id {
                            my_index = index as i64;
                        }
                    }
                    if my_index >= 0 {
                        start_index = (conf.workers * (my_index as u32)) as usize;
                    }
                }
            }
            let pr = DistributedParResource { partitions, start_index };
            Ok(pr)
        }
    }
}

impl<T: Send + Sync + 'static> PartitionedResource for DistributedParResource<T> {
    type Res = T;

    fn get_resource(&self, par: usize) -> Option<&Self::Res> {
        if par >= self.start_index && par < self.start_index + self.partitions.len() {
            self.partitions[par - self.start_index].as_ref()
        } else {
            None
        }
    }

    fn take_resource(&mut self, par: usize) -> Option<Self::Res> {
        if par >= self.start_index && par < self.start_index + self.partitions.len() {
            self.partitions[par - self.start_index].take()
        } else {
            None
        }
    }
}

#[allow(dead_code)]
fn add_resource<T: Any + Send + Sync>(res: T) {
    let type_id = TypeId::of::<T>();
    RESOURCES.with(|store| {
        store
            .borrow_mut()
            .insert(type_id, Box::new(res));
    })
}

#[allow(dead_code)]
fn add_resource_with_key<T: Any + Send + Sync>(key: String, res: T) {
    KEYED_RESOURCES.with(|store| {
        store.borrow_mut().insert(key, Box::new(res));
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn resource_test() {
        // set resource
        {
            let vec = (0..1024).collect::<Vec<_>>();
            add_resource(vec);
            let mut set = HashSet::new();
            for i in 0..1024 {
                set.insert(i);
            }
            add_resource(set);
        }
        // read resource first time
        {
            let vec = get_resource::<Vec<i32>>().expect("resource lost");
            assert_eq!(vec.len(), 1024);
            for i in 0..1024 {
                assert_eq!(i, vec[i] as usize);
            }

            let set = get_resource::<HashSet<i32>>().expect("resource lost");
            assert_eq!(set.len(), 1024);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i < 1024);
            }
        }
        // read resource mut
        {
            let mut vec = get_resource_mut::<Vec<i32>>().expect("resource lost");
            assert_eq!(vec.len(), 1024);
            for i in 0..1024 {
                assert_eq!(i, vec[i] as usize);
            }
            vec.push(1024);

            let mut set = get_resource_mut::<HashSet<i32>>().expect("resource lost");
            assert_eq!(set.len(), 1024);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i < 1024);
            }
            set.insert(1024);
        }

        // read resource after update:
        {
            let vec = get_resource::<Vec<i32>>().expect("resource lost");
            assert_eq!(vec.len(), 1025);
            for i in 0..1025 {
                assert_eq!(i, vec[i] as usize);
            }

            let set = get_resource::<HashSet<i32>>().expect("resource lost");
            assert_eq!(set.len(), 1025);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i <= 1024);
            }
        }
        // read none
        {
            assert!(get_resource::<Vec<u32>>().is_none());
        }
    }

    #[test]
    fn keyed_resources() {
        // set resource
        {
            let vec = (0..1024).collect::<Vec<_>>();
            add_resource_with_key("vec".to_owned(), vec);
            let mut set = HashSet::new();
            for i in 0..1024 {
                set.insert(i);
            }
            add_resource_with_key("set".to_owned(), set);
        }
        // read resource first time
        {
            let vec = get_resource_by_key::<Vec<i32>>("vec").expect("resource lost");
            assert_eq!(vec.len(), 1024);
            for i in 0..1024 {
                assert_eq!(i, vec[i] as usize);
            }

            let set = get_resource_by_key::<HashSet<i32>>("set").expect("resource lost");
            assert_eq!(set.len(), 1024);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i < 1024);
            }
        }
        // read resource mut
        {
            let mut vec = get_resource_mut_by_key::<Vec<i32>>("vec").expect("resource lost");
            assert_eq!(vec.len(), 1024);
            for i in 0..1024 {
                assert_eq!(i, vec[i] as usize);
            }
            vec.push(1024);

            let mut set = get_resource_mut_by_key::<HashSet<i32>>("set").expect("resource lost");
            assert_eq!(set.len(), 1024);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i < 1024);
            }
            set.insert(1024);
        }
        // read resource after update:
        {
            let vec = get_resource_by_key::<Vec<i32>>("vec").expect("resource lost");
            assert_eq!(vec.len(), 1025);
            for i in 0..1025 {
                assert_eq!(i, vec[i] as usize);
            }

            let set = get_resource_by_key::<HashSet<i32>>("set").expect("resource lost");
            assert_eq!(set.len(), 1025);
            for i in 0..2048 {
                assert_eq!(set.contains(&i), i <= 1024);
            }
        }
        {
            assert!(get_resource_by_key::<Vec<u32>>("vec").is_none());
            assert!(get_resource_by_key::<Vec<i32>>("vecx").is_none());
        }
    }
}
