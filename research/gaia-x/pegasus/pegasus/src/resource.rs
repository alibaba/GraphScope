use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use crossbeam_utils::sync::ShardedLock;

use crate::JobConf;

pub type ResourceMap = HashMap<TypeId, Box<dyn Any + Send + Sync>>;
pub type KeyedResources = HashMap<String, Box<dyn Any + Send + Sync>>;

lazy_static! {
    pub static ref GLOBAL_RESOURCE_MAP: ShardedLock<ResourceMap> = ShardedLock::new(Default::default());
    pub static ref GLOBAL_KEYED_RESOURCES: ShardedLock<KeyedResources> =
        ShardedLock::new(Default::default());
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

pub fn add_global_resource<T: Any + Send + Sync>(key: String, res: T) {
    let mut store = GLOBAL_KEYED_RESOURCES
        .write()
        .expect("global resource write lock poisoned");
    store.insert(key, Box::new(res));
}

pub struct PartitionedResource<T> {
    partitions: Vec<Option<T>>,
}

impl<T> PartitionedResource<T> {
    pub fn new(conf: &JobConf, res: Vec<T>) -> Result<Self, Vec<T>> {
        if res.len() as u32 != conf.workers {
            Err(res)
        } else {
            let mut partitions = Vec::with_capacity(res.len());
            for r in res {
                partitions.push(Some(r));
            }
            let pr = PartitionedResource { partitions };
            Ok(pr)
        }
    }

    pub(crate) fn take_partition_of(&mut self, index: usize) -> Option<T> {
        if index < self.partitions.len() {
            self.partitions[index].take()
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
