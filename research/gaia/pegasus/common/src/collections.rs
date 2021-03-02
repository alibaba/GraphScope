//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//! http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::codec::ShadeCodec;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub trait Collection<T: Send>: Send {
    fn add(&mut self, item: T) -> Option<T>;

    fn clear(&mut self);

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;
}

pub trait Drain<T: Send>: Send {
    type Target: Iterator<Item = T>;

    fn drain(&mut self) -> Self::Target;
}

pub trait DrainColleciton<T: Send>: Collection<T> + Drain<T> {}

impl<T: Send, C: Collection<T> + ?Sized> Collection<T> for Box<C> {
    fn add(&mut self, item: T) -> Option<T> {
        (**self).add(item)
    }

    fn clear(&mut self) {
        (**self).clear()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn len(&self) -> usize {
        (**self).len()
    }
}

impl<T: Send, C: Collection<T>> Collection<T> for ShadeCodec<C> {
    fn add(&mut self, item: T) -> Option<T> {
        (**self).add(item)
    }

    fn clear(&mut self) {
        (**self).clear()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn len(&self) -> usize {
        (**self).len()
    }
}

pub trait Iterable<T: Send>: Collection<T> + Iterator<Item = T> {}

impl<T: Send, C: Collection<T> + Iterator<Item = T>> Iterable<T> for C {}

pub trait IntoIterable<T: Send>: Collection<T> + IntoIterator<Item = T> {}

impl<T: Send, C: Collection<T> + IntoIterator<Item = T>> IntoIterable<T> for C {}

pub trait CollectionFactory<T: Send>: Send {
    type Target: Collection<T>;

    fn create(&self) -> Self::Target;
}

impl<T: Send, C: CollectionFactory<T> + ?Sized> CollectionFactory<T> for Box<C> {
    type Target = C::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }
}

pub struct DefaultCollectionFactory<T: Send, C: Collection<T> + Default> {
    _ph: std::marker::PhantomData<(T, C)>,
}

impl<T: Send, C: Collection<T> + Default> DefaultCollectionFactory<T, C> {
    pub fn new() -> Self {
        DefaultCollectionFactory { _ph: std::marker::PhantomData }
    }
}

impl<T: Send, C: Collection<T> + Default> CollectionFactory<T> for DefaultCollectionFactory<T, C> {
    type Target = C;

    fn create(&self) -> Self::Target {
        Default::default()
    }
}

impl<T: Send> Collection<T> for Vec<T> {
    fn add(&mut self, item: T) -> Option<T> {
        self.push(item);
        None
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

pub struct VecFactory<T: Send> {
    capacity: Option<usize>,
    _ph: std::marker::PhantomData<T>,
}

impl<T: Send> CollectionFactory<T> for VecFactory<T> {
    type Target = Vec<T>;

    fn create(&self) -> Self::Target {
        if let Some(cap) = self.capacity {
            Vec::with_capacity(cap)
        } else {
            Vec::new()
        }
    }
}

impl<T: Eq + Hash + Send> Collection<T> for HashSet<T> {
    fn add(&mut self, item: T) -> Option<T> {
        self.insert(item);
        None
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

pub struct HashSetFactory<T: Eq + Hash + Send> {
    capacity: Option<usize>,
    _ph: std::marker::PhantomData<T>,
}

impl<T: Eq + Hash + Send> CollectionFactory<T> for HashSetFactory<T> {
    type Target = HashSet<T>;

    fn create(&self) -> Self::Target {
        if let Some(cap) = self.capacity {
            HashSet::with_capacity(cap)
        } else {
            HashSet::new()
        }
    }
}

pub trait Set<T: Send + Eq>: Send + Debug + Collection<T> {}

pub trait SetFactory<T: Send + Eq>: Send {
    type Target: Set<T>;

    fn create(&self) -> Self::Target;
}

impl<T: Send + Eq, C: SetFactory<T> + ?Sized> SetFactory<T> for Box<C> {
    type Target = C::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }
}

pub trait DrainSet<T: Send + Eq>: Set<T> + Drain<T> {}

pub trait DrainSetFactory<T: Send + Eq>: Send {
    type Target: DrainSet<T>;

    fn create(&self) -> Self::Target;
}

impl<T: Send + Eq, C: DrainSetFactory<T> + ?Sized> DrainSetFactory<T> for Box<C> {
    type Target = C::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }
}

impl<T: Send + Eq, C: DrainSet<T> + ?Sized> Drain<T> for Box<C> {
    type Target = C::Target;

    fn drain(&mut self) -> Self::Target {
        (**self).drain()
    }
}

impl<T: Send + Eq, C: DrainSet<T> + ?Sized> Set<T> for Box<C> {}

impl<T: Send + Eq, C: DrainSet<T> + ?Sized> DrainSet<T> for Box<C> {}
