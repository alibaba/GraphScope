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
use std::io;

pub trait Collection<T: Send>: Send {
    fn add(&mut self, item: T) -> Result<(), io::Error>;

    fn clear(&mut self);

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;
}

impl<T: Send, C: Collection<T> + ?Sized> Collection<T> for Box<C> {
    fn add(&mut self, item: T) -> Result<(), io::Error> {
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
    fn add(&mut self, item: T) -> Result<(), io::Error> {
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
    fn add(&mut self, item: T) -> Result<(), io::Error> {
        self.push(item);
        Ok(())
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
    fn add(&mut self, item: T) -> Result<(), io::Error> {
        self.insert(item);
        Ok(())
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

impl<T: Eq + Send + Hash> Set<T> for HashSet<T> {
    fn contains(&self, item: &T) -> bool {
        self.contains(item)
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

pub trait Set<T: Send + Eq>: Send + Collection<T> {
    fn contains(&self, item: &T) -> bool;
}

impl<T: Send + Eq, C: Set<T> + ?Sized> Set<T> for Box<C> {
    fn contains(&self, item: &T) -> bool {
        (**self).contains(item)
    }
}

pub trait Map<K: Send + Eq, V: Send>: Send + Debug {
    type Target: Iterator<Item = (K, V)>;

    fn insert(&mut self, k: K, v: V) -> Option<V>;

    fn clear(&mut self);

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;

    fn drain(self: Box<Self>) -> Self::Target;
}

impl<K: Send + Eq, V: Send, M: Map<K, V> + ?Sized> Map<K, V> for Box<M> {
    type Target = M::Target;

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        (**self).insert(k, v)
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

    fn drain(self: Box<Self>) -> Self::Target {
        (*self).drain()
    }
}

pub trait MapFactory<K: Send + Eq, V: Send>: Send {
    type Target: Map<K, V>;

    fn create(&self) -> Self::Target;
}

pub struct DefaultMapFactory<K: Send + Eq, V: Send, M: Map<K, V> + Default> {
    _ph: std::marker::PhantomData<(K, V, M)>,
}

impl<K: Send + Eq, V: Send, M: Map<K, V> + Default> DefaultMapFactory<K, V, M> {
    pub fn new() -> Self {
        DefaultMapFactory { _ph: std::marker::PhantomData }
    }
}

impl<K: Send + Eq, V: Send, M: Map<K, V> + Default> MapFactory<K, V>
    for DefaultMapFactory<K, V, M>
{
    type Target = M;

    fn create(&self) -> Self::Target {
        Default::default()
    }
}

impl<K: Send + Eq, V: Send, F: MapFactory<K, V> + ?Sized> MapFactory<K, V> for Box<F> {
    type Target = F::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }
}

impl<K: Send + Eq, V: Send, M: Map<K, V>> Map<K, V> for ShadeCodec<M> {
    type Target = M::Target;

    fn insert(&mut self, k: K, v: V) -> Option<V> {
        (**self).insert(k, v)
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

    fn drain(self: Box<Self>) -> Self::Target {
        Box::new(self.take()).drain()
    }
}
