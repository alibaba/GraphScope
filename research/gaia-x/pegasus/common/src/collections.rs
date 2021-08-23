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

use std::collections::HashSet;
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

pub trait Set<T: Send>: Collection<T> {
    fn contains(&self, item: &T) -> bool;
}

impl<T: Send, C: Set<T> + ?Sized> Set<T> for Box<C> {
    fn contains(&self, item: &T) -> bool {
        (**self).contains(item)
    }
}

impl<T: Eq + Send + Hash> Set<T> for HashSet<T> {
    fn contains(&self, item: &T) -> bool {
        self.contains(item)
    }
}

pub trait Map<K: Send, V: Send>: Collection<(K, V)> {
    fn insert(&mut self, key: K, value: V) -> Result<(), io::Error> {
        self.add((key, value))
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V>;

    fn get(&self, key: &K) -> Option<&V>;

    fn contains(&self, key: &K) -> bool;
}
