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

use crate::api::function::{FnResult, Partition};
use crate::codec::{Decode, Encode};
use pegasus_common::collections::{Collection, Map};
use pegasus_common::io::{ReadExt, WriteExt};
use std::fmt::Debug;
use std::io;
use std::ops::{Deref, DerefMut};

mod dedup;
mod exchange;
mod filter;
mod fold;
mod map;
mod reduce;

#[inline]
pub fn never_clone<T>(raw: T) -> NeverClone<T> {
    NeverClone { inner: raw }
}

pub struct NeverClone<T> {
    inner: T,
}

impl<T> Clone for NeverClone<T> {
    fn clone(&self) -> Self {
        unreachable!("can't clone;")
    }
}

#[allow(dead_code)]
impl<T> NeverClone<T> {
    pub fn take(self) -> T {
        self.inner
    }
}

impl<T> Deref for NeverClone<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for NeverClone<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: Debug> Debug for NeverClone<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<T: Encode> Encode for NeverClone<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.inner.write_to(writer)
    }
}

impl<T: Decode> Decode for NeverClone<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let inner = T::read_from(reader)?;
        Ok(never_clone(inner))
    }
}

impl<T: Send, C: Collection<T>> Collection<T> for NeverClone<C> {
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

impl<K: Send + Eq, V: Send, M: Map<K, V>> Map<K, V> for NeverClone<M> {
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

/// To be removed: for test
impl<T> PartialEq for NeverClone<T> {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl<T> Eq for NeverClone<T> {}

impl<T> Partition for NeverClone<T> {
    fn get_partition(&self) -> FnResult<u64> {
        unimplemented!()
    }
}
