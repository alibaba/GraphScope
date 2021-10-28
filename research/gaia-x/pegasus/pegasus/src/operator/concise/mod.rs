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

use std::fmt::Debug;
use std::io;
use std::ops::{Deref, DerefMut};

use pegasus_common::collections::Collection;
use pegasus_common::io::{ReadExt, WriteExt};

use crate::codec::{Decode, Encode};

mod any;
mod collect;
mod correlate;
mod count;
mod filter;
mod fold;
mod keyed;
mod limit;
mod map;
mod merge;
mod order;
mod reduce;

#[inline]
fn never_clone<T>(raw: T) -> NeverClone<T> {
    NeverClone { inner: raw }
}

struct NeverClone<T> {
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
