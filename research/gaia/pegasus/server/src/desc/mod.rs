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

use pegasus_common::downcast::*;
use std::ops::Deref;
use std::sync::Arc;

mod job;
mod operator;

pub use job::JobDesc;
pub use operator::{
    AccumKind, ChannelDesc, DedupDesc, GroupByDesc, LimitDesc, OpKind, OperatorDesc, RepeatDesc,
    SortByDesc, SubtaskDesc, UnionDesc,
};

/// The user resources;
pub trait Resource: AsAny + Send + Sync + 'static {}

impl<T: AsAny + Send + Sync + 'static> Resource for T {}

#[derive(Clone)]
pub struct SharedResource {
    inner: Arc<dyn Resource>,
}

impl Deref for SharedResource {
    type Target = dyn Resource;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl SharedResource {
    pub fn new<R: Resource>(res: R) -> Self {
        SharedResource { inner: Arc::new(res) }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Option<Self> {
        if bytes.is_empty() {
            None
        } else {
            Some(SharedResource { inner: Arc::new(bytes) })
        }
    }

    #[inline]
    pub fn get(&self) -> &dyn Resource {
        self.inner.deref()
    }

    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut dyn Resource> {
        Arc::get_mut(&mut self.inner)
    }

    #[inline]
    pub fn replace<R: Resource>(&mut self, res: R) -> Arc<dyn Resource> {
        std::mem::replace(&mut self.inner, Arc::new(res))
    }
}

impl From<Box<dyn Resource>> for SharedResource {
    fn from(raw: Box<dyn Resource>) -> Self {
        let inner = Arc::from(raw);
        SharedResource { inner }
    }
}

impl From<Vec<u8>> for SharedResource {
    fn from(bytes: Vec<u8>) -> Self {
        SharedResource { inner: Arc::new(bytes) }
    }
}

#[derive(Copy, Clone)]
pub struct EmptyResource;

impl_as_any!(EmptyResource);

pub const fn empty_resource() -> EmptyResource {
    EmptyResource
}

#[allow(dead_code)]
pub static EMPTY: EmptyResource = crate::desc::empty_resource();
