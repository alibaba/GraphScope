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

use crate::api::meta::OperatorMeta;
use crate::api::{EnterScope, Multiplexing, ScopeInput, ScopeInputEmitter};
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

impl<D: Data> Multiplexing<D> for Stream<D> {
    fn scope_by<F>(&self, key: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnMut(&D) -> Option<u32> + Send + Clone + 'static,
    {
        self.dyn_enter(|_| AnonymityEmitter::new(key))
    }

    fn scope_by_size(&self, length: usize) -> Result<Stream<D>, BuildJobError> {
        self.dyn_enter(|meta| FixSizeEmitter::new(length, meta))
    }
}

struct AnonymityEmitter<D: Data, F: FnMut(&D) -> Option<u32> + Send + Clone + 'static> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data, F: FnMut(&D) -> Option<u32> + Send + Clone + 'static> AnonymityEmitter<D, F> {
    pub fn new(func: F) -> Self {
        AnonymityEmitter { func, _ph: std::marker::PhantomData }
    }
}

impl<D: Data, F: FnMut(&D) -> Option<u32> + Send + Clone + 'static> Clone
    for AnonymityEmitter<D, F>
{
    fn clone(&self) -> Self {
        AnonymityEmitter { func: self.func.clone(), _ph: std::marker::PhantomData }
    }
}

impl<D: Data, F: FnMut(&D) -> Option<u32> + Send + Clone + 'static> ScopeInputEmitter<D>
    for AnonymityEmitter<D, F>
{
    fn get_scope(&mut self, data: D) -> Option<ScopeInput<D>> {
        if let Some(id) = (self.func)(&data) {
            Some(ScopeInput::new(id, false, data))
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct FixSizeEmitter<D: Data> {
    size: usize,
    count: usize,
    next_id: u32,
    id_interval: u32,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> FixSizeEmitter<D> {
    pub fn new(size: usize, meta: &OperatorMeta) -> Self {
        FixSizeEmitter {
            size,
            count: 0,
            next_id: meta.worker_id.index,
            id_interval: meta.worker_id.peers,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D: Data> ScopeInputEmitter<D> for FixSizeEmitter<D> {
    fn has_peers(&self) -> bool {
        false
    }

    fn get_scope(&mut self, data: D) -> Option<ScopeInput<D>> {
        self.count += 1;
        if self.count == self.size {
            self.count = 0;
        }
        let is_last = self.count == 0;
        let id = self.next_id;
        if is_last {
            self.next_id += self.id_interval;
        }
        Some(ScopeInput::new(id, is_last, data))
    }
}

mod subtask;
