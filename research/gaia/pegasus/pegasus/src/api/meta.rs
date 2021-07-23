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

use crate::communication::output::OutputDelta;
use crate::{JobConf, Tag, WorkerId};
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OperatorKind {
    /// TODO : doc
    Source,
    /// TODO : doc
    Map,
    /// TODO : doc
    Expand,
    /// TODO : doc
    Clip,
    /// TODO : doc
    Sink,
    /// TODO : doc
    Unknown,
}

pub trait Priority: Send + Sync {
    fn compare(&self, a: &Tag, b: &Tag) -> std::cmp::Ordering;
}

#[derive(Clone)]
pub enum ScopePrior {
    /// TODO : doc
    None,
    /// TODO : doc
    Prior(Arc<dyn Priority>),
}

#[derive(Clone)]
pub struct OperatorMeta {
    pub worker_id: WorkerId,
    pub name: String,
    pub index: usize,
    pub scope_depth: usize,
    pub(crate) delta: OutputDelta,
    pub(crate) batch_size: usize,
    pub(crate) capacity: usize,
    pub(crate) mem_limit: u32,
    pub(crate) kind: OperatorKind,
    pub(crate) notifiable: bool,
    pub(crate) scope_order: ScopePrior,
}

impl std::fmt::Debug for OperatorMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}]", self.name, self.index)
    }
}

impl OperatorMeta {
    pub fn new(name: &str, worker_id: WorkerId, conf: &Arc<JobConf>) -> Self {
        OperatorMeta {
            worker_id,
            name: name.to_owned(),
            index: 0,
            delta: OutputDelta::None,
            batch_size: conf.batch_size as usize,
            capacity: conf.output_capacity as usize,
            scope_depth: 0,
            mem_limit: conf.memory_limit,
            kind: OperatorKind::Unknown,
            notifiable: false,
            scope_order: ScopePrior::None,
        }
    }

    pub(crate) fn set_scope_depth(&mut self, scope_depth: usize) -> &mut Self {
        self.scope_depth = scope_depth;
        self
    }

    pub(crate) fn set_index(&mut self, index: usize) -> &mut Self {
        self.index = index;
        self
    }

    pub fn set_output_delta(&mut self, delta: OutputDelta) -> &mut Self {
        self.delta = delta;
        self
    }

    pub fn enable_notify(&mut self) -> &mut Self {
        self.notifiable = true;
        self
    }

    pub fn set_scope_order(&mut self, order: ScopePrior) -> &mut Self {
        self.scope_order = order;
        self
    }

    pub fn set_kind(&mut self, kind: OperatorKind) {
        self.kind = kind;
    }
}
