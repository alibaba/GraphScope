//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::HashMap;
use std::cell::{RefCell, UnsafeCell};
use std::rc::Rc;
use dataflow::message::{ExtraEdgeEntity, PropertyEntity};

/// graph store all in memory
#[derive(Clone, Debug)]
pub struct SubGraph {
    pub edges: RefCell<HashMap<(i64, u32), Vec<(i64, u32)>>>,
    pub edge_prop_list: RefCell<Vec<((i64, u32), ExtraEdgeEntity, Vec<PropertyEntity>)>>,
    pub edge_labels: RefCell<Vec<Option<u32>>>,
    pub enable: RefCell<bool>,
}

impl SubGraph {
    pub fn new() -> Self {
        SubGraph {
            edges: RefCell::new(HashMap::new()),
            edge_prop_list: RefCell::new(vec![]),
            edge_labels: RefCell::new(vec![]),
            enable: RefCell::new(false)
        }
    }
}

unsafe impl Send for SubGraph {}
unsafe impl Sync for SubGraph {}
