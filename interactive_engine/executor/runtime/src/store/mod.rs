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

use maxgraph_store::api::prelude::Property;
use maxgraph_store::api::{Vertex, Edge};
use maxgraph_store::schema::PropId;

use std::collections::HashMap;
use alloc::vec::IntoIter;
use itertools::Itertools;
use serde::Serialize;

pub mod store_service;
pub mod store_client;
pub mod store_cache;
pub mod store_delegate;
pub mod utils;
pub mod remote_store_service;
pub mod remote_store_client;
pub mod task_partition_manager;
pub mod ffi;
pub mod global_store;
pub mod global_schema;
pub mod graph_builder_ffi;
mod test_global_store;
mod test_ffi_store;
pub mod groot;

pub enum StoreOperatorType {
    VERTEXOUT,
    VERTEXIN,
    VERTEXBOTH,
    VERTEXPROP,
    EDGEOUT,
    EDGEIN,
    EDGEBOTH,
}

#[derive(Clone, Debug)]
pub struct LocalStoreVertex {
    id: i64,
    label_id: u32,
    prop_list: HashMap<u32, Property>,
}

unsafe impl Send for LocalStoreVertex {}

impl LocalStoreVertex {
    pub fn new(id: i64, label_id: u32) -> Self {
        LocalStoreVertex { id, label_id, prop_list: HashMap::new() }
    }

    pub fn add_property(&mut self, prop_id: u32, prop_value: Property) {
        self.prop_list.insert(prop_id, prop_value);
    }

    pub fn add_properties(&mut self, prop_list: Vec<(i32, Property)>) {
        for (propid, propval) in prop_list.into_iter() {
            self.prop_list.insert(propid as u32, propval);
        }
    }
}

impl Vertex for LocalStoreVertex {
    type PI = IntoIter<(PropId, Property)>;

    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_label_id(&self) -> u32 {
        self.label_id
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if let Some(prop) = self.prop_list.get(&prop_id) {
            Some(prop.clone())
        } else {
            None
        }
    }

    fn get_properties(&self) -> Self::PI {
        self.prop_list.clone().into_iter().collect_vec().into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct LocalStoreEdge {
    src: LocalStoreVertex,
    dst: LocalStoreVertex,
    label_id: u32,
    edge_id: i64,
    prop_list: HashMap<PropId, Property>,
}

unsafe impl Send for LocalStoreEdge {}

impl LocalStoreEdge {
    pub fn new(src: LocalStoreVertex, dst: LocalStoreVertex, label_id: u32, edge_id: i64) -> Self {
        LocalStoreEdge {
            src,
            dst,
            label_id,
            edge_id,
            prop_list: HashMap::new(),
        }
    }

    pub fn add_property(&mut self, propid: PropId, propval: Property) {
        self.prop_list.insert(propid, propval);
    }
}

impl Edge for LocalStoreEdge {
    type PI = IntoIter<(PropId, Property)>;

    fn get_label_id(&self) -> u32 {
        self.label_id
    }

    fn get_src_label_id(&self) -> u32 {
        self.src.get_label_id()
    }

    fn get_dst_label_id(&self) -> u32 {
        self.dst.get_label_id()
    }

    fn get_src_id(&self) -> i64 {
        self.src.get_id()
    }

    fn get_dst_id(&self) -> i64 {
        self.dst.get_id()
    }

    fn get_edge_id(&self) -> i64 {
        self.edge_id
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if let Some(propval) = self.prop_list.get(&prop_id) {
            Some(propval.clone())
        } else {
            None
        }
    }

    fn get_properties(&self) -> Self::PI {
        self.prop_list.clone().into_iter().collect_vec().into_iter()
    }
}
