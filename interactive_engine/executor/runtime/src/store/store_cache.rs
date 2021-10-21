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

use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use maxgraph_store::api::prelude::Property;
use itertools::Itertools;

const SNAPSHOT_COUNT: usize = 3;
const CACHE_ITEM_COUNT: usize = 10000;

#[derive(Clone)]
pub struct StoreEdgeVertexId {
    edge_id: i64,
    edge_label_id: i32,
    target_id: i64,
    target_label_id: i32,
}

impl StoreEdgeVertexId {
    pub fn new(edge_id: i64,
               edge_label_id: i32,
               target_id: i64,
               target_label_id: i32) -> Self {
        StoreEdgeVertexId {
            edge_id,
            edge_label_id,
            target_id,
            target_label_id,
        }
    }

    pub fn get_target_id(&self) -> i64 {
        self.target_id
    }

    pub fn get_target_label_id(&self) -> i32 {
        self.target_label_id
    }

    pub fn get_edge_id(&self) -> i64 {
        self.edge_id
    }

    pub fn get_edge_label_id(&self) -> i32 {
        self.edge_label_id
    }
}

struct SnapshotStoreCache {
    out_label_list: HashMap<i64, HashMap<i32, Vec<StoreEdgeVertexId>>>,
    in_label_list: HashMap<i64, HashMap<i32, Vec<StoreEdgeVertexId>>>,
    vertex_prop_list: HashMap<i64, HashMap<i32, Property>>,
    vertex_all_prop_list: HashSet<i64>,
    edge_prop_list: HashMap<i64, HashMap<i32, Property>>,
    edge_all_prop_list: HashSet<i64>,
}

//impl SnapshotStoreCache {
//    pub fn new() -> Self {
//        SnapshotStoreCache {
//            out_label_list: HashMap::new(),
//            in_label_list: HashMap::new(),
//            vertex_prop_list: HashMap::new(),
//            vertex_all_prop_list: HashSet::new(),
//            edge_prop_list: HashMap::new(),
//            edge_all_prop_list: HashSet::new(),
//        }
//    }
//
//    pub fn add_out_target(&mut self,
//                          vid: i64,
//                          edge_vertex_id: StoreEdgeVertexId) {
//        if self.out_list.len() > CACHE_ITEM_COUNT {
//            self.out_list.clear();
//        }
//        self.out_list.entry(vid).or_insert(vec![]).push(edge_vertex_id);
//    }
//
//    pub fn add_in_target(&mut self,
//                         vid: i64,
//                         edge_vertex_id: StoreEdgeVertexId) {
//        if self.in_list.len() > CACHE_ITEM_COUNT {
//            self.in_list.clear();
//        }
//        self.in_list.entry(vid).or_insert(vec![]).push(edge_vertex_id);
//    }
//
//    pub fn get_out_target(&self, vid: &i64) -> Option<&Vec<StoreEdgeVertexId>> {
//        self.out_list.get(vid)
//    }
//
//    pub fn get_in_target(&self, vid: &i64) -> Option<&Vec<StoreEdgeVertexId>> {
//        self.in_list.get(vid)
//    }
//
//    pub fn add_vertex_prop(&mut self,
//                           vid: i64,
//                           props: Vec<(i32, Property)>) {
//        if self.vertex_prop_list.len() > CACHE_ITEM_COUNT {
//            self.vertex_prop_list.clear();
//            self.vertex_all_prop_list.clear();
//        }
//        let prop_list = self.vertex_prop_list.entry(vid).or_insert(HashMap::new());
//        for (propid, propval) in props.into_iter() {
//            prop_list.insert(propid, propval);
//        }
//    }
//
//    pub fn add_vertex_all_prop(&mut self,
//                               vid: i64,
//                               props: Vec<(i32, Property)>) {
//        self.add_vertex_prop(vid, props);
//        self.vertex_all_prop_list.insert(vid);
//    }
//
//    pub fn is_vertex_all_prop(&self, vid: i64) -> bool {
//        return self.vertex_all_prop_list.contains(&vid);
//    }
//
//    pub fn get_vertex_prop(&self,
//                           vid: i64) -> Option<&HashMap<i32, Property>> {
//        return self.vertex_prop_list.get(&vid);
//    }
//}

//pub struct StoreCacheManager {
//    snapshot_cache_list: RefCell<HashMap<i64, SnapshotStoreCache>>,
//}
//
//unsafe impl Sync for StoreCacheManager {}
//
//impl StoreCacheManager {
//    pub fn new() -> StoreCacheManager {
//        StoreCacheManager {
//            snapshot_cache_list: RefCell::new(HashMap::new()),
//        }
//    }
//
//    fn get_min_snapshot(&self) -> i64 {
//        *self.snapshot_cache_list.borrow().keys().min().unwrap()
//    }
//
//    pub fn get_vertex_out(&self, vid: i64, si: i64) -> Option<Vec<StoreVertexId>> {
//        if let Some(snapshot_store_cache) = self.snapshot_cache_list.borrow().get(&si) {
//            if let Some(store_vertex_list) = snapshot_store_cache.get_vertex_out(&vid) {
//                return Some(store_vertex_list.to_vec());
//            }
//        }
//
//        return None;
//    }
//
//    pub fn get_vertex_in(&self, vid: i64, si: i64) -> Option<Vec<StoreVertexId>> {
//        if let Some(snapshot_store_cache) = self.snapshot_cache_list.borrow().get(&si) {
//            if let Some(store_vertex_list) = snapshot_store_cache.get_vertex_in(&vid) {
//                return Some(store_vertex_list.to_vec());
//            }
//        }
//
//        return None;
//    }
//
//    pub fn add_vertex_out(&self, vid: i64, vertex: StoreVertexId, si: i64) {
//        self.update_cache_snapshot(si);
//        let mut snapshot_borrow = self.snapshot_cache_list.borrow_mut();
//        let snapshot_store_cache = snapshot_borrow.entry(si).or_insert(SnapshotStoreCache::new());
//        snapshot_store_cache.add_vertex_out(vid, vertex);
//    }
//
//    fn update_cache_snapshot(&self, si: i64) {
//        if !self.snapshot_cache_list.borrow().contains_key(&si) &&
//            self.snapshot_cache_list.borrow().len() >= SNAPSHOT_COUNT {
//            let min_si = self.get_min_snapshot();
//            self.snapshot_cache_list.borrow_mut().remove(&min_si);
//        }
//    }
//
//    pub fn add_vertex_in(&self, vid: i64, vertex: StoreVertexId, si: i64) {
//        self.update_cache_snapshot(si);
//        let mut snapshot_borrow = self.snapshot_cache_list.borrow_mut();
//        let snapshot_store_cache = snapshot_borrow.entry(si).or_insert(SnapshotStoreCache::new());
//        snapshot_store_cache.add_vertex_in(vid, vertex);
//    }
//
//    pub fn get_vertex_props(&self, vid: i64, si: i64) -> Option<HashMap<i32, Property>> {
//        if let Some(snapshot_cache) = self.snapshot_cache_list.borrow().get(&si) {
//            if let Some(vertex_props) = snapshot_cache.get_vertex_prop(vid) {
//                return Some(vertex_props.clone());
//            }
//        }
//        return None;
//    }
//
//    pub fn is_vertex_all_props(&self, vid: i64, si: i64) -> bool {
//        if let Some(snapshot_cache) = self.snapshot_cache_list.borrow().get(&si) {
//            return snapshot_cache.is_vertex_all_prop(vid);
//        }
//
//        return false;
//    }
//
//    pub fn add_vertex_prop(&self,
//                           vid: i64,
//                           si: i64,
//                           props: Vec<(i32, Property)>,
//                           all_prop_flag: bool) {
//        self.update_cache_snapshot(si);
//
//        let mut snapshot_borrow = self.snapshot_cache_list.borrow_mut();
//        let snapshot_store_cache = snapshot_borrow.entry(si).or_insert(SnapshotStoreCache::new());
//        if all_prop_flag {
//            snapshot_store_cache.add_vertex_all_prop(vid, props);
//        } else {
//            snapshot_store_cache.add_vertex_prop(vid, props);
//        }
//    }
//}
