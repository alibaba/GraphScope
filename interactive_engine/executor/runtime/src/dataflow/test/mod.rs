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

use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::*;
use maxgraph_store::api::prelude::Property;
use maxgraph_store::schema::{PropId, Schema};

use execution::build_route_fn;

use std::collections::{HashMap, HashSet};
use alloc::vec::IntoIter;
use itertools::Itertools;
use std::sync::Arc;
use maxgraph_store::schema::prelude::{Relation, DataType, TypeDef, PropDef, Type};
use std::collections::hash_map::RandomState;
use store::{LocalStoreVertex, LocalStoreEdge};
use store::task_partition_manager::TaskPartitionManager;

mod join_test;
mod filter_test;
mod dfs_test;
mod store_test;
mod limitstop_test;
mod key_test;
mod order_test;
mod select_test;
mod iterator_test;
mod vineyard_test;

pub fn build_mock_property_def(prop_id: u32,
                               name: String,
                               data_type: DataType) -> PropDef {
    PropDef::build_def(
        prop_id,
        name,
        data_type,
        "".to_string(),
        None)
}

pub fn build_mock_vertex_type_def(label: u32,
                                  name: String,
                                  props: HashMap<u32, PropDef>) -> TypeDef {
    let mut prop_name_mapping = HashMap::new();
    for (propid, propdef) in props.iter() {
        prop_name_mapping.insert(propdef.get_name().to_owned(), *propid);
    }

    TypeDef::build_def(
        name,
        label,
        Type::Vertex,
        prop_name_mapping,
        props,
        false,
        "".to_string(),
        0,
        Default::default(),
        Default::default())
}

pub fn build_mock_edge_type_def(label: u32,
                                name: String,
                                props: HashMap<u32, PropDef>,
                                relation_list: Vec<(u32, u32)>) -> TypeDef {
    let mut prop_name_mapping = HashMap::new();
    for (propid, propdef) in props.iter() {
        prop_name_mapping.insert(propdef.get_name().to_owned(), *propid);
    }

    let mut relations = HashSet::new();
    for (src_label, dst_label) in relation_list.into_iter() {
        relations.insert(Relation {
            label,
            src_label,
            dst_label,
        });
    }

    TypeDef::build_def(
        name,
        label,
        Type::Edge,
        prop_name_mapping,
        props,
        false,
        "".to_string(),
        0,
        relations,
        Default::default())
}

#[derive(Debug)]
pub struct MockSchema {
    label_name_mapping: HashMap<String, LabelId>,
    prop_name_mapping: HashMap<String, PropId>,
    types: HashMap<LabelId, TypeDef>,
    props: HashMap<PropId, Vec<PropDef>>,
}

impl MockSchema {
    pub fn new(label_name_mapping: HashMap<String, LabelId>,
               prop_name_mapping: HashMap<String, PropId>,
               types: HashMap<LabelId, TypeDef>,
               props: HashMap<PropId, Vec<PropDef>>) -> Self {
        MockSchema {
            label_name_mapping,
            prop_name_mapping,
            types,
            props,
        }
    }
}

impl Schema for MockSchema {
    fn get_prop_id(&self, name: &str) -> Option<u32> {
        if let Some(propid) = self.prop_name_mapping.get(name) {
            return Some(*propid);
        } else {
            return None;
        }
    }

    fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<&DataType> {
        if let Some(type_def) = self.props.get(&label) {
            for prop_def in type_def.iter() {
                return Some(prop_def.get_data_type());
            }
        }

        return None;
    }

    fn get_prop_types(&self, prop_id: u32) -> Option<Vec<&DataType>> {
        let mut prop_datatype_list = vec![];
        for proplist in self.props.values() {
            for proptype in proplist.iter() {
                prop_datatype_list.push(proptype.get_data_type());
            }
        }
        if prop_datatype_list.is_empty() {
            return None;
        } else {
            return Some(prop_datatype_list);
        }
    }

    fn get_prop_name(&self, prop_id: u32) -> Option<&str> {
        for (propname, propid) in self.prop_name_mapping.iter() {
            if prop_id == *propid {
                return Some(propname.as_str());
            }
        }

        return None;
    }

    fn get_label_id(&self, name: &str) -> Option<u32> {
        for type_def in self.types.values() {
            if type_def.get_name().eq(name) {
                return Some(type_def.get_label());
            }
        }

        return None;
    }

    fn get_label_name(&self, label: u32) -> Option<&str> {
        if let Some(type_def) = self.types.get(&label) {
            return Some(type_def.get_name());
        }

        return None;
    }

    fn get_type_def(&self, label: u32) -> Option<&TypeDef> {
        return self.types.get(&label);
    }

    fn get_type_defs(&self) -> Vec<&TypeDef> {
        let mut type_def_list = vec![];
        for type_def in self.types.values() {
            type_def_list.push(type_def);
        }

        return type_def_list;
    }

    fn get_version(&self) -> u32 {
        0
    }

    fn get_partition_num(&self) -> u32 {
        1
    }

    fn to_proto(&self) -> Vec<u8> {
        unimplemented!()
    }
}

pub struct MockGraphPartition {
    vertex_list: HashMap<i64, LocalStoreVertex>,
    out_edge_list: HashMap<i64, Vec<LocalStoreEdge>>,
    in_edge_list: HashMap<i64, Vec<LocalStoreEdge>>,
    partition_id: u32,
}

impl MockGraphPartition {
    pub fn new(partition_id: u32) -> Self {
        MockGraphPartition {
            vertex_list: HashMap::new(),
            out_edge_list: HashMap::new(),
            in_edge_list: HashMap::new(),
            partition_id,
        }
    }

    pub fn insert_vertex(&mut self, vertex: LocalStoreVertex) {
        self.vertex_list.insert(vertex.get_id(), vertex);
    }

    pub fn insert_edge(&mut self, edge: LocalStoreEdge) {
        self.out_edge_list.entry(edge.get_src_id()).or_insert(vec![]).push(edge.clone());
        self.in_edge_list.entry(edge.get_dst_id()).or_insert(vec![]).push(edge);
    }

    pub fn get_partition_id(&self) -> u32 {
        self.partition_id
    }
}

impl MVGraphQuery for MockGraphPartition {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_vertex(&self, _si: i64, id: i64, label: Option<u32>) -> Option<Self::V> {
        if let Some(v) = self.vertex_list.get(&id) {
            if let Some(label_id) = label {
                if v.get_label_id() == label_id {
                    return Some(v.clone());
                }
            } else {
                return Some(v.clone());
            }
        }
        return None;
    }

    fn get_out_edges(&self, _si: i64, src_id: i64, label: Option<u32>) -> Self::EI {
        let edge_list = {
            if let Some(out_edge_list) = self.out_edge_list.get(&src_id) {
                if let Some(label_id) = label {
                    out_edge_list.iter()
                        .filter(|e| e.get_label_id() == label_id)
                        .map(|e| e.clone())
                        .collect_vec()
                } else {
                    out_edge_list.clone()
                }
            } else {
                vec![]
            }
        };
        return edge_list.into_iter();
    }

    fn get_in_edges(&self, _si: i64, dst_id: i64, label: Option<u32>) -> Self::EI {
        let edge_list = {
            if let Some(in_edge_list) = self.in_edge_list.get(&dst_id) {
                if let Some(label_id) = label {
                    in_edge_list.iter()
                        .filter(|e| e.get_label_id() == label_id)
                        .map(|e| e.clone())
                        .collect_vec()
                } else {
                    in_edge_list.clone()
                }
            } else {
                vec![]
            }
        };
        return edge_list.into_iter();
    }

    fn scan(&self, _si: i64, label: Option<u32>) -> Self::VI {
        return self.vertex_list.iter()
            .filter(|(id, v)| {
                if let Some(label_id) = label {
                    v.get_label_id() == label_id
                } else {
                    true
                }
            })
            .map(|(id, v)| v.clone())
            .collect_vec()
            .into_iter();
    }

    fn scan_edges(&self, _si: i64, label: Option<u32>) -> Self::EI {
        return self.out_edge_list.iter()
            .map(|(k, v)| {
                v.clone()
            })
            .flat_map(|v| {
                v.into_iter()
            })
            .filter(|e| {
                if let Some(label_id) = label {
                    e.get_label_id() == label_id
                } else {
                    true
                }
            })
            .collect_vec()
            .into_iter();
    }

    fn count_out_edges(&self, _si: i64, src_id: i64, label: Option<u32>) -> usize {
        if let Some(edge_list) = self.out_edge_list.get(&src_id) {
            edge_list.iter()
                .filter(|e| {
                    if let Some(label_id) = label {
                        e.get_label_id() == label_id
                    } else {
                        true
                    }
                })
                .count()
        } else {
            return 0;
        }
    }

    fn count_in_edges(&self, _si: i64, dst_id: i64, label: Option<u32>) -> usize {
        if let Some(edge_list) = self.in_edge_list.get(&dst_id) {
            edge_list.iter()
                .filter(|e| {
                    if let Some(label_id) = label {
                        e.get_label_id() == label_id
                    } else {
                        true
                    }
                })
                .count()
        } else {
            return 0;
        }
    }

    fn edge_count(&self) -> u64 {
        self.out_edge_list.iter()
            .map(|(k, v)| {
                v.len() as u64
            })
            .sum()
    }

    fn vertex_count(&self) -> u64 {
        self.vertex_list.len() as u64
    }

    fn estimate_vertex_count(&self, label: Option<u32>) -> u64 {
        self.vertex_list.iter().filter(|(id, v)| {
            if let Some(label_id) = label {
                v.get_label_id() == label_id
            } else {
                true
            }
        }).count() as u64
    }

    fn estimate_edge_count(&self, label: Option<u32>) -> u64 {
        self.out_edge_list.iter()
            .map(|(k, v)| {
                if let Some(label_id) = label {
                    v.iter()
                        .filter(|vv| vv.get_label_id() == label_id)
                        .count() as u64
                } else {
                    v.len() as u64
                }
            })
            .sum()
    }
}

pub struct MockGraph {
    partition_list: HashMap<u32, Arc<MockGraphPartition>>,
    schema: Option<Arc<Schema>>,
}

impl MockGraph {
    pub fn build_store(partition_list: Vec<Arc<MockGraphPartition>>) -> Self {
        let mut partition_map = HashMap::new();
        for partition in partition_list.into_iter() {
            partition_map.insert(partition.get_partition_id(), partition);
        }
        MockGraph {
            partition_list: partition_map,
            schema: None,
        }
    }

    pub fn build_from_schema(schema: Option<Arc<Schema>>) -> Self {
        MockGraph {
            partition_list: HashMap::new(),
            schema,
        }
    }
}

impl MVGraph for MockGraph {
    fn get_schema(&self, si: i64) -> Option<Arc<dyn Schema>> {
        if let Some(ref schema_val) = self.schema {
            return Some(schema_val.clone());
        }
        return None;
    }

    fn get_schema_by_version(&self, _version: i32) -> Option<Arc<dyn Schema>> {
        unimplemented!()
    }

    fn get_partition_id(&self, src_id: i64) -> u32 {
        let mut partition_id = src_id % self.partition_list.len() as i64;
        if partition_id < 0 {
            partition_id += self.partition_list.len() as i64;
        }
        return partition_id as u32;
    }

    fn get_partitions(&self) -> Vec<u32> {
        self.partition_list.iter().map(move |(id, p)| *id).collect_vec()
    }

    fn active_partitions(&self, _partitions: Vec<u32>) {
        unimplemented!()
    }

    fn get_serving_types(&self, _si: i64) -> Vec<TypePartition> {
        unimplemented!()
    }

    fn get_partition(&self, partition_id: u32) -> Option<Arc<dyn MVGraphQuery<V=Self::V, E=Self::E, VI=Self::VI, EI=Self::EI>>> {
        if let Some(partition) = self.partition_list.get(&partition_id) {
            return Some(partition.clone());
        }
        return None;
    }

    fn get_dimension_partition(&self) -> Arc<dyn MVGraphQuery<V=Self::V, E=Self::E, VI=Self::VI, EI=Self::EI>> {
        unimplemented!()
    }

    fn clear(&self) {
        unimplemented!()
    }

    fn get_cur_serving_snapshot(&self) -> i64 {
        unimplemented!()
    }
}

impl MVGraphQuery for MockGraph {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_vertex(&self, si: i64, id: i64, label: Option<u32>) -> Option<Self::V> {
        for (_, partition) in self.partition_list.iter() {
            if let Some(v) = partition.get_vertex(si, id, label) {
                return Some(v);
            }
        }
        return None;
    }

    fn get_out_edges(&self, si: i64, src_id: i64, label: Option<u32>) -> Self::EI {
        let mut edge_list = vec![];
        for (_, partition) in self.partition_list.iter() {
            let mut ei = partition.get_out_edges(si, src_id, label).collect_vec();
            edge_list.append(&mut ei);
        }
        edge_list.into_iter()
    }

    fn get_in_edges(&self, si: i64, dst_id: i64, label: Option<u32>) -> Self::EI {
        let mut edge_list = vec![];
        for (_, partition) in self.partition_list.iter() {
            let mut ei = partition.get_in_edges(si, dst_id, label).collect_vec();
            edge_list.append(&mut ei);
        }
        edge_list.into_iter()
    }

    fn scan(&self, si: i64, label: Option<u32>) -> Self::VI {
        let mut vertex_list = vec![];
        for (_, partition) in self.partition_list.iter() {
            let mut vi = partition.scan(si, label).collect_vec();
            vertex_list.append(&mut vi);
        }
        vertex_list.into_iter()
    }

    fn scan_edges(&self, si: i64, label: Option<u32>) -> Self::EI {
        let mut edge_list = vec![];
        for (_, partition) in self.partition_list.iter() {
            let mut ei = partition.scan_edges(si, label).collect_vec();
            edge_list.append(&mut ei);
        }
        edge_list.into_iter()
    }

    fn count_out_edges(&self, si: i64, src_id: i64, label: Option<u32>) -> usize {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.count_out_edges(si, src_id, label);
        }
        count_value
    }

    fn count_in_edges(&self, si: i64, dst_id: i64, label: Option<u32>) -> usize {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.count_in_edges(si, dst_id, label);
        }
        count_value
    }

    fn edge_count(&self) -> u64 {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.edge_count();
        }
        count_value
    }

    fn vertex_count(&self) -> u64 {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.vertex_count();
        }
        count_value
    }

    fn estimate_vertex_count(&self, label: Option<u32>) -> u64 {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.estimate_vertex_count(label);
        }
        count_value
    }

    fn estimate_edge_count(&self, label: Option<u32>) -> u64 {
        let mut count_value = 0;
        for (_, partition) in self.partition_list.iter() {
            count_value += partition.estimate_edge_count(label);
        }
        count_value
    }
}

impl GraphUpdate for MockGraph {
    fn insert_vertex(&self, _si: i64, _label: u32, _schema_version: i32, _id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn replace_vertex(&self, _si: i64, _label: u32, _schema_version: i32, _id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn insert_edge(&self, _si: i64, _relation: Relation, _schema_version: i32, _edge_id: i64, _src_id: i64, _dst_id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn replace_edge(&self, _si: i64, _relation: Relation, _schema_version: i32, _edge_id: i64, _src_id: i64, _dst_id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn update_vertex(&self, _si: i64, _label: u32, _schema_version: i32, _id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn update_edge(&self, _si: i64, _relation: Relation, _schema_version: i32, _edge_id: i64, _src_id: i64, _dst_id: i64, _data: &HashMap<i32, Vec<u8>, RandomState>) {
        unimplemented!()
    }

    fn delete_vertex(&self, _si: i64, _label: u32, _id: i64) {
        unimplemented!()
    }

    fn delete_edge(&self, _si: i64, _relation: Relation, _edge_id: i64, _src_id: i64, _dst_id: i64) {
        unimplemented!()
    }

    fn offline(&self, _si: i64) {
        unimplemented!()
    }

    fn online(&self, _si: i64) {
        unimplemented!()
    }
}

impl GraphLoader for MockGraph {
    fn load_table(&self, _schema_version: i32, _table: TableInfo, _source_data_dir: String) -> Result<(), String> {
        unimplemented!()
    }

    fn online_table(&self, _si: i64, _table: TableInfo) -> bool {
        unimplemented!()
    }
}

impl DDL for MockGraph {
    fn update_schema(&self, _si: i64, _schema_version: i32, _schema: Arc<dyn Schema>) {
        unimplemented!()
    }

    fn drop_vertex_type(&self, _si: i64, _label: u32) {
        unimplemented!()
    }

    fn drop_edge_type(&self, _si: i64, _label: u32) {
        unimplemented!()
    }

    fn drop_relation_ship(&self, _si: i64, _relation: Relation) {
        unimplemented!()
    }
}

pub fn build_test_route() -> impl Fn(&i64) -> u64 + 'static {
    let store_config = StoreConfig {
        worker_id: 0,
        alive_id: 0,
        worker_num: 1,
        zk_url: "".to_string(),
        graph_name: "".to_string(),
        partition_num: 1,
        zk_timeout_ms: 0,
        zk_auth_enable: false,
        zk_auth_user: "test".to_string(),
        zk_auth_password: "test".to_string(),
        hb_interval_ms: 0,
        insert_thread_count: 0,
        download_thread_count: 0,
        hadoop_home: "".to_string(),
        local_data_root: "".to_string(),
        load_thread_count: 0,
        rpc_thread_count: 0,
        rpc_port: 0,
        graph_port: 0,
        query_port: 0,
        engine_port: 0,
        gaia_engine_port: 0,
        timely_worker_per_process: 2,
        monitor_interval_ms: 0,
        total_memory_mb: 0,
        hdfs_default_fs: "".to_string(),
        timely_prepare_dir: "".to_string(),
        replica_count: 0,
        realtime_write_buffer_size: 0,
        realtime_write_ingest_count: 0,
        realtime_write_buffer_mb: 0,
        realtime_write_queue_count: 0,
        realtime_precommit_buffer_size: 0,
        instance_id: "".to_string(),
        engine_name: "time".to_string(),
        pegasus_thread_pool_size: 4_u32,
        graph_type: "".to_string(),
        vineyard_graph_id: 0,
        lambda_enabled: false,
    };
    let route = build_route_fn(&store_config);
    return route;
}

pub fn build_modern_mock_graph() -> MockGraph {
    let mut partition0 = MockGraphPartition::new(0);
    let mut partition1 = MockGraphPartition::new(1);
    // person label: 1, software label: 2, knows label: 3, created label: 4
    // id propid: 1, name propid: 2, age propid: 3, lang propid: 4, weight propid: 5

    //add person data:
//    id,name,age
//    1,marko,29
//    2,vadas,27
//    4,josh,32
//    6,peter,35
    let mut p1 = LocalStoreVertex::new(1, 1);
    p1.add_property(1, Property::Long(1));
    p1.add_property(2, Property::String("marko".to_string()));
    p1.add_property(3, Property::Int(29));
    partition1.insert_vertex(p1.clone());

    let mut p2 = LocalStoreVertex::new(2, 1);
    p2.add_property(1, Property::Long(2));
    p2.add_property(2, Property::String("vadas".to_string()));
    p2.add_property(3, Property::Int(27));
    partition0.insert_vertex(p2.clone());

    let mut p4 = LocalStoreVertex::new(4, 1);
    p4.add_property(1, Property::Long(4));
    p4.add_property(2, Property::String("josh".to_string()));
    p4.add_property(3, Property::Int(32));
    partition0.insert_vertex(p4.clone());

    let mut p6 = LocalStoreVertex::new(6, 1);
    p6.add_property(1, Property::Long(6));
    p6.add_property(2, Property::String("peter".to_string()));
    p6.add_property(3, Property::Int(35));
    partition0.insert_vertex(p6.clone());

    // software data:
//    id,name,lang
//    3,lop,java
//    5,ripple,java
    let mut s3 = LocalStoreVertex::new(3, 2);
    s3.add_property(1, Property::Long(3));
    s3.add_property(2, Property::String("lop".to_string()));
    s3.add_property(4, Property::String("java".to_string()));
    partition1.insert_vertex(s3.clone());

    let mut s5 = LocalStoreVertex::new(5, 2);
    s5.add_property(1, Property::Long(5));
    s5.add_property(2, Property::String("ripple".to_string()));
    s5.add_property(4, Property::String("java".to_string()));
    partition1.insert_vertex(s5.clone());

    // knows data:
//    id,srcId,dstId,weight
//    7,1,2,0.5
//    8,1,4,1.0
    let mut k7 = LocalStoreEdge::new(p1.clone(), p2.clone(), 3, 7);
    k7.add_property(5, Property::Double(0.5));
    partition1.insert_edge(k7);

    let mut k8 = LocalStoreEdge::new(p1.clone(), p4.clone(), 3, 8);
    k8.add_property(5, Property::Double(1.0));
    partition1.insert_edge(k8);

    // created data:
//    id,srcId,dstId,weight
//    9,1,3,0.4
//    10,4,5,1.0
//    11,4,3,0.4
//    12,6,3,0.2
    let mut c9 = LocalStoreEdge::new(p1.clone(), s3.clone(), 4, 9);
    c9.add_property(5, Property::Double(0.4));
    partition1.insert_edge(c9);

    let mut c10 = LocalStoreEdge::new(p4.clone(), s5.clone(), 4, 10);
    c10.add_property(5, Property::Double(1.0));
    partition0.insert_edge(c10);

    let mut c11 = LocalStoreEdge::new(p4.clone(), s3.clone(), 4, 11);
    c11.add_property(5, Property::Double(0.4));
    partition0.insert_edge(c11);

    let mut c12 = LocalStoreEdge::new(p6.clone(), s3.clone(), 4, 12);
    c12.add_property(5, Property::Double(0.2));
    partition0.insert_edge(c12);

    return MockGraph::build_store(vec![Arc::new(partition0), Arc::new(partition1)]);
}

pub fn build_modern_mock_schema() -> MockSchema {
    // person label: 1, software label: 2, knows label: 3, created label: 4
    // id propid: 1, name propid: 2, age propid: 3, lang propid: 4, weight propid: 5
    let id_prop = build_mock_property_def(1, "id".to_owned(), DataType::Long);
    let name_prop = build_mock_property_def(2, "name".to_owned(), DataType::String);
    let age_prop = build_mock_property_def(3, "age".to_owned(), DataType::Int);
    let lang_prop = build_mock_property_def(4, "lang".to_owned(), DataType::String);
    let weight_prop = build_mock_property_def(5, "weight".to_owned(), DataType::Double);

    let mut person_prop_list = HashMap::new();
    person_prop_list.insert(id_prop.get_prop_id(), id_prop.clone());
    person_prop_list.insert(name_prop.get_prop_id(), name_prop.clone());
    person_prop_list.insert(age_prop.get_prop_id(), age_prop.clone());
    let person_def = build_mock_vertex_type_def(1, "person".to_owned(), person_prop_list);

    let mut soft_prop_list = HashMap::new();
    soft_prop_list.insert(id_prop.get_prop_id(), id_prop.clone());
    soft_prop_list.insert(name_prop.get_prop_id(), name_prop.clone());
    soft_prop_list.insert(lang_prop.get_prop_id(), lang_prop.clone());
    let software_def = build_mock_vertex_type_def(2, "software".to_owned(), soft_prop_list);

    let mut knows_prop_list = HashMap::new();
    knows_prop_list.insert(id_prop.get_prop_id(), id_prop.clone());
    knows_prop_list.insert(weight_prop.get_prop_id(), weight_prop.clone());
    let knows_def = build_mock_edge_type_def(3,
                                             "knows".to_owned(),
                                             knows_prop_list,
                                             vec![(person_def.get_label(), person_def.get_label())]);

    let mut created_prop_list = HashMap::new();
    created_prop_list.insert(id_prop.get_prop_id(), id_prop.clone());
    created_prop_list.insert(weight_prop.get_prop_id(), weight_prop.clone());
    let created_def = build_mock_edge_type_def(3,
                                               "knows".to_owned(),
                                               created_prop_list,
                                               vec![(person_def.get_label(), software_def.get_label())]);

    let mut label_name_mapping = HashMap::new();
    let mut prop_name_mapping = HashMap::new();
    let mut types = HashMap::new();
    let mut props = HashMap::new();

    label_name_mapping.insert(person_def.get_name().to_owned(), person_def.get_label());
    label_name_mapping.insert(software_def.get_name().to_owned(), software_def.get_label());
    label_name_mapping.insert(knows_def.get_name().to_owned(), knows_def.get_label());
    label_name_mapping.insert(created_def.get_name().to_owned(), created_def.get_label());

    prop_name_mapping.insert(id_prop.get_name().to_owned(), id_prop.get_prop_id());
    prop_name_mapping.insert(name_prop.get_name().to_owned(), name_prop.get_prop_id());
    prop_name_mapping.insert(age_prop.get_name().to_owned(), age_prop.get_prop_id());
    prop_name_mapping.insert(lang_prop.get_name().to_owned(), lang_prop.get_prop_id());
    prop_name_mapping.insert(weight_prop.get_name().to_owned(), weight_prop.get_prop_id());

    types.insert(person_def.get_label(), person_def);
    types.insert(software_def.get_label(), software_def);
    types.insert(knows_def.get_label(), knows_def);
    types.insert(created_def.get_label(), created_def);

    props.insert(id_prop.get_prop_id(), vec![id_prop]);
    props.insert(name_prop.get_prop_id(), vec![name_prop]);
    props.insert(age_prop.get_prop_id(), vec![age_prop]);
    props.insert(lang_prop.get_prop_id(), vec![lang_prop]);
    props.insert(weight_prop.get_prop_id(), vec![weight_prop]);

    return MockSchema::new(label_name_mapping,
                           prop_name_mapping,
                           types,
                           props);
}
