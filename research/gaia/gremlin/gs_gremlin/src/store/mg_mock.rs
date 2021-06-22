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

use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::graph_schema::Schema;
use maxgraph_store::api::prelude::Property;
use maxgraph_store::api::{
    Condition, Edge, GlobalGraphQuery, LabelId, PartitionId, PartitionLabeledVertexIds,
    PartitionVertexIds, PropId, Vertex, VertexId,
};
use maxgraph_store::schema::prelude::DataType;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::IntoIter;

#[derive(Clone, Debug)]
pub struct LocalStoreVertex {
    pub id: i64,
    pub label_id: u32,
    pub prop_list: HashMap<u32, Property>,
}

impl LocalStoreVertex {
    pub fn new(id: i64, label_id: u32) -> Self {
        LocalStoreVertex {
            id,
            label_id,
            prop_list: Default::default(),
        }
    }

    pub fn add_property(&mut self, propid: PropId, propval: Property) {
        self.prop_list.insert(propid, propval);
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
        self.prop_list
            .clone()
            .into_iter()
            .collect::<Vec<(PropId, Property)>>()
            .into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct LocalStoreEdge {
    pub src: LocalStoreVertex,
    pub dst: LocalStoreVertex,
    pub label_id: u32,
    pub edge_id: i64,
    pub prop_list: HashMap<PropId, Property>,
}

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

    pub fn add_property(&mut self, prop_id: PropId, prop_val: Property) {
        self.prop_list.insert(prop_id, prop_val);
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
        self.prop_list
            .clone()
            .into_iter()
            .collect::<Vec<(PropId, Property)>>()
            .into_iter()
    }
}

unsafe impl Send for LocalStoreVertex {}
unsafe impl Sync for LocalStoreVertex {}
unsafe impl Send for LocalStoreEdge {}
unsafe impl Sync for LocalStoreEdge {}

unsafe impl Send for MockGraphPartition {}
unsafe impl Sync for MockGraphPartition {}
unsafe impl Send for MockGraph {}
unsafe impl Sync for MockGraph {}

pub struct MockGraphPartition {
    vertex_list: HashMap<i64, LocalStoreVertex>,
    out_edge_list: HashMap<i64, Vec<LocalStoreEdge>>,
    in_edge_list: HashMap<i64, Vec<LocalStoreEdge>>,
    partition_id: PartitionId,
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
        self.out_edge_list
            .entry(edge.get_src_id())
            .or_insert(vec![])
            .push(edge.clone());
        self.in_edge_list
            .entry(edge.get_dst_id())
            .or_insert(vec![])
            .push(edge);
    }

    pub fn get_partition_id(&self) -> u32 {
        self.partition_id
    }
}

// TODO(bingqing): confirm with graphscope: output_prop_ids of Some(vec![]) means no property needed; and output_prop_ids of None means all properties needed
fn clone_vertex_with_property(
    v: &LocalStoreVertex,
    output_prop_ids: Option<&Vec<u32>>,
) -> LocalStoreVertex {
    if let Some(props_ids) = output_prop_ids {
        let mut vertex = LocalStoreVertex::new(v.get_id(), v.get_label_id());
        for pid in props_ids {
            vertex.add_property(*pid, v.get_property(*pid).expect("get prop id failed"));
        }
        vertex
    } else {
        v.clone()
    }
}

fn clone_edge_with_property(
    e: &LocalStoreEdge,
    output_prop_ids: Option<&Vec<u32>>,
) -> LocalStoreEdge {
    if let Some(props_ids) = output_prop_ids {
        let mut edge = LocalStoreEdge::new(
            e.src.clone(),
            e.dst.clone(),
            e.get_label_id(),
            e.get_edge_id(),
        );
        for pid in props_ids {
            edge.add_property(*pid, e.get_property(*pid).expect("get prop id failed"));
        }
        edge
    } else {
        e.clone()
    }
}

impl MockGraphPartition {
    fn get_out_vertex_ids(
        &self,
        _si: i64,
        src_ids: &Vec<i64>,
        edge_labels: &Vec<u32>,
        _condition: Option<&Condition>,
        _dedup_prop_ids: Option<&Vec<u32>>,
        _limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, IntoIter<LocalStoreVertex>)>> {
        let mut result = Vec::new();
        for src_id in src_ids {
            if let Some(out_edge_list) = self.out_edge_list.get(src_id) {
                let mut out_vertices = Vec::new();
                for out_edge in out_edge_list {
                    if edge_labels.is_empty() || edge_labels.contains(&out_edge.get_label_id()) {
                        out_vertices.push(LocalStoreVertex::new(
                            out_edge.get_dst_id(),
                            out_edge.get_dst_label_id(),
                        ))
                    }
                }
                result.push((*src_id, out_vertices.into_iter()));
            }
        }
        Box::new(result.into_iter())
    }

    fn get_out_edges(
        &self,
        _si: i64,
        src_ids: &Vec<i64>,
        edge_labels: &Vec<u32>,
        _condition: Option<&Condition>,
        _dedup_prop_ids: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        _limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, IntoIter<LocalStoreEdge>)>> {
        let mut result = Vec::new();
        for src_id in src_ids {
            if let Some(out_edge_list) = self.out_edge_list.get(src_id) {
                let mut out_edges = vec![];
                for out_edge in out_edge_list {
                    if edge_labels.is_empty() || edge_labels.contains(&out_edge.get_label_id()) {
                        let e = clone_edge_with_property(out_edge, output_prop_ids);
                        out_edges.push(e);
                    }
                }
                result.push((*src_id, out_edges.into_iter()));
            }
        }
        Box::new(result.into_iter())
    }

    fn get_in_vertex_ids(
        &self,
        _si: i64,
        dst_ids: &Vec<i64>,
        edge_labels: &Vec<u32>,
        _condition: Option<&Condition>,
        _dedup_prop_ids: Option<&Vec<u32>>,
        _limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, IntoIter<LocalStoreVertex>)>> {
        let mut result = Vec::new();
        for dst_id in dst_ids {
            if let Some(out_edge_list) = self.in_edge_list.get(dst_id) {
                let mut out_vertices = Vec::new();
                for out_edge in out_edge_list {
                    if edge_labels.is_empty() || edge_labels.contains(&out_edge.get_label_id()) {
                        out_vertices.push(LocalStoreVertex::new(
                            out_edge.get_src_id(),
                            out_edge.get_src_label_id(),
                        ))
                    }
                }
                result.push((*dst_id, out_vertices.into_iter()));
            }
        }
        Box::new(result.into_iter())
    }

    fn get_in_edges(
        &self,
        _si: i64,
        dst_ids: &Vec<i64>,
        edge_labels: &Vec<u32>,
        _: Option<&Condition>,
        _: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        _: usize,
    ) -> Box<dyn Iterator<Item = (i64, IntoIter<LocalStoreEdge>)>> {
        let mut result = Vec::new();
        for dst_id in dst_ids {
            if let Some(out_edge_list) = self.in_edge_list.get(dst_id) {
                let mut out_edges = vec![];
                for out_edge in out_edge_list {
                    if edge_labels.is_empty() || edge_labels.contains(&out_edge.get_label_id()) {
                        let e = clone_edge_with_property(out_edge, output_prop_ids);
                        out_edges.push(e);
                    }
                }
                result.push((*dst_id, out_edges.into_iter()));
            }
        }
        Box::new(result.into_iter())
    }

    fn get_vertex_properties(
        &self,
        _: i64,
        label_ids: &Vec<(Option<u32>, Vec<i64>)>,
        output_prop_ids: Option<&Vec<u32>>,
    ) -> IntoIter<LocalStoreVertex> {
        let mut result = vec![];
        for (_label, ids) in label_ids {
            for id in ids {
                if let Some(vertex) = self.vertex_list.get(id) {
                    let v = clone_vertex_with_property(vertex, output_prop_ids);
                    result.push(v);
                }
            }
        }
        result.into_iter()
    }

    fn get_all_vertices(
        &self,
        _: i64,
        labels: &Vec<u32>,
        _: Option<&Condition>,
        _: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        _: usize,
        _: &Vec<u32>,
    ) -> IntoIter<LocalStoreVertex> {
        let mut result = vec![];
        for (_vid, vertex) in self.vertex_list.iter() {
            if labels.is_empty() || labels.contains(&vertex.get_label_id()) {
                let v = clone_vertex_with_property(vertex, output_prop_ids);
                result.push(v);
            }
        }
        result.into_iter()
    }

    fn get_all_edges(
        &self,
        _: i64,
        labels: &Vec<u32>,
        _: Option<&Condition>,
        _: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        _: usize,
        _: &Vec<u32>,
    ) -> IntoIter<LocalStoreEdge> {
        let mut result = vec![];
        for (_vid, edges) in self.out_edge_list.iter() {
            for edge in edges {
                if labels.is_empty() || labels.contains(&edge.get_label_id()) {
                    let e = clone_edge_with_property(edge, output_prop_ids);
                    result.push(e);
                }
            }
        }
        result.into_iter()
    }
}

#[derive(Default, Clone)]
pub struct MockGraph {
    partition_list: HashMap<u32, Arc<MockGraphPartition>>,
    schema: Option<Arc<dyn Schema>>,
}

impl MockGraph {
    pub fn build_store(
        partition_list: Vec<Arc<MockGraphPartition>>,
        schema: Option<Arc<dyn Schema>>,
    ) -> Self {
        let mut partition_map = HashMap::new();
        for partition in partition_list.into_iter() {
            partition_map.insert(partition.get_partition_id(), partition);
        }
        MockGraph {
            partition_list: partition_map,
            schema,
        }
    }
}

impl GlobalGraphQuery for MockGraph {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_out_vertex_ids(
        &self,
        si: i64,
        src_ids: Vec<PartitionVertexIds>,
        edge_labels: &Vec<u32>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, Self::VI)>> {
        let mut res = vec![];
        for (pid, partition_ids) in src_ids.iter() {
            let iter = self.partition_list.get(pid).unwrap().get_out_vertex_ids(
                si,
                partition_ids,
                edge_labels,
                condition,
                dedup_prop_ids,
                limit,
            );
            for (src, viter) in iter {
                res.push((src, viter));
            }
        }
        Box::new(res.into_iter())
    }

    fn get_out_edges(
        &self,
        si: i64,
        src_ids: Vec<PartitionVertexIds>,
        edge_labels: &Vec<u32>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, Self::EI)>> {
        let mut res = vec![];
        for (pid, partition_ids) in src_ids.iter() {
            let iter = self.partition_list.get(pid).unwrap().get_out_edges(
                si,
                partition_ids,
                edge_labels,
                condition,
                dedup_prop_ids,
                output_prop_ids,
                limit,
            );
            for (src, viter) in iter {
                res.push((src, viter));
            }
        }
        Box::new(res.into_iter())
    }

    fn get_in_vertex_ids(
        &self,
        si: i64,
        dst_ids: Vec<(u32, Vec<i64>)>,
        edge_labels: &Vec<u32>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, Self::VI)>> {
        let mut res = vec![];
        for (pid, partition_ids) in dst_ids.iter() {
            let iter = self.partition_list.get(pid).unwrap().get_in_vertex_ids(
                si,
                partition_ids,
                edge_labels,
                condition,
                dedup_prop_ids,
                limit,
            );
            for (src, viter) in iter {
                res.push((src, viter));
            }
        }
        Box::new(res.into_iter())
    }

    fn get_in_edges(
        &self,
        si: i64,
        dst_ids: Vec<(u32, Vec<i64>)>,
        edge_labels: &Vec<u32>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        limit: usize,
    ) -> Box<dyn Iterator<Item = (i64, Self::EI)>> {
        let mut res = vec![];
        for (pid, partition_ids) in dst_ids.iter() {
            let iter = self.partition_list.get(pid).unwrap().get_in_edges(
                si,
                partition_ids,
                edge_labels,
                condition,
                dedup_prop_ids,
                output_prop_ids,
                limit,
            );
            for (src, viter) in iter {
                res.push((src, viter));
            }
        }
        Box::new(res.into_iter())
    }

    fn count_out_edges(
        &self,
        _: i64,
        _: Vec<(u32, Vec<i64>)>,
        _: &Vec<u32>,
        _: Option<&Condition>,
    ) -> Box<dyn Iterator<Item = (i64, usize)>> {
        unimplemented!()
    }

    fn count_in_edges(
        &self,
        _: i64,
        _: Vec<(u32, Vec<i64>)>,
        _: &Vec<u32>,
        _: Option<&Condition>,
    ) -> Box<dyn Iterator<Item = (i64, usize)>> {
        unimplemented!()
    }

    fn get_vertex_properties(
        &self,
        si: i64,
        ids: Vec<PartitionLabeledVertexIds>,
        output_prop_ids: Option<&Vec<u32>>,
    ) -> Self::VI {
        let mut res = vec![];
        for (pid, partition_label_ids) in ids.iter() {
            let iter = self.partition_list.get(pid).unwrap().get_vertex_properties(
                si,
                partition_label_ids,
                output_prop_ids,
            );
            for v in iter {
                res.push(v);
            }
        }
        res.into_iter()
    }

    fn get_edge_properties(
        &self,
        _: i64,
        _: Vec<(u32, Vec<(Option<u32>, Vec<i64>)>)>,
        _: Option<&Vec<u32>>,
    ) -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(
        &self,
        si: i64,
        labels: &Vec<u32>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        limit: usize,
        partition_ids: &Vec<u32>,
    ) -> Self::VI {
        let mut res = vec![];
        let partition_ids = if partition_ids.is_empty() {
            self.partition_list.keys().map(|x| *x).collect()
        } else {
            partition_ids.clone()
        };
        for pid in partition_ids {
            let iter = self.partition_list.get(&pid).unwrap().get_all_vertices(
                si,
                labels,
                condition,
                dedup_prop_ids,
                output_prop_ids,
                limit,
                &vec![],
            );
            for v in iter {
                res.push(v);
            }
        }
        res.into_iter()
    }

    fn get_all_edges(
        &self,
        si: i64,
        labels: &Vec<LabelId>,
        condition: Option<&Condition>,
        dedup_prop_ids: Option<&Vec<u32>>,
        output_prop_ids: Option<&Vec<u32>>,
        limit: usize,
        partition_ids: &Vec<PartitionId>,
    ) -> Self::EI {
        let mut res = vec![];
        let mut eids = vec![];
        let partition_ids = if partition_ids.is_empty() {
            self.partition_list.keys().map(|x| *x).collect()
        } else {
            partition_ids.clone()
        };
        for pid in partition_ids {
            let iter = self.partition_list.get(&pid).unwrap().get_all_edges(
                si,
                labels,
                condition,
                dedup_prop_ids,
                output_prop_ids,
                limit,
                &vec![],
            );
            for v in iter {
                // just for test, as we have duplicated edges in partitions
                if !eids.contains(&v.get_edge_id()) {
                    eids.push(v.get_edge_id());
                    res.push(v);
                }
            }
        }
        res.into_iter()
    }

    fn count_all_vertices(&self, _: i64, _: &Vec<u32>, _: Option<&Condition>, _: &Vec<u32>) -> u64 {
        unimplemented!()
    }

    fn count_all_edges(&self, _: i64, _: &Vec<u32>, _: Option<&Condition>, _: &Vec<u32>) -> u64 {
        unimplemented!()
    }

    fn translate_vertex_id(&self, _: i64) -> i64 {
        unimplemented!()
    }

    fn get_schema(&self, _si: i64) -> Option<Arc<dyn Schema>> {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct MockSchema {
    prop_name_mapping: HashMap<String, PropId>,
}

impl MockSchema {
    pub fn new(prop_name_mapping: HashMap<String, PropId>) -> Self {
        MockSchema { prop_name_mapping }
    }
}

impl Schema for MockSchema {
    fn get_prop_id(&self, name: &str) -> Option<u32> {
        if let Some(propid) = self.prop_name_mapping.get(name) {
            Some(*propid)
        } else {
            None
        }
    }

    fn get_prop_type(&self, _label: u32, _prop_id: u32) -> Option<DataType> {
        unimplemented!()
    }

    fn get_prop_name(&self, prop_id: u32) -> Option<String> {
        for (propname, propid) in self.prop_name_mapping.iter() {
            if prop_id == *propid {
                return Some(propname.clone());
            }
        }
        return None;
    }

    fn get_label_id(&self, _name: &str) -> Option<u32> {
        unimplemented!()
    }

    fn get_label_name(&self, _label: u32) -> Option<String> {
        unimplemented!()
    }

    fn to_proto(&self) -> Vec<u8> {
        unimplemented!()
    }
}

// for test case
fn to_global_id(id: usize) -> usize {
    match id {
        1 | 2 | 4 | 6 => id,
        3 => 72057594037927939,
        5 => 72057594037927941,
        _ => unreachable!(),
    }
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
    let mut p1 = LocalStoreVertex::new(to_global_id(1) as i64, 0);
    p1.add_property(1, Property::Long(1));
    p1.add_property(2, Property::String("marko".to_string()));
    p1.add_property(3, Property::Int(29));
    partition1.insert_vertex(p1.clone());

    let mut p2 = LocalStoreVertex::new(to_global_id(2) as i64, 0);
    p2.add_property(1, Property::Long(2));
    p2.add_property(2, Property::String("vadas".to_string()));
    p2.add_property(3, Property::Int(27));
    partition0.insert_vertex(p2.clone());

    let mut p4 = LocalStoreVertex::new(to_global_id(4) as i64, 0);
    p4.add_property(1, Property::Long(4));
    p4.add_property(2, Property::String("josh".to_string()));
    p4.add_property(3, Property::Int(32));
    partition0.insert_vertex(p4.clone());

    let mut p6 = LocalStoreVertex::new(to_global_id(6) as i64, 0);
    p6.add_property(1, Property::Long(6));
    p6.add_property(2, Property::String("peter".to_string()));
    p6.add_property(3, Property::Int(35));
    partition0.insert_vertex(p6.clone());

    // software data:
    //    id,name,lang
    //    3,lop,java
    //    5,ripple,java
    let mut s3 = LocalStoreVertex::new(to_global_id(3) as i64, 1);
    s3.add_property(1, Property::Long(3));
    s3.add_property(2, Property::String("lop".to_string()));
    s3.add_property(4, Property::String("java".to_string()));
    partition1.insert_vertex(s3.clone());

    let mut s5 = LocalStoreVertex::new(to_global_id(5) as i64, 1);
    s5.add_property(1, Property::Long(5));
    s5.add_property(2, Property::String("ripple".to_string()));
    s5.add_property(4, Property::String("java".to_string()));
    partition1.insert_vertex(s5.clone());

    // knows data:
    //    id,srcId,dstId,weight
    //    7,1,2,0.5
    //    8,1,4,1.0
    let mut k7 = LocalStoreEdge::new(p1.clone(), p2.clone(), 0, 7);
    k7.add_property(5, Property::Double(0.5));
    partition1.insert_edge(k7.clone());
    // the reverse edge, for test
    partition0.insert_edge(k7);

    let mut k8 = LocalStoreEdge::new(p1.clone(), p4.clone(), 0, 8);
    k8.add_property(5, Property::Double(1.0));
    partition1.insert_edge(k8.clone());
    // the reverse edge, for test
    partition0.insert_edge(k8);

    // created data:
    //    id,srcId,dstId,weight
    //    9,1,3,0.4
    //    10,4,5,1.0
    //    11,4,3,0.4
    //    12,6,3,0.2
    let mut c9 = LocalStoreEdge::new(p1.clone(), s3.clone(), 1, 9);
    c9.add_property(5, Property::Double(0.4));
    partition1.insert_edge(c9);

    let mut c10 = LocalStoreEdge::new(p4.clone(), s5.clone(), 1, 10);
    c10.add_property(5, Property::Double(1.0));
    partition0.insert_edge(c10.clone());
    // the reverse edge, for test
    partition1.insert_edge(c10);

    let mut c11 = LocalStoreEdge::new(p4.clone(), s3.clone(), 1, 11);
    c11.add_property(5, Property::Double(0.4));
    partition0.insert_edge(c11.clone());
    // the reverse edge, for test
    partition1.insert_edge(c11);

    let mut c12 = LocalStoreEdge::new(p6.clone(), s3.clone(), 1, 12);
    c12.add_property(5, Property::Double(0.2));
    partition0.insert_edge(c12.clone());
    // the reverse edge, for test
    partition1.insert_edge(c12);

    // build schema
    let mut prop_name_mapping = HashMap::new();
    prop_name_mapping.insert("id".to_string(), 1 as PropId);
    prop_name_mapping.insert("name".to_string(), 2 as PropId);
    prop_name_mapping.insert("age".to_string(), 3 as PropId);
    prop_name_mapping.insert("lang".to_string(), 4 as PropId);
    prop_name_mapping.insert("weight".to_string(), 5 as PropId);
    let schema = MockSchema::new(prop_name_mapping);

    return MockGraph::build_store(
        vec![Arc::new(partition0), Arc::new(partition1)],
        Some(Arc::new(schema)),
    );
}

impl GraphPartitionManager for MockGraph {
    fn get_partition_id(&self, vid: VertexId) -> i32 {
        let partition_count = self.partition_list.len();
        floor_mod(vid, partition_count as VertexId) as i32
    }

    fn get_process_partition_list(&self) -> Vec<PartitionId> {
        self.partition_list
            .keys()
            .into_iter()
            .map(|x| *x as PartitionId)
            .collect::<Vec<PartitionId>>()
    }

    fn get_vertex_id_by_primary_key(
        &self,
        _label_id: LabelId,
        _key: &String,
    ) -> Option<(PartitionId, VertexId)> {
        None
    }
}

fn floor_div(x: i64, y: i64) -> i64 {
    let mut r = x / y;
    // if the signs are different and modulo not zero, round down
    if (x ^ y) < 0 && (r * y != x) {
        r = r - 1;
    }
    r
}

pub fn floor_mod(x: i64, y: i64) -> i64 {
    x - floor_div(x, y) * y
}
