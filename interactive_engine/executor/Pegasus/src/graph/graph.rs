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

use std::io::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::collections::HashMap;

#[derive(Debug, Clone, Abomonation, Serialize, Deserialize)]
pub struct VertexId {
    label: i32,
    id: i64,
}

impl  VertexId {
    pub fn new(label: i32, id: i64) -> Self {
         VertexId { label, id }
    }

    pub fn get_id(&self) -> i64 {
        self.id
    }

    pub fn get_label(&self) -> i32 {
        self.label
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeId {
    label: i32,
    id: i64,
}

impl EdgeId {
    pub fn new(label: i32, id: i64) -> Self {
        EdgeId { label, id }
    }

    pub fn get_id(&self) -> i64 {
        self.id
    }

    pub fn get_label(&self) -> i32 {
        self.label
    }
}

#[derive(Debug)]
pub struct Vertex {
    id:  VertexId
}

impl Vertex {
    pub fn new(label: i32, id: i64) -> Self {
        Vertex { id:  VertexId::new(label, id) }
    }

    pub fn get_id(&self) -> & VertexId {
        &self.id
    }
}

#[derive(Debug)]
pub struct Edge {
    id: EdgeId,
    src_id: VertexId,
    dst_id: VertexId,
}

impl Edge {
    pub fn new(label: i32, id: i64, src_id: VertexId, dst_id: VertexId) -> Self {
        Edge { id: EdgeId::new(label, id), src_id, dst_id }
    }

    pub fn get_id(&self) -> &EdgeId {
        &self.id
    }

    pub fn get_src_id(&self) -> &VertexId {
        &self.src_id
    }

    pub fn get_dst_id(&self) -> &VertexId {
        &self.dst_id
    }
}

pub struct Graph {
    vertex_list: HashMap<i64, Vertex>,
    edge_out_list: HashMap<i64, Vec<Edge>>,
}

impl Graph {
    pub fn load_local_file(vertex_file: String, edge_file: String) -> Self {
        let mut vertex_list = HashMap::new();
        let mut edge_out_list: HashMap<i64, Vec<Edge>> = HashMap::new();

        let vertex = File::open(vertex_file).unwrap();
        let vertex_fin = BufReader::new(vertex);
        for line in vertex_fin.lines() {
            if let Ok(line) = line {
                if let Ok(vid) = line.parse::<i64>() {
                    vertex_list.insert(vid, Vertex::new(1, vid));
                }
            }
        }

        let edge = File::open(edge_file).unwrap();
        let edge_fin = BufReader::new(edge);
        let mut edge_id: i64 = 1;
        for line in edge_fin.lines() {
            if let Ok(line) = line {
                let mut edge_iter = line.split_whitespace();
                if let Some(src_id) = edge_iter.next() {
                    if let Ok(src_id) = src_id.parse::<i64>() {
                        if let Some(dst_id) = edge_iter.next() {
                            if let Ok(dst_id) = dst_id.parse::<i64>() {
                                let edge_value = Edge::new(2, edge_id, VertexId::new(1, src_id), VertexId::new(1, dst_id));
                                if let Some(edge_list) = edge_out_list.get_mut(&src_id) {
                                    edge_list.push(edge_value);
                                } else {
                                    edge_out_list.insert(src_id, vec![edge_value]);
                                }
                                edge_id += 1;
                            }
                        }
                    }
                }
            }
        }

        Graph { vertex_list, edge_out_list }
    }

    pub fn scan_vertex(&self) -> impl Iterator<Item=&Vertex> {
        debug!("scan vertex: {:?}", self.vertex_list.len());
        self.vertex_list.values()
    }

    pub fn get_out_edge_list(&self, vid: & VertexId) -> Option<impl Iterator<Item=&Edge>> {
        if let Some(edge_list) = self.edge_out_list.get(&vid.id) {
            Some(edge_list.into_iter())
        } else {
            None
        }
    }
}

#[test]
fn test_graph() {
    let graph = Graph::load_local_file("data/source_vertex.csv".to_string(), "data/twitter_rv.net".to_string());
    for vertex in graph.scan_vertex() {
        if let Some(edge_list) = graph.get_out_edge_list(vertex.get_id()) {
            let mut edge_value_list = Vec::new();
            for edge in edge_list {
                edge_value_list.push(edge);
            }
            println!("vertex->{:?} has out edge list->{:?}", vertex, edge_value_list);
        } else {
            println!("vertex->{:?} has no out edges", vertex);
        }
    }
}
