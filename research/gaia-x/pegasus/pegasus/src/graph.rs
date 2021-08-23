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

use std::borrow::Cow;

use dot::LabelText::LabelStr;
use dot::{Edges, Id, LabelText, Nodes};

use crate::channel_id::ChannelInfo;

#[derive(Copy, Clone, Hash, Eq, PartialEq, Default)]
pub struct Port {
    pub index: usize,
    pub port: usize,
}

impl Port {
    pub fn new(index: usize, port: usize) -> Self {
        Port { index, port }
    }
}

impl ::std::fmt::Debug for Port {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "({}.{})", self.index, self.port)
    }
}

/// Edge representation in the direct cycle graph;
#[derive(Copy, Clone)]
pub struct Edge {
    pub id: usize,
    pub source: Port,
    pub target: Port,
    pub source_peers: usize,
    pub target_peers: usize,
    pub scope_level: usize,
}

impl Edge {
    pub fn new(ch_info: ChannelInfo) -> Self {
        Edge {
            id: ch_info.id.index as usize,
            source: ch_info.source_port,
            target: ch_info.target_port,
            source_peers: ch_info.source_peers,
            target_peers: ch_info.source_peers,
            scope_level: ch_info.scope_level as usize,
        }
    }
}

/// meaningless
impl Default for Edge {
    fn default() -> Self {
        Edge {
            id: 0,
            source: Default::default(),
            target: Default::default(),
            source_peers: 1,
            target_peers: 1,
            scope_level: 0,
        }
    }
}

impl ::std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        if self.source_peers == 1 {
            write!(f, "[{}: {:?} -> {:?}]", self.id, self.source, self.target)
        } else {
            write!(f, "[{}: {:?} => {:?}]", self.id, self.source, self.target)
        }
    }
}

/// the dependencies between operators in a dataflow task;
#[allow(dead_code)]
#[derive(Default)]
pub struct Dependency {
    /// represents as "[operator_index, [output_port, [(follower_index, pipeline follow or not, target_peers)]]]";
    down: Vec<Vec<Vec<(usize, bool, usize)>>>,
    /// represents as "[operator_index, [input_port, (parent, pipeline follow or not)]]";
    up: Vec<Vec<(usize, bool)>>,
    /// sink operators' indexes;
    sinks: Vec<usize>,
}

impl Dependency {
    pub fn add(&mut self, edge: &Edge) {
        while self.down.len() <= edge.source.index {
            self.down.push(Vec::new());
        }
        let n = &mut self.down[edge.source.index];
        while n.len() <= edge.source.port {
            n.push(Vec::new());
        }
        n[edge.source.port].push((edge.target.index, edge.source_peers == 1, edge.target_peers));
    }

    pub fn set_sinks(&mut self, sinks: Vec<usize>) {
        self.sinks = sinks
    }

    #[allow(dead_code)]
    #[inline]
    pub fn get_children_of(&self, index: usize) -> Option<&[Vec<(usize, bool, usize)>]> {
        self.down.get(index).map(|x| x.as_slice())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn get_parents_of(&self, index: usize) -> Option<&[(usize, bool)]> {
        self.up.get(index).map(|x| x.as_slice())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn get_leaf(&self) -> &[usize] {
        &self.sinks
    }
}

pub struct DotGraph {
    job_name: String,
    job_id: u64,
    operators: Vec<String>,
    edges: Vec<Edge>,
}

impl DotGraph {
    pub fn new(job_name: String, job_id: u64, operators: Vec<String>, edges: Vec<Edge>) -> Self {
        DotGraph { job_name, job_id, operators, edges }
    }
}

type V = (u32, String);

impl<'a> dot::Labeller<'a, V, Edge> for DotGraph {
    fn graph_id(&'a self) -> Id<'a> {
        let name = self.job_name.replace("-", "_");
        match dot::Id::new(name) {
            Err(_) => {
                warn!("create dot graph failure, maybe invalid job name;");
                dot::Id::new(format!("unknown_{}", self.job_id)).unwrap()
            }
            Ok(r) => r,
        }
    }

    fn node_id(&'a self, n: &(u32, String)) -> Id<'a> {
        let x = n.1.replace("-", "_");
        dot::Id::new(format!("{}_{}", x, n.0)).unwrap()
    }

    fn edge_label(&'a self, e: &Edge) -> LabelText<'a> {
        let label = if e.source_peers == 1 {
            format!("{}_{}_{}_{}", e.id, e.source.port, e.target.port, e.scope_level)
        } else if e.target_peers == 1 {
            format!("{}G_{}_{}_{}", e.id, e.source.port, e.target.port, e.scope_level)
        } else {
            format!("{}S_{}_{}_{}", e.id, e.source.port, e.target.port, e.scope_level)
        };

        LabelStr(Cow::Owned(label))
    }
}

impl<'a> dot::GraphWalk<'a, V, Edge> for DotGraph {
    fn nodes(&'a self) -> Nodes<'a, (u32, String)> {
        let mut nodes = Vec::with_capacity(self.operators.len());
        for (i, op) in self.operators.iter().enumerate() {
            nodes.push((i as u32, op.clone()));
        }
        Cow::Owned(nodes)
    }

    fn edges(&'a self) -> Edges<'a, Edge> {
        Cow::Borrowed(&self.edges)
    }

    fn source(&'a self, edge: &Edge) -> (u32, String) {
        let offset = edge.source.index;
        (offset as u32, self.operators[offset].clone())
    }

    fn target(&'a self, edge: &Edge) -> (u32, String) {
        let offset = edge.target.index;
        (offset as u32, self.operators[offset].clone())
    }
}
