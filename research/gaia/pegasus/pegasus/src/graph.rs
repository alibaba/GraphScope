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

use dot::LabelText::LabelStr;
use dot::{Edges, Id, LabelText, Nodes};
use std::borrow::Cow;

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
    pub scope_depth: usize,
    pub src_peers: usize,
    pub dst_peers: usize,
    pub is_local: bool,
}

/// meaningless
impl Default for Edge {
    fn default() -> Self {
        Edge {
            id: 0,
            source: Default::default(),
            target: Default::default(),
            scope_depth: 0,
            src_peers: 1,
            dst_peers: 1,
            is_local: true,
        }
    }
}

impl ::std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        if self.is_local {
            write!(f, "[{}: {:?} -> {:?}]", self.id, self.source, self.target)
        } else {
            write!(f, "[{}: {:?} => {:?}]", self.id, self.source, self.target)
        }
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct LogicalGraph {
    /// A collection of all edges in the direct cyclic graph; It is ordered by the edge id from 1 to n;
    edges: Vec<Edge>,
    /// A collection of incident edges(id) of every vertex. It is ordered by the vertex id from 0 to n;
    in_e: Vec<Vec<usize>>,
    /// A collection of out edges(id) of every vertex. It is ordered by the vertex id from 0 to n;
    out_e: Vec<Vec<Vec<usize>>>,
}

pub const INVALID_EDGE_ID: usize = !0;

impl LogicalGraph {
    pub fn new(mut edges: Vec<Edge>, vertices: usize) -> Self {
        edges.sort_by(|a, b| a.id.cmp(&b.id));
        let mut in_e = Vec::with_capacity(vertices);
        let mut out_e = Vec::with_capacity(vertices);
        for (index, e) in edges.iter().enumerate() {
            // as edge id is start from 1;
            assert_eq!(index + 1, e.id, "Unexpected edge id {} at {};", e.id, index);
            assert!(
                e.target.index < vertices,
                "Unexpected vertex id {} out of {}",
                e.target.index,
                vertices
            );
            assert!(
                e.source.index < vertices,
                "Unexpected vertex id {} out of {}",
                e.source.index,
                vertices
            );
            while in_e.len() <= e.target.index {
                in_e.push(Vec::new());
            }
            while in_e[e.target.index].len() <= e.target.port {
                in_e[e.target.index].push(INVALID_EDGE_ID);
            }
            in_e[e.target.index][e.target.port] = e.id;

            while out_e.len() <= e.source.index {
                out_e.push(Vec::new());
            }

            while out_e[e.source.index].len() <= e.source.port {
                out_e[e.source.index].push(Vec::new());
            }
            out_e[e.source.index][e.source.port].push(e.id);
        }

        in_e.iter().enumerate().for_each(|(i, edges)| {
            edges.iter().enumerate().for_each(|(j, e)| {
                assert_ne!(*e, INVALID_EDGE_ID, "Invalid in edge id {} at {}.{}", e, i, j)
            })
        });

        out_e.iter().enumerate().for_each(|(i, edges)| {
            edges
                .iter()
                .enumerate()
                .for_each(|(j, e)| assert!(!e.is_empty(), "Invalid output port at {}.{}", i, j))
        });

        LogicalGraph { edges, in_e, out_e }
    }

    #[allow(dead_code)]
    #[inline]
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn get_edge(&self, edge_index: usize) -> Option<&Edge> {
        let offset = edge_index - 1;
        if offset >= self.edges.len() {
            None
        } else {
            Some(&self.edges[offset])
        }
    }

    /// Get incident edges' ids of vertex with id `index`;
    #[allow(dead_code)]
    #[inline]
    pub fn get_in_edges(&self, index: usize) -> Option<&[usize]> {
        self.in_e.get(index).map(|e| e.as_slice())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn get_out_edges(&self, index: usize) -> Option<&[Vec<usize>]> {
        self.out_e.get(index).map(|e| e.as_slice())
    }

    #[allow(dead_code)]
    pub fn vertex_size(&self) -> usize {
        self.in_e.len()
    }
}

/// Create a *.dot file which contains the dataflow logical plan;
/// Open the dot file with graphviz; There is online graphviz at :
/// https://dreampuf.github.io/GraphvizOnline/
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
        match dot::Id::new(format!("{}_{}", x, n.0)) {
            Err(_) => {
                warn!("create dot graph failure, maybe invalid job name;");
                dot::Id::new(format!("unknown_{}", n.0)).unwrap()
            }
            Ok(r) => r,
        }
    }

    fn edge_label(&'a self, e: &Edge) -> LabelText<'a> {
        let label = if e.is_local {
            format!("{}_{}_{}", e.source.port, e.id, e.target.port)
        } else {
            format!("{}_{}R_{}", e.source.port, e.id, e.target.port)
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
