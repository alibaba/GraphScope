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
//!

use std::collections::HashMap;

use indexmap::map::IndexMap;
use itertools::Itertools;
use petgraph::graph::IndexType;
use petgraph::prelude::{Direction, EdgeIndex, NodeIndex};
use vec_map::VecMap;

use crate::common::{Label, LabelId, INVALID_LABEL_ID};
use crate::graph_db::labeled_topo::{LabeledTopology, MutLabeledTopology};
use crate::utils::{Iter, IterList};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeIndex {
    /// The starting index of the range
    start: usize,
    /// The offset
    off: u32,
}

impl RangeIndex {
    pub fn new(start: usize, off: u32) -> Self {
        Self { start, off }
    }
}

#[derive(Clone, Debug, Default)]
struct MutEdgeVec<I: IndexType> {
    /// An adjacent list which maps each node's id (as index)
    /// to its adjacent edges (together with its label)
    edges: VecMap<HashMap<LabelId, Vec<EdgeIndex<I>>>>,
    /// The max node's id that has ever seen
    max_seen_node_id: NodeIndex<I>,
    /// The number of edges ever seen
    edge_count: usize,
}

impl<I: IndexType> From<MutEdgeVec<I>> for EdgeVec<I> {
    fn from(adj: MutEdgeVec<I>) -> Self {
        let mut offsets = vec![RangeByLabel::default(); adj.node_count() + 1];
        let mut offsets_no_label = vec![0; adj.node_count() + 1];
        let mut edges = Vec::with_capacity(adj.edge_count());
        for (node, mut label_vec) in adj.edges.into_iter() {
            let mut num_edges = 0;
            // will be sorted via label
            for label in label_vec.keys().cloned().sorted() {
                for vec in label_vec.get_mut(&label) {
                    vec.sort();
                    offsets[node]
                        .inner
                        .insert(label, RangeIndex::new(num_edges, vec.len() as u32));
                    num_edges += vec.len();
                    edges.extend(vec.drain(..));
                }
            }
            offsets_no_label[node + 1] = num_edges;
        }

        for node in 1..offsets_no_label.len() {
            offsets_no_label[node] += offsets_no_label[node - 1];
        }
        for (node, offset) in offsets_no_label.drain(..).enumerate() {
            if offsets[node].inner.is_empty() {
                offsets[node]
                    .inner
                    .insert(INVALID_LABEL_ID, RangeIndex::new(offset, 0));
            } else {
                for (_, ranges) in offsets[node].inner.iter_mut() {
                    ranges.start += offset;
                }
            }
        }

        let mut edge_vec = EdgeVec { offsets, edges };
        edge_vec.shrink_to_fit();

        edge_vec
    }
}

impl<I: IndexType> MutEdgeVec<I> {
    pub fn add_edge(&mut self, edge: EdgeIndex<I>, label: LabelId, end_node: NodeIndex<I>) {
        self.edges
            .entry(end_node.index())
            .or_insert(HashMap::new())
            .entry(label)
            .or_default()
            .push(edge);
        if end_node > self.max_seen_node_id {
            self.max_seen_node_id = end_node;
        }
        self.edge_count += 1;
    }

    #[inline]
    pub fn node_count(&self) -> usize {
        self.max_seen_node_id.index() + 1
    }

    #[inline]
    pub fn edge_count(&self) -> usize {
        self.edge_count
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct RangeByLabel<I: IndexType> {
    inner: IndexMap<I, RangeIndex>,
}

impl<I: IndexType> RangeByLabel<I> {
    pub fn get_index(&self) -> Option<usize> {
        self.inner.first().map(|(_, r)| r.start)
    }

    pub fn get_range(&self, label: I) -> Option<(usize, usize)> {
        self.inner
            .get(&label)
            .map(|range| (range.start, range.start + range.off as usize))
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct EdgeVec<I: IndexType> {
    /// This is an extension of a typical CSR structure to support label indexing.
    /// * `offsets[i]`: maintain the adjacent edges of the node of id i,
    /// * `offsets[i][j]` maintains the start and end indices of the adjacent edges of
    /// the label j for node i, if node i has connection to the edge of label j
    offsets: Vec<RangeByLabel<LabelId>>,
    /// A vector to maintain edges' id
    edges: Vec<EdgeIndex<I>>,
}

impl<I: IndexType> EdgeVec<I> {
    #[inline]
    pub fn nodes_count(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    pub fn degree(&self, node: NodeIndex<I>) -> usize {
        if self.has_node(node) {
            // must at least have an index
            let start = self.offsets[node.index()]
                .get_index()
                .unwrap_or(0);
            let end = self.offsets[node.index() + 1]
                .get_index()
                .unwrap_or(0);

            end - start
        } else {
            0
        }
    }

    #[inline]
    pub fn has_node(&self, node: NodeIndex<I>) -> bool {
        node.index() < self.nodes_count()
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.edges.shrink_to_fit();
        self.offsets.shrink_to_fit();
    }
}

impl<I: IndexType + Send + Sync> EdgeVec<I> {
    /// Obtain the adjacent edges of certain label of a given node.
    /// If the label is `None`, return all its adjacent edges
    pub fn adjacent_edges(&self, node: NodeIndex<I>, label: Option<LabelId>) -> &[EdgeIndex<I>] {
        if self.has_node(node) {
            if let Some(l) = label {
                if let Some((start, end)) = self.offsets[node.index()].get_range(l) {
                    &self.edges[start..end]
                } else {
                    &[]
                }
            } else {
                let start = self.offsets[node.index()]
                    .get_index()
                    .unwrap_or(0);
                let end = self.offsets[node.index() + 1]
                    .get_index()
                    .unwrap_or(0);
                &self.edges[start..end]
            }
        } else {
            &[]
        }
    }

    #[inline]
    pub fn adjacent_edges_iter(
        &self, node: NodeIndex<I>, label_opt: Option<LabelId>,
    ) -> Iter<EdgeIndex<I>> {
        Iter::from_iter(
            self.adjacent_edges(node, label_opt)
                .iter()
                .cloned(),
        )
    }

    #[inline]
    pub fn adjacent_edges_of_labels_iter(
        &self, node: NodeIndex<I>, labels: Vec<LabelId>,
    ) -> Iter<EdgeIndex<I>> {
        let mut iters = vec![];
        for label in labels.into_iter().rev() {
            iters.push(self.adjacent_edges_iter(node, Some(label)));
        }
        Iter::from_iter(IterList::new(iters))
    }
}

#[derive(Default, Clone)]
struct MutBiDirEdges<I: IndexType> {
    incoming: MutEdgeVec<I>,
    outgoing: MutEdgeVec<I>,
}

impl<I: IndexType> From<MutBiDirEdges<I>> for BiDirEdges<I> {
    fn from(bi_edges: MutBiDirEdges<I>) -> Self {
        Self { incoming: bi_edges.incoming.into(), outgoing: bi_edges.outgoing.into() }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// To maintain edges of both directions in a directed graph
struct BiDirEdges<I: IndexType> {
    incoming: EdgeVec<I>,
    outgoing: EdgeVec<I>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct EdgeTuple<I: IndexType> {
    start_node: NodeIndex<I>,
    end_node: NodeIndex<I>,
    label: LabelId,
}

impl<I: IndexType> EdgeTuple<I> {
    pub fn new(start_node: NodeIndex<I>, end_node: NodeIndex<I>, label: LabelId) -> Self {
        Self { start_node, end_node, label }
    }
}

#[derive(Default, Clone)]
pub struct MutTopo<I: IndexType> {
    /// Record the label of nodes
    nodes: Vec<Label>,
    /// Record the edge's both end nodes' index
    edges: Vec<EdgeTuple<I>>,
    /// Record the adjacent edges/nodes of nodes, per each label
    adj_edges: MutBiDirEdges<I>,
    /// Record the currently seen max node's id
    max_seen_node_id: usize,
    /// Record the currently seen max edge's id
    max_seen_edge_id: usize,
}

impl<I: IndexType + Send + Sync> MutLabeledTopology for MutTopo<I> {
    type I = I;
    type T = CsrTopo<I>;

    fn current_nodes_count(&self) -> usize {
        self.nodes.len()
    }

    fn current_edges_count(&self) -> usize {
        self.edges.len()
    }

    fn get_node_label_mut(&mut self, node: NodeIndex<I>) -> Option<&mut Label> {
        self.nodes.get_mut(node.index())
    }

    #[inline]
    fn add_node(&mut self, label: Label) -> NodeIndex<I> {
        let node_id = NodeIndex::new(self.max_seen_node_id);
        self.nodes.push(label);

        self.max_seen_node_id += 1;
        node_id
    }

    fn add_edge(
        &mut self, start_node_id: NodeIndex<I>, end_node_id: NodeIndex<I>, edge_label: LabelId,
    ) -> EdgeIndex<I> {
        let edge_id = EdgeIndex::new(self.max_seen_edge_id);
        self.edges
            .push(EdgeTuple::new(start_node_id, end_node_id, edge_label));
        self.adj_edges
            .outgoing
            .add_edge(edge_id, edge_label, start_node_id);
        self.adj_edges
            .incoming
            .add_edge(edge_id, edge_label, end_node_id);
        self.max_seen_edge_id += 1;

        edge_id
    }

    fn into_immutable(self) -> Self::T {
        self.into()
    }
}

impl<I: IndexType> From<MutTopo<I>> for CsrTopo<I> {
    fn from(mut mut_topo: MutTopo<I>) -> Self {
        let nodes = mut_topo.nodes.drain(..).collect();
        let edges = mut_topo.edges.drain(..).collect();
        let csr = mut_topo.adj_edges.into();

        Self { nodes, edges, csr }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// Record the topology of a graph in a CSR format
pub struct CsrTopo<I: IndexType> {
    /// To maintain the label of nodes
    nodes: Vec<Label>,
    /// Record the edge's both end nodes' index
    edges: Vec<EdgeTuple<I>>,
    /// To maintain the neighbors (edges and/or nodes) of nodes in a CSR structure,
    /// per each label, in both directions
    csr: BiDirEdges<I>,
}

impl<I: IndexType + Send + Sync> CsrTopo<I> {
    #[inline]
    pub fn get_adjacent_edges(
        &self, node: NodeIndex<I>, edge_label: Option<LabelId>, dir: Direction,
    ) -> &[EdgeIndex<I>] {
        match dir {
            Direction::Outgoing => self
                .csr
                .outgoing
                .adjacent_edges(node, edge_label),
            Direction::Incoming => self
                .csr
                .incoming
                .adjacent_edges(node, edge_label),
        }
    }
}

impl<I: IndexType + Sync + Send> LabeledTopology for CsrTopo<I> {
    type I = I;

    #[inline]
    fn nodes_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    fn edges_count(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    fn in_degree(&self, node: NodeIndex<I>) -> usize {
        self.csr.incoming.degree(node)
    }

    #[inline]
    fn out_degree(&self, node: NodeIndex<I>) -> usize {
        self.csr.outgoing.degree(node)
    }

    #[inline]
    fn get_node_label(&self, node: NodeIndex<I>) -> Option<Label> {
        self.nodes.get(node.index()).cloned()
    }

    #[inline]
    fn get_edge_label(&self, edge: EdgeIndex<I>) -> Option<LabelId> {
        self.edges.get(edge.index()).map(|e| e.label)
    }

    #[inline]
    fn get_edge_end_points(&self, edge: EdgeIndex<I>) -> Option<(NodeIndex<I>, NodeIndex<I>)> {
        self.edges
            .get(edge.index())
            .map(|e| (e.start_node, e.end_node))
    }

    #[inline]
    fn get_node_indices(&self) -> Iter<NodeIndex<I>> {
        Iter::from_iter((0..self.nodes_count()).map(|i| NodeIndex::new(i)))
    }

    #[inline]
    fn get_edge_indices(&self) -> Iter<EdgeIndex<I>> {
        Iter::from_iter((0..self.edges_count()).map(|i| EdgeIndex::new(i)))
    }

    #[inline]
    fn get_adjacent_edges_iter(
        &self, node: NodeIndex<I>, edge_label: Option<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<I>> {
        Iter::from_iter(
            self.get_adjacent_edges(node, edge_label, dir)
                .iter()
                .cloned(),
        )
    }

    #[inline]
    fn get_adjacent_edges_of_labels_iter(
        &self, node: NodeIndex<I>, edge_labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<I>> {
        match dir {
            Direction::Outgoing => self
                .csr
                .outgoing
                .adjacent_edges_of_labels_iter(node, edge_labels),
            Direction::Incoming => self
                .csr
                .incoming
                .adjacent_edges_of_labels_iter(node, edge_labels),
        }
    }

    #[inline]
    fn get_adjacent_nodes_iter(
        &self, node: NodeIndex<I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<I>> {
        Iter::from_iter(
            self.get_adjacent_edges_iter(node, label, dir)
                .map(move |e| {
                    // can safely unwrap due to inner function
                    let (s, t) = self.get_edge_end_points(e).unwrap();
                    match dir {
                        Direction::Outgoing => t,
                        Direction::Incoming => s,
                    }
                }),
        )
    }

    #[inline]
    fn get_adjacent_nodes_of_labels_iter(
        &self, node: NodeIndex<I>, labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<I>> {
        Iter::from_iter(
            self.get_adjacent_edges_of_labels_iter(node, labels, dir)
                .map(move |e| {
                    // can safely unwrap due to inner function
                    let (s, t) = self.get_edge_end_points(e).unwrap();
                    match dir {
                        Direction::Outgoing => t,
                        Direction::Incoming => s,
                    }
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use petgraph::graph::{edge_index, node_index};

    use super::*;

    #[test]
    fn test_edge_vec() {
        let n0 = node_index(0);
        let n1 = node_index(1);
        let n2 = node_index(2);
        let n3 = node_index(3);

        let e0 = edge_index(0);
        let e1 = edge_index(1);
        let e2 = edge_index(2);
        let e3 = edge_index(3);
        let e4 = edge_index(4);
        let e5 = edge_index(5);

        let mut mut_edges = MutEdgeVec::<u32>::default();
        mut_edges.add_edge(e0, 0, n0);
        mut_edges.add_edge(e1, 1, n1);
        mut_edges.add_edge(e2, 1, n0);
        mut_edges.add_edge(e3, 2, n1);
        mut_edges.add_edge(e4, 2, n2);
        mut_edges.add_edge(e5, 1, n3);

        let edges: EdgeVec<u32> = mut_edges.into();

        assert_eq!(edges.adjacent_edges(n0, None), &[e0, e2]);
        assert_eq!(edges.adjacent_edges(n0, Some(0)), &[e0]);
        assert_eq!(edges.adjacent_edges(n0, Some(1)), &[e2]);
        assert_eq!(edges.adjacent_edges(n0, Some(3)), &[]);
        assert_eq!(
            edges
                .adjacent_edges_of_labels_iter(n0, vec![0, 1])
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e0, e2]
        );

        assert_eq!(edges.adjacent_edges(n1, None), &[e1, e3]);
        assert_eq!(edges.adjacent_edges(n1, Some(1)), &[e1]);
        assert_eq!(edges.adjacent_edges(n1, Some(2)), &[e3]);
        assert_eq!(edges.adjacent_edges(n1, Some(3)), &[]);
        assert_eq!(
            edges
                .adjacent_edges_of_labels_iter(n1, vec![1, 2])
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e1, e3]
        );

        assert_eq!(edges.adjacent_edges(n2, None), &[e4]);
        assert_eq!(edges.adjacent_edges(n2, Some(2)), &[e4]);
        assert_eq!(edges.adjacent_edges(n2, Some(1)), &[]);
        assert_eq!(
            edges
                .adjacent_edges_of_labels_iter(n2, vec![2])
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e4]
        );

        assert_eq!(edges.adjacent_edges(n3, None), &[e5]);
        assert_eq!(edges.adjacent_edges(n3, Some(1)), &[e5]);
        assert_eq!(edges.adjacent_edges(n3, Some(2)), &[]);
        assert_eq!(
            edges
                .adjacent_edges_of_labels_iter(n3, vec![1, 2])
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e5]
        );

        assert_eq!(edges.adjacent_edges(node_index(4), None), &[]);
    }

    #[test]
    fn test_csr_topology() {
        let mut mut_topo = MutTopo::<u32>::default();
        let n0 = mut_topo.add_node([0, 0]);
        let n1 = mut_topo.add_node([1, 0]);
        let n2 = mut_topo.add_node([2, 0]);
        let n3 = mut_topo.add_node([3, 0]);

        let e0 = mut_topo.add_edge(n0, n1, 0);
        let e1 = mut_topo.add_edge(n1, n0, 1);
        let e2 = mut_topo.add_edge(n0, n2, 0);
        let e3 = mut_topo.add_edge(n1, n2, 1);
        let e4 = mut_topo.add_edge(n2, n3, 2);
        let e5 = mut_topo.add_edge(n3, n1, 1);

        let topo: CsrTopo<u32> = mut_topo.into();

        assert_eq!(topo.get_node_label(n0), Some([0, 0]));
        assert_eq!(topo.get_node_label(n1), Some([1, 0]));
        assert_eq!(topo.get_node_label(n2), Some([2, 0]));
        assert_eq!(topo.get_node_label(n3), Some([3, 0]));

        assert_eq!(topo.get_edge_end_points(e0), Some((n0, n1)));
        assert_eq!(topo.get_edge_end_points(e1), Some((n1, n0)));
        assert_eq!(topo.get_edge_end_points(e2), Some((n0, n2)));
        assert_eq!(topo.get_edge_end_points(e3), Some((n1, n2)));
        assert_eq!(topo.get_edge_end_points(e4), Some((n2, n3)));
        assert_eq!(topo.get_edge_end_points(e5), Some((n3, n1)));

        assert_eq!(topo.get_adjacent_edges(n0, Some(0), Direction::Outgoing), &[e0, e2]);
        assert_eq!(topo.get_adjacent_edges(n0, Some(1), Direction::Incoming), &[e1]);
        assert_eq!(
            topo.get_adjacent_nodes_iter(n0, Some(0), Direction::Outgoing)
                .collect::<Vec<NodeIndex<u32>>>(),
            vec![n1, n2]
        );
        assert_eq!(
            topo.get_adjacent_nodes_iter(n0, Some(1), Direction::Incoming)
                .collect::<Vec<NodeIndex<u32>>>(),
            vec![n1]
        );
        assert_eq!(
            topo.get_adjacent_nodes_of_labels_iter(n1, vec![0], Direction::Incoming)
                .collect::<Vec<NodeIndex<u32>>>(),
            vec![n0]
        );
        assert_eq!(
            topo.get_adjacent_nodes_of_labels_iter(n1, vec![1], Direction::Incoming)
                .collect::<Vec<NodeIndex<u32>>>(),
            vec![n3]
        );
        assert_eq!(
            topo.get_adjacent_nodes_of_labels_iter(n1, vec![0, 1], Direction::Incoming)
                .collect::<Vec<NodeIndex<u32>>>(),
            vec![n0, n3]
        );
        assert_eq!(
            topo.get_adjacent_edges_of_labels_iter(n1, vec![0, 1], Direction::Incoming)
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e0, e5]
        );
    }
}
