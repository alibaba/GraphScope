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

use ahash::{HashMap, HashMapExt};
// use indexmap::map::IndexMap;
use itertools::Itertools;
use petgraph::graph::IndexType;
use petgraph::prelude::{DiGraph, Direction, EdgeIndex, NodeIndex};

use crate::common::{Label, LabelId, INVALID_LABEL_ID};
use crate::graph_db::labeled_topo::{LabeledTopology, MutLabeledTopology};
use crate::sorted_map::SortedMap;
use crate::utils::{Iter, IterList};

struct MutEdgeVec<I: IndexType> {
    offsets: Vec<RangeByLabel<LabelId, I>>,
    offsets_no_label: Vec<usize>,
    edges: Vec<EdgeIndex<I>>,
}

impl<I: IndexType> MutEdgeVec<I> {
    pub fn new(num_nodes: usize, num_edges: usize) -> Self {
        let offsets = vec![RangeByLabel::default(); num_nodes + 1];
        let offsets_no_label = vec![0; num_nodes + 1];
        let edges = Vec::with_capacity(num_edges);

        Self { offsets, offsets_no_label, edges }
    }

    pub fn append_node_adj(&mut self, node: usize, adj: &mut HashMap<LabelId, Vec<EdgeIndex<I>>>) {
        let mut num_edges = 0;
        // will be sorted via label
        for label in adj.keys().cloned().sorted() {
            if let Some(vec) = adj.get_mut(&label) {
                vec.sort();
                self.offsets[node]
                    .inner
                    .insert(label, (I::new(num_edges), I::new(vec.len())));
                num_edges += vec.len();
                self.edges.extend(vec.drain(..));
            }
        }
        self.offsets_no_label[node + 1] = num_edges;
    }
}

impl<I: IndexType> From<MutEdgeVec<I>> for EdgeVec<I> {
    fn from(adj: MutEdgeVec<I>) -> Self {
        let MutEdgeVec { mut offsets, mut offsets_no_label, edges } = adj;
        for node in 1..offsets_no_label.len() {
            offsets_no_label[node] += offsets_no_label[node - 1];
        }
        for (node, offset) in offsets_no_label.drain(..).enumerate() {
            offsets[node].update_by_offset(offset);
        }

        let mut edge_vec = EdgeVec {
            offsets: offsets
                .into_iter()
                .map(|x| x.into_immutable())
                .collect(),
            edges,
        };
        edge_vec.shrink_to_fit();

        edge_vec
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct RangeByLabel<K: IndexType, V: IndexType, C: AsRef<[(K, (V, V))]> = Vec<(K, (V, V))>> {
    /// K -> the key refers to label
    /// (V, V) -> the first element refers to the starting index in the sparse row,
    ///        -> the second element refers to the size of the elements regarding the given `K`
    inner: SortedMap<K, (V, V), C>,
}

impl<K: IndexType, V: IndexType, C: AsRef<[(K, (V, V))]>> RangeByLabel<K, V, C> {
    #[inline]
    pub fn get_index(&self) -> Option<usize> {
        self.inner
            .get_entry_at(0)
            .map(|(_, r)| r.0.index())
    }

    #[inline]
    pub fn get_range(&self, label: K) -> Option<(usize, usize)> {
        self.inner
            .get(&label)
            .map(|range| (range.0.index(), range.0.index() + range.1.index()))
    }
}

impl<K: IndexType, V: IndexType> RangeByLabel<K, V> {
    #[inline]
    pub fn update_by_offset(&mut self, offset: usize) {
        if self.inner.is_empty() {
            self.inner
                .insert(K::new(INVALID_LABEL_ID as usize), (V::new(offset), V::default()));
        } else {
            for (_, range) in self.inner.iter_mut() {
                range.0 = V::new(range.0.index() + offset);
            }
        }
    }

    pub fn into_immutable(self) -> RangeByLabel<K, V, Box<[(K, (V, V))]>> {
        RangeByLabel { inner: self.inner.into_immutable() }
    }
}
/// This is an extension of a typical CSR structure to support label indexing.
#[derive(Default, Clone, Serialize, Deserialize)]
struct EdgeVec<I: IndexType> {
    /// * `offsets[i]`: maintain the adjacent edges of the node of id i,
    /// * `offsets[i][j]` maintains the start and end indices of the adjacent edges of
    /// the label j for node i, if node i has connection to the edge of label j
    offsets: Vec<RangeByLabel<LabelId, I, Box<[(LabelId, (I, I))]>>>,
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
                if let Some(start) = self.offsets[node.index()].get_index() {
                    let end = self.offsets[node.index() + 1]
                        .get_index()
                        .unwrap_or(start);
                    &self.edges[start..end]
                } else {
                    &[]
                }
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

#[derive(Default, Clone, Serialize, Deserialize)]
/// To maintain edges of both directions in a directed graph
struct BiDirEdges<I: IndexType> {
    incoming: EdgeVec<I>,
    outgoing: EdgeVec<I>,
}

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
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
    inner: DiGraph<Label, LabelId, I>,
}

impl<I: IndexType + Send + Sync> MutLabeledTopology for MutTopo<I> {
    type I = I;
    type T = CsrTopo<I>;

    fn current_nodes_count(&self) -> usize {
        self.inner.node_count()
    }

    fn current_edges_count(&self) -> usize {
        self.inner.edge_count()
    }

    fn get_node_label_mut(&mut self, node: NodeIndex<I>) -> Option<&mut Label> {
        self.inner.node_weight_mut(node)
    }

    #[inline]
    fn add_node(&mut self, label: Label) -> NodeIndex<I> {
        self.inner.add_node(label)
    }

    fn add_edge(
        &mut self, start_node_id: NodeIndex<I>, end_node_id: NodeIndex<I>, edge_label: LabelId,
    ) -> EdgeIndex<I> {
        self.inner
            .add_edge(start_node_id, end_node_id, edge_label)
    }

    fn into_immutable(self) -> Self::T {
        self.into()
    }
}

impl<I: IndexType> From<MutTopo<I>> for CsrTopo<I> {
    fn from(mut_topo: MutTopo<I>) -> Self {
        let (raw_nodes, raw_edges) = mut_topo.inner.into_nodes_edges();
        let mut mut_in_edges = MutEdgeVec::new(raw_nodes.len(), 0);
        let mut mut_out_edges = MutEdgeVec::new(raw_nodes.len(), 0);
        let mut in_adj_edges = HashMap::<LabelId, Vec<EdgeIndex<I>>>::new();
        let mut out_adj_edges = HashMap::<LabelId, Vec<EdgeIndex<I>>>::new();
        let mut nodes = vec![Label::default(); raw_nodes.len()];
        for (index, node) in raw_nodes.into_iter().enumerate() {
            nodes[index] = node.weight.clone();
            out_adj_edges.clear();
            let mut edge_index = node.next_edge(Direction::Outgoing);
            while edge_index != EdgeIndex::end() {
                let edge = &raw_edges[edge_index.index()];
                out_adj_edges
                    .entry(edge.weight)
                    .or_default()
                    .push(edge_index);
                edge_index = edge.next_edge(Direction::Outgoing);
            }
            mut_out_edges.append_node_adj(index, &mut out_adj_edges);

            in_adj_edges.clear();
            edge_index = node.next_edge(Direction::Incoming);
            while edge_index != EdgeIndex::end() {
                let edge = &raw_edges[edge_index.index()];
                in_adj_edges
                    .entry(edge.weight)
                    .or_default()
                    .push(edge_index);
                edge_index = edge.next_edge(Direction::Incoming);
            }
            mut_in_edges.append_node_adj(index, &mut in_adj_edges);
        }

        let mut edges = Vec::new();
        for edge in raw_edges {
            edges.push(EdgeTuple::new(edge.source(), edge.target(), edge.weight));
        }

        CsrTopo {
            nodes,
            edges,
            csr: BiDirEdges { incoming: mut_in_edges.into(), outgoing: mut_out_edges.into() },
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
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

    #[inline]
    fn shrink_to_fit(&mut self) {
        self.nodes.shrink_to_fit();
        self.edges.shrink_to_fit();
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

        let mut mut_edges = MutEdgeVec::<u32>::new(4, 6);
        let mut adj = HashMap::new();
        adj.insert(0, vec![e0]);
        adj.insert(1, vec![e2]);
        mut_edges.append_node_adj(0, &mut adj);

        adj.clear();
        adj.insert(1, vec![e1]);
        adj.insert(2, vec![e3]);
        mut_edges.append_node_adj(1, &mut adj);

        adj.clear();
        adj.insert(2, vec![e4]);
        mut_edges.append_node_adj(2, &mut adj);

        adj.clear();
        adj.insert(1, vec![e5]);
        mut_edges.append_node_adj(3, &mut adj);

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
