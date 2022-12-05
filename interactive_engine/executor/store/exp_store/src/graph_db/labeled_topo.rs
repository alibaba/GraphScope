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

use ahash::HashMap;
use petgraph::graph::{DiGraph, IndexType};
use petgraph::prelude::{Direction, EdgeIndex, EdgeRef, NodeIndex};
use serde::{Deserialize, Serialize};

use crate::common::{Label, LabelId};
use crate::utils::{Iter, IterList};

pub trait LabeledTopology {
    type I: IndexType + Send + Sync;

    fn nodes_count(&self) -> usize;

    fn edges_count(&self) -> usize;

    fn in_degree(&self, node: NodeIndex<Self::I>) -> usize;

    fn out_degree(&self, node: NodeIndex<Self::I>) -> usize;

    fn degree(&self, node: NodeIndex<Self::I>) -> usize {
        self.in_degree(node) + self.out_degree(node)
    }

    fn get_node_label(&self, node: NodeIndex<Self::I>) -> Option<Label>;

    fn get_edge_label(&self, edge: EdgeIndex<Self::I>) -> Option<LabelId>;

    fn get_edge_end_points(
        &self, edge: EdgeIndex<Self::I>,
    ) -> Option<(NodeIndex<Self::I>, NodeIndex<Self::I>)>;

    fn get_node_indices(&self) -> Iter<NodeIndex<Self::I>>;

    fn get_edge_indices(&self) -> Iter<EdgeIndex<Self::I>>;

    fn get_adjacent_edges_iter(
        &self, node: NodeIndex<Self::I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<Self::I>>;

    fn get_adjacent_edges_of_labels_iter(
        &self, node: NodeIndex<Self::I>, labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<Self::I>> {
        let mut iters = vec![];
        for label in labels.into_iter().rev() {
            iters.push(self.get_adjacent_edges_iter(node, Some(label), dir));
        }

        Iter::from_iter(IterList::new(iters))
    }

    fn get_adjacent_nodes_iter(
        &self, node: NodeIndex<Self::I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<Self::I>>;

    fn get_adjacent_nodes_of_labels_iter(
        &self, node: NodeIndex<Self::I>, labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<Self::I>>;

    fn shrink_to_fit(&mut self);
}

pub trait MutLabeledTopology {
    type I: IndexType + Send + Sync;
    type T: LabeledTopology<I = Self::I>;

    fn current_nodes_count(&self) -> usize;
    fn current_edges_count(&self) -> usize;
    fn get_node_label_mut(&mut self, node: NodeIndex<Self::I>) -> Option<&mut Label>;
    fn add_node(&mut self, label: Label) -> NodeIndex<Self::I>;
    fn add_edge(
        &mut self, start_node: NodeIndex<Self::I>, end_node: NodeIndex<Self::I>, label: LabelId,
    ) -> EdgeIndex<Self::I>;
    fn into_immutable(self) -> Self::T;
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct PGWrapper<I: IndexType> {
    inner: DiGraph<Label, LabelId, I>,
    /// Grouping the adjacent edges of a vertex by the labels
    /// tuple 0 -> outgoing, tuple 1 -> incoming
    adjacent_label_indices: HashMap<NodeIndex<I>, HashMap<LabelId, (Vec<EdgeIndex<I>>, Vec<EdgeIndex<I>>)>>,
}

impl<I: IndexType + Send + Sync> LabeledTopology for PGWrapper<I> {
    type I = I;
    #[inline]
    fn nodes_count(&self) -> usize {
        self.inner.node_count()
    }

    #[inline]
    fn edges_count(&self) -> usize {
        self.inner.edge_count()
    }

    #[inline]
    fn in_degree(&self, node: NodeIndex<I>) -> usize {
        if let Some(label_indices) = self.adjacent_label_indices.get(&node) {
            let mut degree = 0;
            for (_, (_, in_neighbors)) in label_indices {
                degree += in_neighbors.len();
            }
            degree
        } else {
            self.inner
                .neighbors_directed(node, Direction::Incoming)
                .count()
        }
    }

    #[inline]
    fn out_degree(&self, node: NodeIndex<I>) -> usize {
        if let Some(label_indices) = self.adjacent_label_indices.get(&node) {
            let mut degree = 0;
            for (_, (out_neighbors, _)) in label_indices {
                degree += out_neighbors.len();
            }
            degree
        } else {
            self.inner
                .neighbors_directed(node, Direction::Outgoing)
                .count()
        }
    }

    #[inline]
    fn degree(&self, node: NodeIndex<I>) -> usize {
        if let Some(label_indices) = self.adjacent_label_indices.get(&node) {
            let mut degree = 0;
            for (_, (out_neighbors, in_neighbors)) in label_indices {
                degree += out_neighbors.len();
                degree += in_neighbors.len();
            }
            degree
        } else {
            self.inner.neighbors(node).count()
        }
    }

    #[inline]
    fn get_node_label(&self, node: NodeIndex<I>) -> Option<Label> {
        self.inner.node_weight(node).cloned()
    }

    #[inline]
    fn get_edge_label(&self, edge: EdgeIndex<I>) -> Option<LabelId> {
        self.inner.edge_weight(edge).cloned()
    }

    #[inline]
    fn get_edge_end_points(&self, edge: EdgeIndex<I>) -> Option<(NodeIndex<I>, NodeIndex<I>)> {
        self.inner.edge_endpoints(edge)
    }

    #[inline]
    fn get_node_indices(&self) -> Iter<NodeIndex<I>> {
        Iter::from_iter(self.inner.node_indices())
    }

    #[inline]
    fn get_edge_indices(&self) -> Iter<EdgeIndex<I>> {
        Iter::from_iter(self.inner.edge_indices())
    }

    fn get_adjacent_edges_iter(
        &self, node: NodeIndex<I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<I>> {
        if let Some(l) = label {
            if self.adjacent_label_indices.contains_key(&node) {
                if let Some(label_indices) = self
                    .adjacent_label_indices
                    .get(&node)
                    .unwrap()
                    .get(&l)
                {
                    match dir {
                        Direction::Outgoing => Iter::from_iter(label_indices.0.iter().cloned()),
                        Direction::Incoming => Iter::from_iter(label_indices.1.iter().cloned()),
                    }
                } else {
                    Iter::default()
                }
            } else {
                Iter::from_iter(
                    self.inner
                        .edges_directed(node, dir)
                        .filter_map(move |e| if *e.weight() == l { Some(e.id()) } else { None }),
                )
            }
        } else {
            Iter::from_iter(
                self.inner
                    .edges_directed(node, dir)
                    .map(|e| e.id()),
            )
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
        self.inner.shrink_to_fit();
        self.adjacent_label_indices.shrink_to_fit();
    }
}

impl<I: IndexType + Send + Sync> MutLabeledTopology for PGWrapper<I> {
    type I = I;
    type T = PGWrapper<I>;

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
        &mut self, start_node: NodeIndex<I>, end_node: NodeIndex<I>, label: LabelId,
    ) -> EdgeIndex<I> {
        let edge_id = self.inner.add_edge(start_node, end_node, label);
        self.adjacent_label_indices
            .entry(start_node)
            .or_default()
            .entry(label)
            .or_default()
            .0
            .push(edge_id);

        self.adjacent_label_indices
            .entry(end_node)
            .or_default()
            .entry(label)
            .or_default()
            .1
            .push(edge_id);

        edge_id
    }

    fn into_immutable(self) -> Self::T {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pgwrapper() {
        let mut topo = PGWrapper::<u32>::default();
        let n0 = topo.add_node([0, 0]);
        let n1 = topo.add_node([1, 0]);
        let n2 = topo.add_node([2, 0]);
        let n3 = topo.add_node([3, 0]);

        let e0 = topo.add_edge(n0, n1, 0);
        let e1 = topo.add_edge(n1, n0, 1);
        let e2 = topo.add_edge(n0, n2, 0);
        let e3 = topo.add_edge(n1, n2, 1);
        let e4 = topo.add_edge(n2, n3, 2);
        let e5 = topo.add_edge(n3, n1, 1);

        assert_eq!(topo.get_edge_end_points(e0), Some((n0, n1)));
        assert_eq!(topo.get_edge_end_points(e1), Some((n1, n0)));
        assert_eq!(topo.get_edge_end_points(e2), Some((n0, n2)));
        assert_eq!(topo.get_edge_end_points(e3), Some((n1, n2)));
        assert_eq!(topo.get_edge_end_points(e4), Some((n2, n3)));
        assert_eq!(topo.get_edge_end_points(e5), Some((n3, n1)));

        assert_eq!(
            topo.get_adjacent_edges_iter(n0, Some(0), Direction::Outgoing)
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e0, e2]
        );
        assert_eq!(
            topo.get_adjacent_edges_iter(n0, Some(1), Direction::Incoming)
                .collect::<Vec<EdgeIndex<u32>>>(),
            vec![e1]
        );
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
