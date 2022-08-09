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

use std::collections::HashMap;

use petgraph::graph::{DiGraph, IndexType};
use petgraph::prelude::{Direction, EdgeIndex, EdgeRef, NodeIndex};

use crate::common::{Label, LabelId};
use crate::utils::{Iter, IterList};

pub trait LabeledTopology<I: IndexType + Send + Sync> {
    // immutable apis
    fn nodes_count(&self) -> usize;

    fn edges_count(&self) -> usize;

    fn get_edge_end_points(&self, edge: EdgeIndex<I>) -> Option<(NodeIndex<I>, NodeIndex<I>)>;

    fn get_adjacent_edges_iter(
        &self, node: NodeIndex<I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<I>>;

    fn get_adjacent_edges_of_labels_iter(
        &self, node: NodeIndex<I>, labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<EdgeIndex<I>> {
        let mut iters = vec![];
        for label in labels.into_iter().rev() {
            iters.push(self.get_adjacent_edges_iter(node, Some(label), dir));
        }

        Iter::from_iter(IterList::new(iters))
    }

    fn get_adjacent_nodes_iter(
        &self, node: NodeIndex<I>, label: Option<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<I>>
    where
        Self: Sync,
    {
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

    fn get_adjacent_nodes_of_labels_iter(
        &self, node: NodeIndex<I>, labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<NodeIndex<I>>
    where
        Self: Sync,
    {
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

pub trait MutLabeledTopology<I: IndexType> {
    fn add_node(&mut self, label: Label) -> NodeIndex<I>;
    fn add_edge(
        &mut self, start_node: NodeIndex<I>, end_node: NodeIndex<I>, label: LabelId,
    ) -> EdgeIndex<I>;
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct PGWrapper<I: IndexType> {
    inner: DiGraph<Label, LabelId, I>,
    /// Grouping the adjacent edges of a vertex by the labels
    /// tuple 0 -> outgoing, tuple 1 -> incoming
    adjacent_label_indices: HashMap<NodeIndex<I>, HashMap<LabelId, (Vec<EdgeIndex<I>>, Vec<EdgeIndex<I>>)>>,
}

impl<I: IndexType + Send + Sync> LabeledTopology<I> for PGWrapper<I> {
    #[inline]
    fn nodes_count(&self) -> usize {
        self.inner.node_count()
    }

    #[inline]
    fn edges_count(&self) -> usize {
        self.inner.edge_count()
    }

    #[inline]
    fn get_edge_end_points(&self, edge: EdgeIndex<I>) -> Option<(NodeIndex<I>, NodeIndex<I>)> {
        self.inner.edge_endpoints(edge)
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
}

impl<I: IndexType> MutLabeledTopology<I> for PGWrapper<I> {
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
}

#[cfg(test)]
mod tests {
    use petgraph::graph::{edge_index, node_index};

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
