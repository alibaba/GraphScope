//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::convert::TryInto;
use std::sync::Arc;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::graph::element::{Element, GraphElement, GraphObject};
use crate::graph::{Direction, Statement, ID};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Entry, Record, RecordElement};

/// An ExpandOrIntersect operator to expand neighbors
/// and intersect with the ones of the same tag found previously (if exists).
/// Notice that start_v_tag (from which tag to expand from)
/// and edge_or_end_v_tag (the alias of expanded neighbors) must be specified.
struct ExpandOrIntersect<E: Into<GraphObject>> {
    start_v_tag: KeyId,
    edge_or_end_v_tag: KeyId,
    stmt: Box<dyn Statement<ID, E>>,
}

fn do_intersection<E: Into<GraphObject> + 'static, Iter: Iterator<Item = E>>(
    prober: &mut Vec<RecordElement>, seeker: Iter,
) {
    let mut s = bit_set::BitSet::with_capacity(prober.len());
    for item in seeker {
        let graph_obj = item.into();
        if let Ok(idx) = prober
            // Notice that binary_search will return any one when multiple records exist
            .binary_search_by(|e| {
                e.as_graph_element()
                    .unwrap() // TODO(unwrap safe?)
                    .id()
                    .cmp(&graph_obj.id())
            })
        {
            s.insert(idx);
        }
    }
    let mut idx = 0;
    for i in s.iter() {
        prober.swap(idx, i);
        idx += 1;
    }
    prober.drain(idx..);
}

impl<E: Into<GraphObject> + 'static> FilterMapFunction<Record, Record> for ExpandOrIntersect<E> {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(Some(&self.start_v_tag))
            .ok_or(FnExecError::get_tag_error(&format!(
                "start_v_tag {:?} in ExpandOrIntersect",
                self.start_v_tag
            )))?;
        if let Some(v) = entry.as_graph_vertex() {
            let id = v.id();
            let iter = self.stmt.exec(id)?;
            if let Some(pre_entry) = input.get_column_mut(&self.edge_or_end_v_tag) {
                // the case of expansion and intersection
                match pre_entry {
                    Entry::Element(e) => Err(FnExecError::unexpected_data_error(&format!(
                        "entry {:?} is not a collection in ExpandOrIntersect",
                        e
                    )))?,
                    Entry::Collection(pre_collection) => {
                        do_intersection(pre_collection, iter);
                        if pre_collection.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(input))
                        }
                    }
                }
            } else {
                // the case of expansion only
                let mut neighbors_collection: Vec<RecordElement> = iter
                    .map(|e| RecordElement::OnGraph(e.into()))
                    .collect();
                neighbors_collection.sort_by(|r1, r2| {
                    r1.as_graph_element()
                        .unwrap()
                        .id()
                        .cmp(&r2.as_graph_element().unwrap().id())
                });
                if neighbors_collection.is_empty() {
                    Ok(None)
                } else {
                    // append columns without changing head
                    let columns = input.get_columns_mut();
                    columns.insert(self.edge_or_end_v_tag as usize, Arc::new(neighbors_collection.into()));
                    Ok(Some(input))
                }
            }
        } else {
            Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.edge_or_end_v_tag
            )))?
        }
    }
}

impl FilterMapFuncGen for algebra_pb::EdgeExpand {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let graph = crate::get_graph().ok_or(FnGenError::NullGraphError)?;
        let start_v_tag = self
            .v_tag
            .ok_or(ParsePbError::from("`EdgeExpand::v_tag` cannot be empty for intersection"))?
            .try_into()?;
        let edge_or_end_v_tag = self
            .alias
            .ok_or(ParsePbError::from("`EdgeExpand::alias` cannot be empty for intersection"))?
            .try_into()?;
        let direction_pb: algebra_pb::edge_expand::Direction =
            unsafe { ::std::mem::transmute(self.direction) };
        let direction = Direction::from(direction_pb);
        let query_params = self.params.try_into()?;
        debug!(
            "Runtime expand collection operator of edge with start_v_tag {:?}, edge_tag {:?}, direction {:?}, query_params {:?}",
            start_v_tag, edge_or_end_v_tag, direction, query_params
        );
        if self.is_edge {
            let stmt = graph.prepare_explore_edge(direction, &query_params)?;
            let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
            Ok(Box::new(edge_expand_operator))
        } else {
            let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
            let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
            Ok(Box::new(edge_expand_operator))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{do_intersection, RecordElement};
    use crate::graph::element::Element;
    use crate::graph::ID;

    #[test]
    fn intersect_test_01() {
        let mut pre_collection = vec![1, 2, 3]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![5, 4, 3, 2, 1];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_02() {
        let mut pre_collection = vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![3, 2, 1];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_03() {
        let mut pre_collection = vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![9, 7, 5, 3, 1];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 3, 5]
        );
    }

    #[test]
    fn intersect_test_04() {
        let mut pre_collection = vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![9, 8, 7, 6];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![]
        );
    }

    #[test]
    fn intersect_test_05() {
        let mut pre_collection = vec![1, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![1, 2, 3];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_06() {
        let mut pre_collection = vec![1, 2, 3]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![1, 1, 2, 3, 4, 5];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_07() {
        // The duplication will be removed
        let mut pre_collection = vec![1, 1, 2, 2, 3, 3]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![1, 2, 3];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_08() {
        let mut pre_collection = vec![1, 2, 3]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![1, 1, 2, 2, 3, 3];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn intersect_test_09() {
        let mut pre_collection = vec![1, 1, 2, 2, 3, 3]
            .into_iter()
            .map(|id| RecordElement::OnGraph(id.into()))
            .collect();
        let curr_collection = vec![1, 1, 2, 2, 3, 3];
        do_intersection(&mut pre_collection, curr_collection.into_iter());
        assert_eq!(
            pre_collection
                .into_iter()
                .map(|r| r.as_graph_element().unwrap().id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        );
    }
}
