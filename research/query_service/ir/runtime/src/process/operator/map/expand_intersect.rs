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

use std::collections::HashSet;
use std::convert::TryInto;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::graph::element::{Element, GraphElement, GraphObject};
use crate::graph::{Direction, Statement, ID};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Record, RecordElement};

/// An ExpandOrIntersect operator to expand neighbors
/// and intersect with the ones of the same tag found previously (if exists).
struct ExpandOrIntersect<E: Into<GraphObject>> {
    start_v_tag: KeyId,
    edge_or_end_v_tag: Option<KeyId>,
    stmt: Box<dyn Statement<ID, E>>,
}

impl<E: Into<GraphObject> + 'static> FilterMapFunction<Record, Record> for ExpandOrIntersect<E> {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(Some(&self.start_v_tag))
            .ok_or(FnExecError::get_tag_error("get start_v failed"))?;
        if let Some(v) = entry.as_graph_vertex() {
            let id = v.id();
            let iter = self.stmt.exec(id)?;
            let mut neighbors_collection = vec![];
            if let Some(pre_entry) = input.take(self.edge_or_end_v_tag.as_ref()) {
                // the case of expansion and intersection
                let neighbors_id_set: HashSet<ID> = iter.map(|e| e.into().id()).collect();
                let pre_collection =
                    pre_entry.as_collection().ok_or(FnExecError::unexpected_data_error(
                        "Alias to intersect does not refer to a collection entry in EdgeExpandIntersectionOperator",
                    ))?;
                // TODO: optimize intersection
                for record_element in pre_collection {
                    let graph_element =
                        record_element.as_graph_element().ok_or(FnExecError::unexpected_data_error(
                            "Should intersect with a collection of graph_element entry in EdgeExpandIntersectionOperator",
                        ))?;
                    if neighbors_id_set.contains(&graph_element.id()) {
                        neighbors_collection.push(record_element.clone());
                    }
                }
            } else {
                // the case of expansion only
                neighbors_collection = iter
                    .map(|e| RecordElement::OnGraph(e.into()))
                    .collect();
            }
            if neighbors_collection.is_empty() {
                Ok(None)
            } else {
                input.append(neighbors_collection, self.edge_or_end_v_tag.clone());
                Ok(Some(input))
            }
        } else if let Some(_graph_path) = entry.as_graph_path() {
            Err(FnExecError::unsupported_error(
                "Have not support to expand and intersect neighbors on a path entry in EdgeExpandIntersectionOperator yet",
            ))?
        } else {
            Err(FnExecError::unexpected_data_error(&format!(
                "Cannot Expand from current entry {:?}",
                entry
            )))?
        }
    }
}

impl FilterMapFuncGen for algebra_pb::EdgeExpand {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let graph = crate::get_graph().ok_or(FnGenError::NullGraphError)?;
        let start_v_tag = self
            .v_tag
            .ok_or(ParsePbError::from("v_tag cannot be empty in edge_expand for intersection"))?
            .try_into()?;
        let edge_or_end_v_tag = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
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
