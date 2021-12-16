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

use crate::error::{FnExecError, FnGenResult};
use crate::graph::element::{GraphElement, VertexOrEdge};
use crate::graph::QueryParams;
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Entry, Record};
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FilterMapFunction, FnResult};
use std::convert::TryInto;
use std::sync::Arc;

/// An Auxilia operator to get extra information for the given entity.
/// Specifically, we will replace the old entity with the new one with details,
/// and set rename the entity, if `alias` has been set.
#[derive(Debug)]
struct AuxiliaOperator {
    tag: Option<NameOrId>,
    query_params: QueryParams,
    alias: Option<NameOrId>,
}

impl FilterMapFunction<Record, Record> for AuxiliaOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error("get tag failed in GetVertexOperator"))?
            .clone();
        // Make sure there is anything to query with
        if self.query_params.is_queryable() {
            let vertex_or_edge_opt = entry.as_graph_element();
            // If queryable, then turn into graph element and do the query
            if let Some(vertex_or_edge) = vertex_or_edge_opt {
                let graph = crate::get_graph().ok_or(FnExecError::NullGraphError)?;
                let new_entry: Option<Entry> = match vertex_or_edge {
                    VertexOrEdge::V(v) => {
                        let mut result_iter = graph.get_vertex(&[v.id()], &self.query_params)?;
                        result_iter.next().map(|vertex| vertex.into())
                    }
                    VertexOrEdge::E(e) => {
                        let mut result_iter = graph.get_edge(&[e.id()], &self.query_params)?;
                        result_iter.next().map(|edge| edge.into())
                    }
                };
                if new_entry.is_some() {
                    let arc_entry = Arc::new(new_entry.unwrap());
                    input.append_arc_entry(arc_entry.clone(), self.tag.clone());
                    if self.alias.is_some() {
                        input.append_arc_entry(arc_entry, self.alias.clone());
                    }
                } else {
                    return Ok(None);
                }
            }
        } else {
            if self.alias.is_some() {
                input.append_arc_entry(entry.clone(), self.alias.clone());
            }
        }

        Ok(Some(input))
    }
}

impl FilterMapFuncGen for algebra_pb::Auxilia {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let start_tag = self
            .tag
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let query_params = self.params.try_into()?;
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let auxilia_operator = AuxiliaOperator { tag: start_tag, query_params, alias };
        debug!("Runtime auxilia operator: {:?}", auxilia_operator);
        Ok(Box::new(auxilia_operator))
    }
}
