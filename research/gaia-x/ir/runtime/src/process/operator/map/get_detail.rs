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

use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::graph::element::{GraphElement, VertexOrEdge};
use crate::graph::QueryParams;
use crate::process::operator::map::MapFuncGen;
use crate::process::record::Record;

/// Get details for the given entity.
/// Specifically, we will replace the old entity with the new one with details.
#[derive(Debug)]
struct GetDetailOperator {
    tag: Option<NameOrId>,
    query_params: QueryParams,
}

impl MapFunction<Record, Record> for GetDetailOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        let entry = input
            .get(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error("get tag failed in GetVertexOperator"))?;
        let vertex_or_edge = entry
            .as_graph_element()
            .ok_or(FnExecError::unexpected_data_error("tag does not refer to a graph element"))?;
        let graph = crate::get_graph().ok_or(FnExecError::NullGraphError)?;
        match vertex_or_edge {
            VertexOrEdge::V(v) => {
                let mut result_iter = graph.get_vertex(&[v.id()], &self.query_params)?;
                if let Some(vertex) = result_iter.next() {
                    input.append(vertex, self.tag.clone());
                    Ok(input)
                } else {
                    Err(FnExecError::query_store_error(&format!(
                        "Get property of vertex with id {} failed",
                        v.id()
                    )))?
                }
            }
            VertexOrEdge::E(e) => {
                let mut result_iter = graph.get_edge(&[e.id()], &self.query_params)?;
                if let Some(edge) = result_iter.next() {
                    input.append(edge, self.tag.clone());
                    Ok(input)
                } else {
                    Err(FnExecError::query_store_error(&format!(
                        "Get property of edge with id {} failed",
                        e.id()
                    )))?
                }
            }
        }
    }
}

impl MapFuncGen for algebra_pb::GetDetails {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        let start_tag = self
            .tag
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let query_params = self.params.try_into()?;
        let get_detail_operator = GetDetailOperator { tag: start_tag, query_params };
        debug!("Runtime get_details operator: {:?}", get_detail_operator);
        Ok(Box::new(get_detail_operator))
    }
}
