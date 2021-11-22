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
use crate::process::operator::map::MapFuncGen;
use crate::process::operator::KeyedError;
use crate::process::record::Record;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::get_v::VOpt;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};
use std::convert::TryInto;

struct GetVertexOperator {
    start_tag: Option<NameOrId>,
    opt: VOpt,
    query_params: QueryParams,
    alias: Option<NameOrId>,
}

impl MapFunction<Record, Record> for GetVertexOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        let entry = input
            .get(self.start_tag.as_ref())
            .ok_or(FnExecError::GetTagError(KeyedError::from(
                "get tag failed in GetVertexOperator",
            )))?;
        let vertex_or_edge = entry
            .as_graph_element()
            .ok_or(FnExecError::UnExpectedDataType(
                "tag does not refer to a graph element".to_string(),
            ))?;
        let id = match vertex_or_edge {
            VertexOrEdge::V(v) => match self.opt {
                VOpt::This => v.id().ok_or(FnExecError::QueryStoreError(
                    "id of Vertex cannot be None".to_string(),
                ))?,
                _ => Err(FnExecError::UnExpectedDataType(
                    "should be vertex entry".to_string(),
                ))?,
            },
            VertexOrEdge::E(e) => match self.opt {
                VOpt::Start => e.src_id,
                VOpt::End => e.dst_id,
                VOpt::Other => Err(FnExecError::UnSupported(
                    "VOpt ot Other is not supported".to_string(),
                ))?,
                VOpt::This => Err(FnExecError::UnExpectedDataType(
                    "should be edge entry".to_string(),
                ))?,
            },
        };
        let graph = crate::get_graph().ok_or(FnExecError::EmptyGraphError)?;
        let mut result_iter = graph.get_vertex(&[id], &self.query_params)?;
        if let Some(vertex) = result_iter.next() {
            input.append(vertex, self.alias.clone());
            Ok(input)
        } else {
            Err(FnExecError::QueryStoreError(format!(
                "vertex with id {} not found",
                id
            )))?
        }
    }
}

impl MapFuncGen for algebra_pb::GetV {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        let start_tag = self
            .tag
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        let query_params = self.params.try_into()?;
        let alias = self
            .alias
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        Ok(Box::new(GetVertexOperator {
            start_tag,
            opt,
            query_params,
            alias,
        }))
    }
}
