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

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::graph::element::{GraphElement, VertexOrEdge};
use crate::graph::{Direction, QueryParams, Statement, ID};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::KeyedError;
use crate::process::record::{Record, RecordExpandIter};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};
use std::convert::{TryFrom, TryInto};

pub struct EdgeExpandOperator<E: Into<VertexOrEdge>> {
    start_v_tag: Option<NameOrId>,
    edge_or_end_v_tag: Option<NameOrId>,
    stmt: Box<dyn Statement<ID, E>>,
}

impl<E: Into<VertexOrEdge> + 'static> FlatMapFunction<Record, Record> for EdgeExpandOperator<E> {
    type Target = DynIter<Record>;

    fn exec(&self, input: Record) -> FnResult<Self::Target> {
        let entry = input
            .get(self.start_v_tag.as_ref())
            .ok_or(FnExecError::GetTagError(KeyedError::from(
                "get start_v failed",
            )))?;
        let vertex_or_edge = entry
            .as_graph_element()
            .ok_or(FnExecError::UnExpectedDataType(
                "start_v does not refer to a graph element".to_string(),
            ))?;
        let id = vertex_or_edge.id();
        let iter = self.stmt.exec(id)?;
        Ok(Box::new(RecordExpandIter::new(
            input,
            self.edge_or_end_v_tag.as_ref(),
            iter,
        )))
    }
}

impl FlatMapFuncGen for algebra_pb::EdgeExpand {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let graph = crate::get_graph().ok_or(FnGenError::NullGraphError)?;
        let expand_base = ExpandBase::try_from(self.base)?;
        if self.is_edge {
            let stmt =
                graph.prepare_explore_edge(expand_base.direction, &expand_base.query_params)?;
            let edge_expand_operator = EdgeExpandOperator {
                start_v_tag: expand_base.v_tag.clone(),
                edge_or_end_v_tag: self.alias.map(|e_tag| e_tag.try_into()).transpose()?,
                stmt,
            };
            debug!(
                "Runtime expand operator of edge with start_v_tag {:?} and edge_tag {:?}",
                edge_expand_operator.start_v_tag, edge_expand_operator.edge_or_end_v_tag
            );
            Ok(Box::new(edge_expand_operator))
        } else {
            let stmt =
                graph.prepare_explore_vertex(expand_base.direction, &expand_base.query_params)?;
            let edge_expand_operator = EdgeExpandOperator {
                start_v_tag: expand_base.v_tag,
                edge_or_end_v_tag: self.alias.map(|v_tag| v_tag.try_into()).transpose()?,
                stmt,
            };
            debug!(
                "Runtime expand operator of vertex with start_v_tag {:?} and end_v_tag {:?}",
                edge_expand_operator.start_v_tag, edge_expand_operator.edge_or_end_v_tag
            );
            Ok(Box::new(edge_expand_operator))
        }
    }
}

pub struct ExpandBase {
    v_tag: Option<NameOrId>,
    direction: Direction,
    query_params: QueryParams,
}

impl TryFrom<Option<algebra_pb::ExpandBase>> for ExpandBase {
    type Error = ParsePbError;

    fn try_from(expand_base: Option<algebra_pb::ExpandBase>) -> Result<Self, Self::Error> {
        if let Some(expand_base) = expand_base {
            let v_tag = if let Some(tag) = expand_base.v_tag {
                Some(tag.try_into()?)
            } else {
                None
            };
            let direction: algebra_pb::expand_base::Direction =
                unsafe { ::std::mem::transmute(expand_base.direction) };
            let query_params = expand_base.params.try_into()?;
            Ok(ExpandBase {
                v_tag,
                direction: Direction::from(direction),
                query_params,
            })
        } else {
            Err(ParsePbError::EmptyFieldError(
                "empty expand_base pb".to_string(),
            ))
        }
    }
}
