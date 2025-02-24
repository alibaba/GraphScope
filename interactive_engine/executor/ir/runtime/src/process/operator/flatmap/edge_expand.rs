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

use dyn_type::Object;
use graph_proxy::apis::{
    get_graph, Direction, DynDetails, GraphElement, QueryParams, Statement, Vertex, ID,
};
use ir_common::generated::algebra::edge_expand::ExpandOpt;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::entry::{Entry, EntryType, NullEntry};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Record, RecordExpandIter, RecordPathExpandIter};

pub struct EdgeExpandOperator<E: Entry> {
    start_v_tag: Option<KeyId>,
    alias: Option<KeyId>,
    stmt: Box<dyn Statement<ID, E>>,
    expand_opt: ExpandOpt,
    is_optional: bool,
}

impl<E: Entry + 'static> FlatMapFunction<Record, Record> for EdgeExpandOperator<E> {
    type Target = DynIter<Record>;

    fn exec(&self, mut input: Record) -> FnResult<Self::Target> {
        if let Some(entry) = input.get(self.start_v_tag) {
            match entry.get_type() {
                EntryType::Vertex => {
                    let id = entry.id();
                    let mut iter = self.stmt.exec(id)?.peekable();
                    match self.expand_opt {
                        // the case of expand edge, and get end vertex;
                        ExpandOpt::Vertex => {
                            if self.is_optional && iter.peek().is_none() {
                                input.append(NullEntry, self.alias);
                                Ok(Box::new(vec![input].into_iter()))
                            } else {
                                let neighbors_iter = iter.map(|e| {
                                    if let Some(e) = e.as_edge() {
                                        Vertex::new(
                                            e.get_other_id(),
                                            e.get_other_label().cloned(),
                                            DynDetails::default(),
                                        )
                                    } else {
                                        unreachable!()
                                    }
                                });
                                Ok(Box::new(RecordExpandIter::new(
                                    input,
                                    self.alias.as_ref(),
                                    Box::new(neighbors_iter),
                                )))
                            }
                        }
                        // the case of expand neighbors, including edges/vertices
                        ExpandOpt::Edge => {
                            if self.is_optional && iter.peek().is_none() {
                                input.append(NullEntry, self.alias);
                                Ok(Box::new(vec![input].into_iter()))
                            } else {
                                Ok(Box::new(RecordExpandIter::new(
                                    input,
                                    self.alias.as_ref(),
                                    Box::new(iter),
                                )))
                            }
                        }
                        // the case of get degree.
                        ExpandOpt::Degree => {
                            let degree = iter.count();
                            input.append(object!(degree), self.alias);
                            Ok(Box::new(vec![input].into_iter()))
                        }
                    }
                }
                EntryType::Path => {
                    if self.is_optional {
                        Err(FnExecError::unsupported_error(
                            "Have not supported Optional Edge Expand in Path entry yet",
                        ))?
                    } else {
                        let graph_path = entry
                            .as_graph_path()
                            .ok_or_else(|| FnExecError::Unreachable)?;
                        let iter = self.stmt.exec(graph_path.get_path_end().id())?;
                        let curr_path = graph_path.clone();
                        Ok(Box::new(RecordPathExpandIter::new(input, curr_path, iter)))
                    }
                }
                EntryType::Object => {
                    let obj = entry
                        .as_object()
                        .ok_or_else(|| FnExecError::Unreachable)?;
                    if Object::None.eq(obj) {
                        input.append(Object::None, self.alias);
                        Ok(Box::new(vec![input].into_iter()))
                    } else {
                        Err(FnExecError::unexpected_data_error(&format!(
                            "Cannot Expand from current entry {:?}",
                            entry
                        )))?
                    }
                }
                _ => Err(FnExecError::unexpected_data_error(&format!(
                    "Cannot Expand from current entry {:?}",
                    entry
                )))?,
            }
        } else {
            Ok(Box::new(vec![].into_iter()))
        }
    }
}

impl FlatMapFuncGen for pb::EdgeExpand {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let graph = get_graph().ok_or_else(|| FnGenError::NullGraphError)?;
        let start_v_tag = self.v_tag;
        let edge_or_end_v_tag = self.alias;
        let direction_pb: pb::edge_expand::Direction = unsafe { ::std::mem::transmute(self.direction) };
        let direction = Direction::from(direction_pb);
        let query_params: QueryParams = self.params.try_into()?;
        let expand_opt: ExpandOpt = unsafe { ::std::mem::transmute(self.expand_opt) };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!(
                "Runtime expand operator of edge with start_v_tag {:?}, end_tag {:?}, direction {:?}, query_params {:?}, expand_opt {:?}",
                start_v_tag, edge_or_end_v_tag, direction, query_params, expand_opt
            );
        }

        match expand_opt {
            ExpandOpt::Vertex => {
                if query_params.filter.is_some() {
                    // Expand vertices with filters on edges.
                    // This can be regarded as a combination of EdgeExpand (with is_edge = true) + GetV
                    let stmt = graph.prepare_explore_edge(direction, &query_params)?;
                    let edge_expand_operator = EdgeExpandOperator {
                        start_v_tag,
                        alias: edge_or_end_v_tag,
                        stmt,
                        expand_opt: ExpandOpt::Vertex,
                        is_optional: self.is_optional,
                    };
                    Ok(Box::new(edge_expand_operator))
                } else {
                    // Expand vertices without any filters
                    let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
                    let edge_expand_operator = EdgeExpandOperator {
                        start_v_tag,
                        alias: edge_or_end_v_tag,
                        stmt,
                        expand_opt: ExpandOpt::Edge,
                        is_optional: self.is_optional,
                    };
                    Ok(Box::new(edge_expand_operator))
                }
            }
            _ => {
                // Expand edges or degree
                let stmt = graph.prepare_explore_edge(direction, &query_params)?;
                let edge_expand_operator = EdgeExpandOperator {
                    start_v_tag,
                    alias: edge_or_end_v_tag,
                    stmt,
                    expand_opt,
                    is_optional: self.is_optional,
                };
                Ok(Box::new(edge_expand_operator))
            }
        }
    }
}
