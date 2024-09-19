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
use graph_proxy::apis::GraphElement;
use graph_proxy::apis::{get_graph, DynDetails, GraphPath, QueryParams, Vertex};
use graph_proxy::utils::expr::eval_pred::EvalPred;
use ir_common::error::ParsePbError;
use ir_common::generated::physical as pb;
use ir_common::generated::physical::get_v::VOpt;
use ir_common::{KeyId, LabelId};
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnExecResult, FnGenError, FnGenResult};
use crate::process::entry::{DynEntry, Entry};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct GetVertexOperator {
    start_tag: Option<KeyId>,
    opt: VOpt,
    alias: Option<KeyId>,
    query_labels: Vec<LabelId>,
}

impl GetVertexOperator {
    fn contains_label(&self, label: Option<&LabelId>) -> FnExecResult<bool> {
        if self.query_labels.is_empty() {
            // no label constraint
            Ok(true)
        } else {
            if let Some(label) = label {
                Ok(self.query_labels.contains(label))
            } else {
                Err(FnExecError::UnExpectedData(format!(
                    "Label is None in GetVertexOperator, with Opr {:?}",
                    self,
                )))?
            }
        }
    }
}

impl FilterMapFunction<Record, Record> for GetVertexOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if let Some(entry) = input.get(self.start_tag) {
            if let Some(e) = entry.as_edge() {
                let (id, label) = match self.opt {
                    VOpt::Start => (e.src_id, e.get_src_label()),
                    VOpt::End => (e.dst_id, e.get_dst_label()),
                    VOpt::Other => (e.get_other_id(), e.get_other_label()),
                    _ => unreachable!(),
                };
                if self.contains_label(label)? {
                    let vertex = Vertex::new(id, label.cloned(), DynDetails::default());
                    input.append(vertex, self.alias.clone());
                    Ok(Some(input))
                } else {
                    Ok(None)
                }
            } else if let Some(graph_path) = entry.as_graph_path() {
                // Specifically, when dealing with `GetV` on a path entry, we need to consider the following cases:
                // 1. if the last entry is an edge, we expand the path with the other vertex of the edge; (in which case is expanding the path with a adj vertex)
                // 2. if the last entry is a vertex, we get the vertex. (in which case is getting the end vertex from the path)
                let path_end = graph_path.get_path_end();
                if let Some(path_end_edge) = path_end.as_edge() {
                    let label = path_end_edge.get_other_label();
                    if self.contains_label(label)? {
                        let vertex = Vertex::new(
                            path_end_edge.get_other_id(),
                            label.cloned(),
                            DynDetails::default(),
                        );
                        let mut_graph_path = input
                            .get_mut(self.start_tag)
                            .ok_or_else(|| {
                                FnExecError::unexpected_data_error(&format!(
                                    "get_mut of GraphPath failed in {:?}",
                                    self
                                ))
                            })?
                            .as_any_mut()
                            .downcast_mut::<GraphPath>()
                            .ok_or_else(|| {
                                FnExecError::unexpected_data_error(&format!("entry is not a path in GetV"))
                            })?;
                        if mut_graph_path.append(vertex) {
                            Ok(Some(input))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                } else if let Some(path_end_vertex) = path_end.as_vertex() {
                    let label = path_end_vertex.label();
                    if self.contains_label(label.as_ref())? {
                        input.append(path_end_vertex.clone(), self.alias.clone());
                        Ok(Some(input))
                    } else {
                        Ok(None)
                    }
                } else {
                    Err(FnExecError::unexpected_data_error("unreachable path end entry in GetV"))?
                }
            } else if let Some(obj) = entry.as_object() {
                if Object::None.eq(obj) {
                    input.append(Object::None, self.alias);
                    Ok(Some(input))
                } else {
                    Err(FnExecError::unexpected_data_error(&format!(
                        "Can only apply `GetV` on an object that is not None. The entry is {:?}",
                        entry
                    )))?
                }
            } else {
                Err(FnExecError::unexpected_data_error( &format!(
                    "Can only apply `GetV` (`Auxilia` instead) on an edge or path entry, while the entry is {:?}", entry
                )))?
            }
        } else {
            Ok(None)
        }
    }
}

/// An Auxilia operator to get extra information for the current entity.
/// Specifically, we will update the old entity by appending the new extra information,
/// and rename the entity, if `alias` has been set.
#[derive(Debug)]
struct AuxiliaOperator {
    tag: Option<KeyId>,
    query_params: QueryParams,
    alias: Option<KeyId>,
}

impl FilterMapFunction<Record, Record> for AuxiliaOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if let Some(entry) = input.get(self.tag) {
            // Note that we need to guarantee the requested column if it has any alias,
            // e.g., for g.V().out().as("a").has("name", "marko"), we should compile as:
            // g.V().out().auxilia(as("a"))... where we give alias in auxilia,
            //     then we set tag=None and alias="a" in auxilia

            // 1. If to filter by labels, and the entry itself carries label information already, directly eval it without query the store
            if self.query_params.has_labels() && entry.label().is_some() {
                if !self
                    .query_params
                    .labels
                    .contains(&entry.label().unwrap())
                {
                    // pruning by labels
                    return Ok(None);
                } else if !self.query_params.has_predicates() && !self.query_params.has_columns() {
                    // if only filter by labels, return the results.
                    // if it has alias, append without moving head
                    if let Some(alias) = self.alias {
                        // append without moving head
                        let entry_clone = entry.clone();
                        input
                            .get_columns_mut()
                            .insert(alias as usize, entry_clone);
                    }
                    return Ok(Some(input));
                }
            }
            // 2. Otherwise, filter after query store, e.g., the case of filter by columns.
            let graph = get_graph().ok_or_else(|| FnExecError::NullGraphError)?;
            if let Some(v) = entry.as_vertex() {
                let id = v.id();
                if let Some(vertex) = graph
                    .get_vertex(&[id], &self.query_params)?
                    .next()
                    .map(|vertex| DynEntry::new(vertex))
                {
                    if let Some(alias) = self.alias {
                        // append without moving head
                        input
                            .get_columns_mut()
                            .insert(alias as usize, vertex.into());
                    } else {
                        input.append(vertex, self.alias.clone());
                    }
                } else {
                    return Ok(None);
                }
            } else if let Some(_edge) = entry.as_edge() {
                // TODO: This is a little bit tricky. Modify this logic to query store once query by eid is supported.
                // Currently, we support two cases:
                // 1. use auxilia to rename the edge to a new alias;
                // 2. use auxilia to filter the edge by predicates, where the necessary properties of the edge is assumed to be already pre-cached.
                let entry = entry.clone();
                if let Some(predicate) = &self.query_params.filter {
                    let res = predicate
                        .eval_bool(Some(&input))
                        .map_err(|e| FnExecError::from(e))?;
                    if !res {
                        return Ok(None);
                    }
                }
                if let Some(alias) = self.alias {
                    // append without moving head
                    input
                        .get_columns_mut()
                        .insert(alias as usize, entry);
                } else {
                    input.append_arc_entry(entry, self.alias.clone());
                }
            } else if let Some(graph_path) = entry.as_graph_path() {
                // 1. Auxilia for vertices in Path for filtering.
                // 2. Auxilia for vertices in Path for property caching.
                let path_end = graph_path.get_path_end();
                if let Some(v) = graph
                    .get_vertex(&[path_end.id()], &self.query_params)?
                    .next()
                {
                    if self.query_params.has_columns() {
                        // for property caching
                        let mut_graph_path = input
                            .get_mut(self.tag)
                            .ok_or_else(|| {
                                FnExecError::unexpected_data_error(&format!(
                                    "get_mut of GraphPath failed in {:?}",
                                    self
                                ))
                            })?
                            .as_any_mut()
                            .downcast_mut::<GraphPath>()
                            .ok_or_else(|| {
                                FnExecError::unexpected_data_error(&format!("entry is not a path in GetV"))
                            })?;
                        let path_end = mut_graph_path.get_path_end_mut();
                        *path_end = v.into();
                    }
                    return Ok(Some(input));
                } else {
                    return Ok(None);
                }
            } else if let Some(obj) = entry.as_object() {
                if Object::None.eq(obj) {
                    if let Some(predicate) = &self.query_params.filter {
                        let res = predicate
                            .eval_bool(Some(&input))
                            .map_err(|e| FnExecError::from(e))?;
                        if res {
                            input.append(Object::None, self.alias);
                            return Ok(Some(input));
                        } else {
                            return Ok(None);
                        }
                    } else {
                        input.append(Object::None, self.alias);
                        return Ok(Some(input));
                    }
                } else {
                    Err(FnExecError::unexpected_data_error(&format!(
                        "neither Vertex nor Edge entry is accessed in `Auxilia` operator, the entry is {:?}",
                        entry
                    )))?
                }
            } else {
                Err(FnExecError::unexpected_data_error(&format!(
                    "neither Vertex nor Edge entry is accessed in `Auxilia` operator, the entry is {:?}",
                    entry
                )))?
            }
            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}

impl FilterMapFuncGen for pb::GetV {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        match opt {
            VOpt::Both => Err(ParsePbError::from(
                "the `GetV` operator is not a `FilterMap`, which has GetV::VOpt::Both",
            ))?,
            VOpt::Start | VOpt::End | VOpt::Other => {
                let mut tables_condition: Vec<LabelId> = vec![];
                if let Some(params) = self.params {
                    if params.has_predicates() || params.has_columns() {
                        Err(FnGenError::unsupported_error(&format!("QueryParams in GetV {:?}", params)))?
                    } else {
                        tables_condition = params
                            .tables
                            .into_iter()
                            .map(|label| label.try_into())
                            .collect::<Result<Vec<_>, _>>()?;
                    }
                }
                let get_vertex_operator = GetVertexOperator {
                    start_tag: self.tag,
                    opt,
                    alias: self.alias,
                    query_labels: tables_condition,
                };
                if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                    debug!("Runtime GetVertexOperator: {:?}", get_vertex_operator);
                }
                Ok(Box::new(get_vertex_operator))
            }
            VOpt::Itself => {
                let query_params: QueryParams = self.params.try_into()?;
                let auxilia_operator = AuxiliaOperator { tag: self.tag, query_params, alias: self.alias };
                if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                    debug!("Runtime AuxiliaOperator: {:?}", auxilia_operator);
                }
                Ok(Box::new(auxilia_operator))
            }
        }
    }
}
