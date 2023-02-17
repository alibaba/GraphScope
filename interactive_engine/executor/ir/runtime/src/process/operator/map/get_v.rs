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

use graph_proxy::apis::Details;
use graph_proxy::apis::GraphElement;
use graph_proxy::apis::{get_graph, DynDetails, QueryParams, Vertex};
use ir_common::error::ParsePbError;
use ir_common::generated::physical as pb;
use ir_common::generated::physical::get_v::VOpt;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::entry::{DynEntry, Entry, EntryType};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct GetVertexOperator {
    start_tag: Option<KeyId>,
    opt: VOpt,
    alias: Option<KeyId>,
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
                let vertex = Vertex::new(id, label.map(|l| l.clone()), DynDetails::default());
                input.append(vertex, self.alias.clone());
                Ok(Some(input))
            } else if let Some(graph_path) = entry.as_graph_path() {
                if let VOpt::End = self.opt {
                    let path_end = graph_path
                        .get_path_end()
                        .ok_or(FnExecError::unexpected_data_error("Get path_end failed in path expand"))?
                        .clone();
                    input.append(path_end, self.alias.clone());
                    Ok(Some(input))
                } else {
                    Err(FnExecError::unsupported_error(
                        "Only support `GetV` with VOpt::End on a path entry",
                    ))?
                }
            } else {
                Err(FnExecError::unexpected_data_error(
                    "Can only apply `GetV` (`Auxilia` instead) on an edge or path entry",
                ))?
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
            // If queryable, then turn into graph element and do the query
            let graph = get_graph().ok_or(FnExecError::NullGraphError)?;
            let new_entry: Option<DynEntry> = match entry.get_type() {
                EntryType::Vertex => {
                    let id = entry.id();
                    let mut result_iter = graph.get_vertex(&[id], &self.query_params)?;
                    result_iter.next().map(|mut vertex| {
                        // TODO:confirm the update case, and avoid it if possible.
                        if let Some(details) = entry
                            .as_vertex()
                            .map(|v| v.details())
                            .unwrap_or(None)
                        {
                            if let Some(properties) = details.get_all_properties() {
                                for (key, val) in properties {
                                    vertex
                                        .get_details_mut()
                                        .insert_property(key, val);
                                }
                            }
                        }
                        DynEntry::new(vertex)
                    })
                }
                EntryType::Edge => {
                    // let id = entry.id();
                    // let mut result_iter = graph.get_edge(&[id], &self.query_params)?;
                    // result_iter.next().map(|mut edge| {
                    //     if let Some(details) = entry
                    //         .as_edge()
                    //         .map(|e| e.details())
                    //         .unwrap_or(None)
                    //     {
                    //         if let Some(properties) = details.get_all_properties() {
                    //             for (key, val) in properties {
                    //                 edge.get_details_mut().insert_property(key, val);
                    //             }
                    //         }
                    //     }
                    //     DynEntry::new(edge)
                    // })

                    // TODO: This is a little bit tricky. Modify this logic to query store with eid when supported.
                    // Currently, when getting properties from an edge,
                    // we assume that it has already been carried in the edge (when the first time queried the edge)
                    // since on most storages, query edges by eid is not supported yet.
                    Some(entry.clone())
                }
                _ => Err(FnExecError::unexpected_data_error(&format!(
                    "neither Vertex nor Edge entry is accessed in `Auxilia` operator, the entry is {:?}",
                    entry
                )))?,
            };
            if new_entry.is_some() {
                input.append(new_entry.unwrap(), self.alias.clone());
            } else {
                return Ok(None);
            }

            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
struct SimpleAuxiliaOperator {
    tag: Option<KeyId>,
    alias: Option<KeyId>,
}

impl FilterMapFunction<Record, Record> for SimpleAuxiliaOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if input.get(self.tag).is_none() {
            return Ok(None);
        }
        if self.alias.is_some() {
            input.append_arc_entry(input.get(self.tag).unwrap().clone(), self.alias.clone());
        }
        Ok(Some(input))
    }
}

impl FilterMapFuncGen for pb::GetV {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        let query_params: QueryParams = self.params.try_into()?;
        match opt {
            VOpt::Both => Err(ParsePbError::from(
                "the `GetV` operator is not a `FilterMap`, which has GetV::VOpt::Both",
            ))?,
            VOpt::Start | VOpt::End | VOpt::Other => {
                let get_vertex_operator = GetVertexOperator { start_tag: self.tag, opt, alias: self.alias };
                if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                    debug!("Runtime get_vertex operator: {:?}", get_vertex_operator);
                }
                Ok(Box::new(get_vertex_operator))
            }
            VOpt::Itself => {
                if query_params.is_queryable() {
                    let auxilia_operator =
                        AuxiliaOperator { tag: self.tag, query_params, alias: self.alias };
                    if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                        debug!("Runtime AuxiliaOperator: {:?}", auxilia_operator);
                    }
                    Ok(Box::new(auxilia_operator))
                } else {
                    let auxilia_operator = SimpleAuxiliaOperator { tag: self.tag, alias: self.alias };
                    if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                        debug!("Runtime SimpleAuxiliaOperator: {:?}", auxilia_operator);
                    }
                    Ok(Box::new(auxilia_operator))
                }
            }
        }
    }
}
