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

use graph_proxy::apis::{get_graph, Details, GraphElement, QueryParams};
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::entry::{DynEntry, Entry, EntryType};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

/// An Auxilia operator to get extra information for the current entity.
/// Specifically, we will update the old entity by appending the new extra information,
/// and rename the entity, if `alias` has been set.
#[derive(Debug)]
struct AuxiliaOperator {
    tag: Option<KeyId>,
    query_params: QueryParams,
    alias: Option<KeyId>,
    remove_tags: Vec<KeyId>,
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
                EntryType::VID | EntryType::VERTEX => {
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
                EntryType::EDGE => {
                    let id = entry.id();
                    let mut result_iter = graph.get_edge(&[id], &self.query_params)?;
                    result_iter.next().map(|mut edge| {
                        if let Some(details) = entry
                            .as_edge()
                            .map(|e| e.details())
                            .unwrap_or(None)
                        {
                            if let Some(properties) = details.get_all_properties() {
                                for (key, val) in properties {
                                    edge.get_details_mut().insert_property(key, val);
                                }
                            }
                        }
                        DynEntry::new(edge)
                    })
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

            for remove_tag in &self.remove_tags {
                input.take(Some(remove_tag));
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
    remove_tags: Vec<KeyId>,
}

impl FilterMapFunction<Record, Record> for SimpleAuxiliaOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if input.get(self.tag).is_none() {
            return Ok(None);
        }
        if self.alias.is_some() {
            input.append_arc_entry(input.get(self.tag).unwrap().clone(), self.alias.clone());
        }
        for remove_tag in &self.remove_tags {
            input.take(Some(remove_tag));
        }
        Ok(Some(input))
    }
}

impl FilterMapFuncGen for algebra_pb::Auxilia {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let tag = self
            .tag
            .map(|alias| alias.try_into())
            .transpose()?;
        let query_params: QueryParams = self.params.try_into()?;
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let remove_tags = self
            .remove_tags
            .into_iter()
            .map(|alias| alias.try_into())
            .collect::<Result<_, _>>()?;
        if query_params.is_queryable() {
            let auxilia_operator = AuxiliaOperator { tag, query_params, alias, remove_tags };
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("Runtime AuxiliaOperator: {:?}", auxilia_operator);
            }
            Ok(Box::new(auxilia_operator))
        } else {
            let auxilia_operator = SimpleAuxiliaOperator { tag, alias, remove_tags };
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("Runtime SimpleAuxiliaOperator: {:?}", auxilia_operator);
            }
            Ok(Box::new(auxilia_operator))
        }
    }
}
