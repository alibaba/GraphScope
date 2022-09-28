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
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Entry, Record};

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
            let entry = entry.clone();
            // Make sure there is anything to query with
            // Note that we need to guarantee the requested column if it has any alias,
            // e.g., for g.V().out().as("a").has("name", "marko"), we should compile as:
            // g.V().out().auxilia(as("a"))... where we give alias in auxilia,
            //     then we set tag=None and alias="a" in auxilia
            // TODO: it seems that we do not really care about getting head from curr or "a", we only need to save the updated entry with expected alias "a"
            if self.query_params.is_queryable() {
                // If queryable, then turn into graph element and do the query
                let graph = get_graph().ok_or(FnExecError::NullGraphError)?;
                let new_entry: Option<Entry> = if let Some(v) = entry.as_graph_vertex() {
                    let mut result_iter = graph.get_vertex(&[v.id()], &self.query_params)?;
                    result_iter.next().map(|mut vertex| {
                        if let Some(details) = v.details() {
                            if let Some(properties) = details.get_all_properties() {
                                for (key, val) in properties {
                                    vertex
                                        .get_details_mut()
                                        .insert_property(key, val);
                                }
                            }
                        }
                        vertex.into()
                    })
                } else if let Some(e) = entry.as_graph_edge() {
                    let mut result_iter = graph.get_edge(&[e.id()], &self.query_params)?;
                    result_iter.next().map(|mut edge| {
                        if let Some(details) = e.details() {
                            if let Some(properties) = details.get_all_properties() {
                                for (key, val) in properties {
                                    edge.get_details_mut().insert_property(key, val);
                                }
                            }
                        }
                        edge.into()
                    })
                } else {
                    Err(FnExecError::unexpected_data_error("should be vertex or edge in AuxiliaOperator"))?
                };
                if new_entry.is_some() {
                    input.append(new_entry.unwrap(), self.alias.clone());
                } else {
                    return Ok(None);
                }
            } else {
                if self.alias.is_some() {
                    input.append_arc_entry(entry, self.alias.clone());
                }
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

impl FilterMapFuncGen for algebra_pb::Auxilia {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let tag = self
            .tag
            .map(|alias| alias.try_into())
            .transpose()?;
        let query_params = self.params.try_into()?;
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let remove_tags = self
            .remove_tags
            .into_iter()
            .map(|alias| alias.try_into())
            .collect::<Result<_, _>>()?;
        let auxilia_operator = AuxiliaOperator { tag, query_params, alias, remove_tags };
        debug!("Runtime AuxiliaOperator: {:?}", auxilia_operator);
        Ok(Box::new(auxilia_operator))
    }
}
