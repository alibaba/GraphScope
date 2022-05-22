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

use std::collections::HashMap;
use std::sync::Arc;

use dyn_type::{Object, Primitives};
use graph_store::utils::IterList;
use ir_common::NameOrId as Label;
use ir_common::{KeyId, NameOrId};
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::prelude::Property;
use maxgraph_store::api::*;
use maxgraph_store::api::{Edge as StoreEdge, Vertex as StoreVertex};
use maxgraph_store::api::{PropId, SnapshotId};
use pegasus::api::function::FnResult;
use runtime::error::{FnExecError, FnExecResult};
use runtime::graph::element::{Edge, Vertex};
use runtime::graph::property::{DefaultDetails, DynDetails};
use runtime::graph::{Direction, GraphProxy, QueryParams, Statement, ID};
use runtime::register_graph;

use crate::from_fn;
use crate::{filter_limit, limit_n};

// Should be identical to the param_name given by compiler
const SNAPSHOT_ID: &str = "SID";
// This will refer to the latest graph
const DEFAULT_SNAPSHOT_ID: SnapshotId = SnapshotId::max_value() - 1;

pub struct GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    store: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
}

#[allow(dead_code)]
pub fn create_gs_store<V, VI, E, EI>(
    store: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
) where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    let graph = GraphScopeStore { store, partition_manager };
    register_graph(Arc::new(graph));
}

impl<V, VI, E, EI> GraphProxy for GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn scan_vertex(&self, params: &QueryParams) -> FnResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| s.as_i64().unwrap_or(DEFAULT_SNAPSHOT_ID))
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_label(params.labels.as_ref())?;
            let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
            let filter = params.filter.clone();
            let partitions: Vec<PartitionId> = partitions
                .iter()
                .map(|pid| *pid as PartitionId)
                .collect();
            if let Some(indexed_values) = params.get_extra_param("PK") {
                match indexed_values {
                    Object::Vector(prop_vals) => {
                        let mut properties = Vec::with_capacity(prop_vals.len());
                        for prop_val in prop_vals {
                            let property = encode_store_prop_val(prop_val.clone());
                            properties.push(property);
                        }
                        if label_ids.len() != 1 {
                            Err(FnExecError::query_store_error("PK only supports a single label"))?
                        }
                        if let Some(vid) = self
                            .partition_manager
                            // TODO: vineyard should also implement this function
                            .get_vertex_id_by_primary_keys(*label_ids.get(0).unwrap(), properties.as_ref())
                        {
                            Ok(Box::new(
                                vec![Vertex::new(
                                    vid as ID,
                                    params.labels.get(0).cloned(),
                                    DynDetails::new(DefaultDetails::new(HashMap::new())),
                                )]
                                .into_iter(),
                            ))
                        } else {
                            Ok(Box::new(std::iter::empty()))
                        }
                    }
                    _ => Err(FnExecError::query_store_error("PK values should be a vector"))?,
                }
            } else {
                let result = store
                    .get_all_vertices(
                        si,
                        label_ids.as_ref(),
                        // None means no filter condition pushed down to storage as not supported yet. Same as follows.
                        None,
                        // None means no need to dedup by properties. Same as follows.
                        None,
                        prop_ids.as_ref(),
                        // Zero limit means no limit. Same as follows.
                        params.limit.unwrap_or(0),
                        // Each worker will scan the partitions pre-allocated in source operator. Same as follows.
                        partitions.as_ref(),
                    )
                    .map(move |v| to_runtime_vertex(&v));

                Ok(filter_limit!(result, filter, None))
            }
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn scan_edge(&self, params: &QueryParams) -> FnResult<Box<dyn Iterator<Item = Edge> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| s.as_i64().unwrap_or(DEFAULT_SNAPSHOT_ID))
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_label(params.labels.as_ref())?;
            let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
            let filter = params.filter.clone();
            let partitions: Vec<PartitionId> = partitions
                .iter()
                .map(|pid| *pid as PartitionId)
                .collect();
            let result = store
                .get_all_edges(
                    si,
                    label_ids.as_ref(),
                    None,
                    None,
                    prop_ids.as_ref(),
                    params.limit.unwrap_or(0),
                    partitions.as_ref(),
                )
                .map(move |e| to_runtime_edge(&e));

            Ok(filter_limit!(result, filter, None))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> FnResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| s.as_i64().unwrap_or(DEFAULT_SNAPSHOT_ID))
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
        let filter = params.filter.clone();
        let partition_label_vertex_ids =
            get_partition_label_vertex_ids(ids, self.partition_manager.clone());
        let result = store
            .get_vertex_properties(si, partition_label_vertex_ids, prop_ids.as_ref())
            .map(move |v| to_runtime_vertex(&v));

        Ok(filter_limit!(result, filter, None))
    }

    fn get_edge(
        &self, _ids: &[ID], _params: &QueryParams,
    ) -> FnResult<Box<dyn Iterator<Item = Edge> + Send>> {
        // TODO(bingqing): adapt get_edge when graphscope support this
        Err(FnExecError::query_store_error("GraphScope storage does not support get_edge for now"))?
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> FnResult<Box<dyn Statement<ID, Vertex>>> {
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let store = self.store.clone();
        let partition_manager = self.partition_manager.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| s.as_i64().unwrap_or(DEFAULT_SNAPSHOT_ID))
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let edge_label_ids = encode_storage_label(params.labels.as_ref())?;

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_ids(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_vertex_ids(
                    si,
                    src_id,
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_vertex_ids(
                    si,
                    src_id,
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::Both => {
                    let mut iters = vec![];
                    let out_iter = store.get_out_vertex_ids(
                        si,
                        src_id.clone(),
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        limit.clone().unwrap_or(0),
                    );
                    iters.push(out_iter);
                    let in_iter = store.get_in_vertex_ids(
                        si,
                        src_id,
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        limit.unwrap_or(0),
                    );
                    iters.push(in_iter);
                    Box::new(IterList::new(iters))
                }
            };
            let iters = iter.map(|(_src, vi)| vi).collect();
            let iter_list = IterList::new(iters).map(move |v| to_runtime_vertex(&v));
            Ok(filter_limit!(iter_list, filter, None))
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> FnResult<Box<dyn Statement<ID, Edge>>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| s.as_i64().unwrap_or(DEFAULT_SNAPSHOT_ID))
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let partition_manager = self.partition_manager.clone();
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let edge_label_ids = encode_storage_label(params.labels.as_ref())?;
        let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_ids(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_edges(
                    si,
                    src_id,
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    prop_ids.as_ref(),
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_edges(
                    si,
                    src_id,
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    prop_ids.as_ref(),
                    limit.unwrap_or(0),
                ),
                Direction::Both => {
                    let mut iter = vec![];
                    let out_iter = store.get_out_edges(
                        si,
                        src_id.clone(),
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        prop_ids.as_ref(),
                        limit.clone().unwrap_or(0),
                    );
                    iter.push(out_iter);
                    let in_iter = store.get_in_edges(
                        si,
                        src_id,
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        prop_ids.as_ref(),
                        limit.unwrap_or(0),
                    );
                    iter.push(in_iter);
                    Box::new(IterList::new(iter))
                }
            };
            let iters = iter.map(|(_src, ei)| ei).collect();
            let iter_list = IterList::new(iters).map(move |e| to_runtime_edge(&e));
            Ok(filter_limit!(iter_list, filter, None))
        });
        Ok(stmt)
    }
}

#[inline]
fn to_runtime_vertex<V: StoreVertex>(v: &V) -> Vertex {
    let id = v.get_id() as ID;
    let label = encode_runtime_v_label(v);
    let properties = v
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    Vertex::new(id, Some(label), DynDetails::new(DefaultDetails::new(properties)))
}

#[inline]
fn to_runtime_edge<E: StoreEdge>(e: &E) -> Edge {
    let id = e.get_edge_id() as ID;
    let label = encode_runtime_e_label(e);
    let properties = e
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    let mut edge = Edge::new(
        id,
        Some(label),
        e.get_src_id() as ID,
        e.get_dst_id() as ID,
        DynDetails::new(DefaultDetails::new(properties)),
    );
    edge.set_src_label(Some(NameOrId::Id(e.get_src_label_id() as KeyId)));
    edge.set_dst_label(Some(NameOrId::Id(e.get_dst_label_id() as KeyId)));
    edge
}

/// in maxgraph store, Option<Vec<PropId>>: None means we need all properties,
/// and Some means we need given properties (and Some(vec![]) means we do not need any property)
/// while in ir, None means we do not need any properties,
/// and Some means we need given properties (and Some(vec![]) means we need all properties)
#[inline]
fn encode_storage_prop_keys(prop_names: Option<&Vec<NameOrId>>) -> FnExecResult<Option<Vec<PropId>>> {
    if let Some(prop_names) = prop_names {
        if prop_names.is_empty() {
            Ok(None)
        } else {
            let encoded_prop_ids = prop_names
                .iter()
                .map(|prop_key| match prop_key {
                    NameOrId::Str(_) => Err(FnExecError::query_store_error(
                        "encode storage prop key error, should provide prop_id",
                    )),
                    NameOrId::Id(prop_id) => Ok(*prop_id as PropId),
                })
                .collect::<Result<Vec<LabelId>, _>>()?;
            Ok(Some(encoded_prop_ids))
        }
    } else {
        Ok(Some(vec![]))
    }
}

#[inline]
fn encode_storage_label(labels: &Vec<Label>) -> FnExecResult<Vec<LabelId>> {
    labels
        .iter()
        .map(|label| match label {
            Label::Str(_) => {
                Err(FnExecError::query_store_error("encode storage label error, should provide label_id"))
            }
            Label::Id(id) => Ok(*id as LabelId),
        })
        .collect::<Result<Vec<LabelId>, _>>()
}

#[inline]
fn encode_runtime_v_label<V: StoreVertex>(v: &V) -> NameOrId {
    NameOrId::Id(v.get_label_id() as KeyId)
}

#[inline]
fn encode_runtime_e_label<E: StoreEdge>(e: &E) -> NameOrId {
    NameOrId::Id(e.get_label_id() as KeyId)
}

#[inline]
fn encode_runtime_property(prop_id: PropId, prop_val: Property) -> (NameOrId, Object) {
    let prop_key = NameOrId::Id(prop_id as KeyId);
    let prop_val = encode_runtime_prop_val(prop_val);
    (prop_key, prop_val)
}

#[inline]
fn encode_runtime_prop_val(prop_val: Property) -> Object {
    match prop_val {
        Property::Bool(b) => b.into(),
        Property::Char(c) => {
            if c <= (i8::MAX as u8) {
                Object::Primitive(Primitives::Byte(c as i8))
            } else {
                Object::Primitive(Primitives::Integer(c as i32))
            }
        }
        Property::Short(s) => Object::Primitive(Primitives::Integer(s as i32)),
        Property::Int(i) => Object::Primitive(Primitives::Integer(i)),
        Property::Long(l) => Object::Primitive(Primitives::Long(l)),
        Property::Float(f) => Object::Primitive(Primitives::Float(f as f64)),
        Property::Double(d) => Object::Primitive(Primitives::Float(d)),
        Property::Bytes(v) => Object::Blob(v.into_boxed_slice()),
        Property::String(s) => Object::String(s),
        _ => unimplemented!(),
    }
}

#[inline]
fn encode_store_prop_val(prop_val: Object) -> Property {
    match prop_val {
        Object::Primitive(p) => match p {
            Primitives::Byte(b) => Property::Char(b as u8),
            Primitives::Integer(i) => Property::Int(i),
            Primitives::Long(i) => Property::Long(i),
            // TODO: overflow check
            Primitives::ULLong(i) => Property::Long(i as i64),
            Primitives::Float(f) => Property::Double(f),
        },
        Object::String(s) => Property::String(s),
        Object::Vector(vec) => {
            if let Some(probe) = vec.get(0) {
                match probe {
                    Object::Primitive(p) => match p {
                        Primitives::Byte(_) | Primitives::Integer(_) => Property::ListInt(
                            vec.into_iter()
                                .map(|i| i.as_i32().unwrap())
                                .collect(),
                        ),
                        Primitives::Long(_) => Property::ListLong(
                            vec.into_iter()
                                .map(|i| i.as_i64().unwrap())
                                .collect(),
                        ),
                        Primitives::ULLong(_) => {
                            // TODO:  overflow check
                            Property::ListLong(
                                vec.into_iter()
                                    .map(|i| i.as_u128().unwrap() as i64)
                                    .collect(),
                            )
                        }
                        Primitives::Float(_) => Property::ListDouble(
                            vec.into_iter()
                                .map(|i| i.as_f64().unwrap())
                                .collect(),
                        ),
                    },
                    Object::String(_) => Property::ListString(
                        vec.into_iter()
                            .map(|i| i.as_str().unwrap().into_owned())
                            .collect(),
                    ),
                    Object::Blob(_) => Property::ListBytes(
                        vec.into_iter()
                            .map(|i| i.as_bytes().unwrap().to_vec())
                            .collect(),
                    ),
                    Object::None => Property::Null,
                    _ => Property::Unknown,
                }
            } else {
                Property::Null
            }
        }
        Object::Blob(b) => Property::Bytes(b.to_vec()),
        Object::None => Property::Null,
        _ => Property::Unknown,
    }
}

/// Transform type of ids to PartitionLabeledVertexIds as required by graphscope store,
/// which consists of (PartitionId, Vec<(Option<LabelId>, Vec<VertexId>)>)
fn get_partition_label_vertex_ids(
    ids: &[ID], graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> Vec<PartitionLabeledVertexIds> {
    let mut partition_label_vid_map = HashMap::new();
    for vid in ids {
        let partition_id = graph_partition_manager.get_partition_id(*vid as VertexId) as PartitionId;
        let label_vid_list = partition_label_vid_map
            .entry(partition_id)
            .or_insert(HashMap::new());
        label_vid_list
            .entry(None)
            .or_insert(vec![])
            .push(*vid as VertexId);
    }

    partition_label_vid_map
        .into_iter()
        .map(|(pid, label_vid_map)| (pid, label_vid_map.into_iter().collect()))
        .collect()
}

/// Transform type of ids to PartitionVertexIds as required by graphscope store,
/// which consists of (PartitionId,Vec<VertexId>)
fn get_partition_vertex_ids(
    id: ID, graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> Vec<PartitionVertexIds> {
    let partition_id = graph_partition_manager.get_partition_id(id as VertexId) as PartitionId;
    vec![(partition_id, vec![id as VertexId])]
}
