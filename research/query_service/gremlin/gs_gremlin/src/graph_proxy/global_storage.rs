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

use dyn_type::{Object, Primitives};
use gremlin_core::graph_proxy::from_fn;
use gremlin_core::structure::LabelId as RuntimeLabelId;
use gremlin_core::structure::{
    DefaultDetails, Direction, DynDetails, Edge, Label, PropKey, QueryParams, Statement, Vertex,
};
use gremlin_core::{filter_limit, limit_n, str_to_dyn_error, IterList};
use gremlin_core::{register_graph, DynResult, GraphProxy, ID};
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::graph_schema::Schema;
use maxgraph_store::api::prelude::Property;
use maxgraph_store::api::PropId;
use maxgraph_store::api::*;
use maxgraph_store::api::{Edge as StoreEdge, Vertex as StoreVertex};
use std::collections::HashMap;
use std::sync::Arc;

static INVALID_LABEL_ID: LabelId = 0xffffffff;
static INVALID_PROP_ID: PropId = 0xffffffff;
// TODO(bingqing): confirm with compiler, compiler should set the param_key as "SID"
const SNAPSHOT_ID: &str = "SID";

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
    let graph = GraphScopeStore {
        store,
        partition_manager,
    };
    register_graph(Arc::new(graph));
}

impl<V, VI, E, EI> GraphProxy for GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn scan_vertex(
        &self,
        params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .ok_or(str_to_dyn_error("get snapshot_id failed"))?
                .as_i64()
                .map_err(|e| str_to_dyn_error(&e.to_string()))? as SnapshotId;
            let schema = store
                .get_schema(si)
                .ok_or(str_to_dyn_error("get schema failed"))?;
            let label_ids = encode_storage_label(params.labels.as_ref(), schema.clone());
            let prop_ids = encode_storage_prop_key(params.props.as_ref(), schema.clone());
            let filter = params.filter.clone();
            let partitions: Vec<PartitionId> =
                partitions.iter().map(|pid| *pid as PartitionId).collect();
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
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn scan_edge(
        &self,
        params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .ok_or(str_to_dyn_error("get snapshot_id failed"))?
                .as_i64()
                .map_err(|e| str_to_dyn_error(&e.to_string()))? as SnapshotId;
            let schema = store
                .get_schema(si)
                .ok_or(str_to_dyn_error("get schema failed"))?;
            let label_ids = encode_storage_label(params.labels.as_ref(), schema.clone());
            let prop_ids = encode_storage_prop_key(params.props.as_ref(), schema.clone());
            let filter = params.filter.clone();
            let partitions: Vec<PartitionId> =
                partitions.iter().map(|pid| *pid as PartitionId).collect();
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
        &self,
        ids: &[ID],
        params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .ok_or(str_to_dyn_error("get snapshot_id failed"))?
            .as_i64()
            .map_err(|e| str_to_dyn_error(&e.to_string()))? as SnapshotId;
        let schema = store
            .get_schema(si)
            .ok_or(str_to_dyn_error("get schema failed"))?;
        let prop_ids = encode_storage_prop_key(params.props.as_ref(), schema.clone());
        let filter = params.filter.clone();
        let partition_label_vertex_ids =
            get_partition_label_vertex_ids(ids, self.partition_manager.clone());
        let result = store
            .get_vertex_properties(si, partition_label_vertex_ids, prop_ids.as_ref())
            .map(move |v| to_runtime_vertex(&v));

        Ok(filter_limit!(result, filter, None))
    }

    fn get_edge(
        &self,
        _ids: &[ID],
        _params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>> {
        // TODO(bingqing): adapt get_edge when graphscope support this
        Err(str_to_dyn_error(
            "GraphScope storage does not support get_edge for now",
        ))
    }

    fn prepare_explore_vertex(
        &self,
        direction: Direction,
        params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Statement<ID, Vertex>>> {
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let store = self.store.clone();
        let partition_manager = self.partition_manager.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .ok_or(str_to_dyn_error("get snapshot_id failed"))?
            .as_i64()
            .map_err(|e| str_to_dyn_error(&e.to_string()))? as SnapshotId;
        let schema = store
            .get_schema(si)
            .ok_or(str_to_dyn_error("get schema failed"))?;
        let edge_label_ids = encode_storage_label(params.labels.as_ref(), schema.clone());

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
        &self,
        direction: Direction,
        params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Statement<ID, Edge>>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .ok_or(str_to_dyn_error("get snapshot_id failed"))?
            .as_i64()
            .map_err(|e| str_to_dyn_error(&e.to_string()))? as SnapshotId;
        let partition_manager = self.partition_manager.clone();
        let schema = store
            .get_schema(si)
            .ok_or(str_to_dyn_error("get schema failed"))?;
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let edge_label_ids = encode_storage_label(params.labels.as_ref(), schema.clone());
        let prop_ids = encode_storage_prop_key(params.props.as_ref(), schema.clone());

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
    let label = Some(Label::Id(v.get_label_id() as RuntimeLabelId));
    let properties = v
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    let details = DefaultDetails::new_with_prop(id, label.clone().unwrap(), properties);
    Vertex::new(id, label, details)
}

#[inline]
fn to_runtime_edge<E: StoreEdge>(e: &E) -> Edge {
    let id = e.get_edge_id() as ID;
    let label = Some(Label::Id(e.get_label_id() as RuntimeLabelId));
    let properties = e
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    let mut edge = Edge::new(
        id,
        label.clone(),
        e.get_src_id() as ID,
        e.get_dst_id() as ID,
        DynDetails::new(DefaultDetails::new_with_prop(
            id,
            label.unwrap(),
            properties,
        )),
    );
    edge.set_src_label(Label::Id(e.get_src_label_id() as RuntimeLabelId));
    edge.set_dst_label(Label::Id(e.get_dst_label_id() as RuntimeLabelId));
    edge
}

/// in maxgraph store, Option<Vec<PropId>>: None means we need all properties,
/// and Some means we need given properties (and Some(vec![]) means we do not need any property)
/// while in gaia, None means we do not need any properties,
/// and Some means we need given properties (and Some(vec![]) means we need all properties)
#[inline]
fn encode_storage_prop_key(
    prop_names: Option<&Vec<PropKey>>,
    schema: Arc<dyn Schema>,
) -> Option<Vec<PropId>> {
    if let Some(prop_names) = prop_names {
        if prop_names.is_empty() {
            None
        } else {
            Some(
                prop_names
                    .iter()
                    .map(|prop_key| match prop_key {
                        PropKey::Str(prop_name) => {
                            schema.get_prop_id(prop_name).unwrap_or(INVALID_PROP_ID)
                        }
                        PropKey::Id(prop_id) => (*prop_id),
                    })
                    .collect(),
            )
        }
    } else {
        Some(vec![])
    }
}

#[inline]
fn encode_storage_label(labels: &Vec<Label>, schema: Arc<dyn Schema>) -> Vec<LabelId> {
    labels
        .iter()
        .map(|label| match label {
            Label::Str(s) => schema.get_label_id(s).unwrap_or(INVALID_LABEL_ID),
            Label::Id(id) => *id as LabelId,
        })
        .collect::<Vec<LabelId>>()
}

#[inline]
fn encode_runtime_property(prop_id: PropId, prop_val: Property) -> (PropKey, Object) {
    let prop_key = PropKey::Id(prop_id);
    let prop_val = match prop_val {
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
    };
    (prop_key, prop_val)
}

/// Transform type of ids to PartitionLabeledVertexIds as required by graphscope store,
/// which consists of (PartitionId, Vec<(Option<LabelId>, Vec<VertexId>)>)
fn get_partition_label_vertex_ids(
    ids: &[ID],
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> Vec<PartitionLabeledVertexIds> {
    let mut partition_label_vid_map = HashMap::new();
    for vid in ids {
        let partition_id =
            graph_partition_manager.get_partition_id(*vid as VertexId) as PartitionId;
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
    id: ID,
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> Vec<PartitionVertexIds> {
    let partition_id = graph_partition_manager.get_partition_id(id as VertexId) as PartitionId;
    vec![(partition_id, vec![id as VertexId])]
}
