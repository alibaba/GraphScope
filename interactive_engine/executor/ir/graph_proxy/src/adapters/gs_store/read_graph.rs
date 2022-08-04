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
use std::fmt;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use dyn_type::{Object, Primitives};
use global_query::store_api::prelude::Property;
use global_query::store_api::{Edge as StoreEdge, LabelId, PartitionId, Vertex as StoreVertex, VertexId};
use global_query::store_api::{PropId, SnapshotId};
use global_query::{
    GlobalGraphQuery, GraphPartitionManager, PartitionLabeledVertexIds, PartitionVertexIds,
};
use graph_store::utils::IterList;
use ir_common::{KeyId, NameOrId};
use ir_common::{NameOrId as Label, OneOrMany};
use pegasus_common::downcast::*;

use crate::apis::graph::PKV;
use crate::apis::{
    from_fn, register_graph, DefaultDetails, Details, Direction, DynDetails, Edge, PropertyValue,
    QueryParams, ReadGraph, Statement, Vertex, ID,
};
use crate::utils::expr::eval_pred::EvalPred;
use crate::{filter_limit, limit_n};
use crate::{GraphProxyError, GraphProxyResult};

// Should be identical to the param_name given by compiler
const SNAPSHOT_ID: &str = "SID";
// This will refer to the latest graph
const DEFAULT_SNAPSHOT_ID: SnapshotId = SnapshotId::max_value() - 1;
// This represents the primary key of GraphScopeStore
const GS_STORE_PK: KeyId = 0;

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

impl<V, VI, E, EI> ReadGraph for GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| {
                    s.parse::<SnapshotId>()
                        .unwrap_or(DEFAULT_SNAPSHOT_ID)
                })
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_labels(params.labels.as_ref())?;
            let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
            let filter = params.filter.clone();
            let partitions: Vec<PartitionId> = partitions
                .iter()
                .map(|pid| *pid as PartitionId)
                .collect();

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
                .map(move |v| to_runtime_vertex(v, prop_ids.clone()));
            Ok(filter_limit!(result, filter, None))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, label_id: &NameOrId, primary_key: &PKV, _params: &QueryParams,
    ) -> GraphProxyResult<Option<Vertex>> {
        // get_vertex_id_by_primary_keys() is a global query function, that is,
        // you can query vertices (with only vertex id) by pks on any graph partitions (not matter locally or remotely).
        // To guarantee the correctness (i.e., avoid duplication results), only worker 0 is assigned for query.
        if pegasus::get_current_worker().index == 0 {
            let store_label_id = encode_storage_label(label_id)?;
            let store_indexed_values = match primary_key {
                OneOrMany::One(pkv) => {
                    vec![encode_store_prop_val(pkv[0].1.clone())]
                }
                OneOrMany::Many(pkvs) => pkvs
                    .iter()
                    .map(|(_pk, value)| encode_store_prop_val(value.clone()))
                    .collect(),
            };

            if let Some(vid) = self
                .partition_manager
                .get_vertex_id_by_primary_keys(store_label_id, store_indexed_values.as_ref())
            {
                Ok(Some(Vertex::new(vid as ID, Some(label_id.clone()), DynDetails::default())))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn scan_edge(&self, params: &QueryParams) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| {
                    s.parse::<SnapshotId>()
                        .unwrap_or(DEFAULT_SNAPSHOT_ID)
                })
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_labels(params.labels.as_ref())?;
            let filter = params.filter.clone();
            // TODO: Currently, if filter exists, we scan edges with all props for filtering, and then trim the edges by only preserving necessary props;
            // TODO: In the future, the filter will be pushed to the storage.
            let prop_ids =
                if filter.is_some() { None } else { encode_storage_prop_keys(params.columns.as_ref())? };
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

            if let Some(filter) = filter {
                let columns = params.columns.clone();
                let filter_result = result
                    .filter(move |e| filter.eval_bool(Some(e)).unwrap_or(false))
                    .map(move |e| edge_trim(e, columns.as_ref()));
                Ok(Box::new(filter_result))
            } else {
                Ok(Box::new(result))
            }
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
        let filter = params.filter.clone();
        let partition_label_vertex_ids =
            get_partition_label_vertex_ids(ids, self.partition_manager.clone());
        let result = store
            .get_vertex_properties(si, partition_label_vertex_ids.clone(), prop_ids.as_ref())
            .map(move |v| to_runtime_vertex(v, prop_ids.clone()));

        Ok(filter_limit!(result, filter, None))
    }

    fn get_edge(
        &self, _ids: &[ID], _params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        // TODO(bingqing): adapt get_edge when graphscope support this
        Err(GraphProxyError::query_store_error("GraphScope storage does not support get_edge for now"))?
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Vertex>>> {
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let store = self.store.clone();
        let partition_manager = self.partition_manager.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let edge_label_ids = encode_storage_labels(params.labels.as_ref())?;

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_id(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_vertex_ids(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_vertex_ids(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::Both => {
                    let mut iters = vec![];
                    let out_iter = store.get_out_vertex_ids(
                        si,
                        vec![src_id.clone()],
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        limit.clone().unwrap_or(0),
                    );
                    iters.push(out_iter);
                    let in_iter = store.get_in_vertex_ids(
                        si,
                        vec![src_id],
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
            let iter_list = IterList::new(iters).map(move |v| to_empty_vertex(&v));
            Ok(filter_limit!(iter_list, filter, None))
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Edge>>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let partition_manager = self.partition_manager.clone();
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let edge_label_ids = encode_storage_labels(params.labels.as_ref())?;
        // TODO: Currently, if filter exists, we scan edges with all props for filtering, and then trim the edges by only preserving necessary props;
        // TODO: In the future, the filter will be pushed to the storage.
        let prop_ids =
            if filter.is_some() { None } else { encode_storage_prop_keys(params.columns.as_ref())? };
        let columns = params.columns.clone();

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_id(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_edges(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    None,
                    None,
                    prop_ids.as_ref(),
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_edges(
                    si,
                    vec![src_id],
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
                        vec![src_id.clone()],
                        edge_label_ids.as_ref(),
                        None,
                        None,
                        prop_ids.as_ref(),
                        limit.clone().unwrap_or(0),
                    );
                    iter.push(out_iter);
                    let in_iter = store.get_in_edges(
                        si,
                        vec![src_id],
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
            if let Some(filter) = filter.clone() {
                let columns = columns.clone();
                let filter_result = iter_list
                    .filter(move |e| filter.eval_bool(Some(e)).unwrap_or(false))
                    .map(move |e| edge_trim(e, columns.as_ref()));
                Ok(Box::new(filter_result))
            } else {
                Ok(Box::new(iter_list))
            }
        });
        Ok(stmt)
    }

    fn get_primary_key(&self, id: &ID) -> GraphProxyResult<Option<PKV>> {
        let store = self.store.clone();
        let outer_id = store.translate_vertex_id(*id as VertexId);
        let pk_val = Object::from(outer_id);
        Ok(Some((GS_STORE_PK.into(), pk_val).into()))
    }
}

#[inline]
fn to_runtime_vertex<V>(v: V, prop_keys: Option<Vec<PropId>>) -> Vertex
where
    V: 'static + StoreVertex,
{
    let id = v.get_id() as ID;
    let label = encode_runtime_v_label(&v);
    let details = LazyVertexDetails::new(v, prop_keys);
    Vertex::new(id, Some(label), DynDetails::new(details))
}

#[inline]
fn to_empty_vertex<V: StoreVertex>(v: &V) -> Vertex {
    let id = v.get_id() as ID;
    let label = encode_runtime_v_label(v);
    Vertex::new(id, Some(label), DynDetails::default())
}

#[inline]
fn to_runtime_edge<E: StoreEdge>(e: &E) -> Edge {
    // TODO: LazyEdgeDetails
    let id = e.get_edge_id() as ID;
    let label = encode_runtime_e_label(e);
    let properties = e
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    // TODO: new an edge with with_from_src()
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

#[inline]
fn edge_trim(mut edge: Edge, columns: Option<&Vec<NameOrId>>) -> Edge {
    if let Some(columns) = columns {
        if columns.is_empty() {
            // vec![] means all properties are needed, and do nothing
        } else {
            let details = edge.get_details_mut();
            let mut trimmed_details = HashMap::new();
            for column in columns {
                trimmed_details.insert(
                    column.clone(),
                    details
                        .get_property(column)
                        .map(|p| p.try_to_owned())
                        .unwrap_or(None)
                        .unwrap_or(Object::None),
                );
            }
            *details = DynDetails::new(DefaultDetails::new(trimmed_details));
        }
    } else {
        // None means no properties are needed
        let details = edge.get_details_mut();
        *details = DynDetails::new(DefaultDetails::default());
    }
    edge
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, in graphscope store, None means we need all properties,
    // and Some(vec![]) means we do not need any property
    prop_keys: Option<Vec<PropId>>,
    inner: AtomicPtr<V>,
}

impl<V> LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    pub fn new(v: V, prop_keys: Option<Vec<PropId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyVertexDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_vertex_ptr(&self) -> Option<*mut V> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl<V> fmt::Debug for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("gs_store LazyVertexDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<V> Details for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    // TODO: consider the situation when push `props` down to groot
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Id(key) = key {
            if let Some(ptr) = self.get_vertex_ptr() {
                unsafe {
                    (*ptr)
                        .get_property(*key as PropId)
                        .map(|prop| PropertyValue::Owned(encode_runtime_prop_val(prop)))
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_name in gs_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        let mut all_props = HashMap::new();
        if self.prop_keys.is_none() {
            // the case of get_all_properties from vertex;
            if let Some(ptr) = self.get_vertex_ptr() {
                unsafe {
                    all_props = (*ptr)
                        .get_properties()
                        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
                        .collect();
                }
            } else {
                return None;
            }
        } else {
            let prop_keys = self.prop_keys.as_ref().unwrap();
            // the case of get_all_properties with prop_keys pre-specified
            for key in prop_keys.iter() {
                let key = NameOrId::Id(*key as KeyId);
                if let Some(prop) = self.get_property(&key) {
                    all_props.insert(key.clone(), prop.try_to_owned().unwrap());
                } else {
                    all_props.insert(key.clone(), Object::None);
                }
            }
        }
        Some(all_props)
    }

    fn insert_property(&mut self, key: NameOrId, _value: Object) {
        if let NameOrId::Id(key) = key {
            if let Some(prop_keys) = self.prop_keys.as_mut() {
                prop_keys.push(key as PropId);
            }
        } else {
            info!("Have not support insert property by prop_name in gs_store yet");
        }
    }
}

impl<V> AsAny for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<V> Drop for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

// TODO: make this identical in GAIA and GlobalGraphQuery
/// in graphscope store, Option<Vec<PropId>>: None means we need all properties,
/// and Some means we need given properties (and Some(vec![]) means we do not need any property)
/// while in ir, None means we do not need any properties,
/// and Some means we need given properties (and Some(vec![]) means we need all properties)
#[inline]
fn encode_storage_prop_keys(prop_names: Option<&Vec<NameOrId>>) -> GraphProxyResult<Option<Vec<PropId>>> {
    if let Some(prop_names) = prop_names {
        if prop_names.is_empty() {
            Ok(None)
        } else {
            let encoded_prop_ids = prop_names
                .iter()
                .map(|prop_key| match prop_key {
                    NameOrId::Str(_) => Err(GraphProxyError::query_store_error(
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
fn encode_storage_labels(labels: &Vec<Label>) -> GraphProxyResult<Vec<LabelId>> {
    labels
        .iter()
        .map(|label| encode_storage_label(label))
        .collect::<Result<Vec<LabelId>, _>>()
}

#[inline]
fn encode_storage_label(label: &NameOrId) -> GraphProxyResult<LabelId> {
    match label {
        Label::Str(_) => {
            Err(GraphProxyError::query_store_error("encode storage label error, should provide label_id"))
        }
        Label::Id(id) => Ok(*id as LabelId),
    }
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
                        Primitives::ULLong(_) => Property::ListLong(
                            vec.into_iter()
                                .map(|i| i.as_u128().unwrap() as i64)
                                .collect(),
                        ),
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
fn get_partition_vertex_id(
    id: ID, graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> PartitionVertexIds {
    let partition_id = graph_partition_manager.get_partition_id(id as VertexId) as PartitionId;
    (partition_id, vec![id as VertexId])
}
