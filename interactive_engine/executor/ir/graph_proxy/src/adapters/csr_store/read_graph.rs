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

use std::convert::TryFrom;
use std::fmt;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use ahash::HashMap;
use dyn_type::{BorrowObject, Object, Primitives};
use ir_common::{KeyId, LabelId, NameOrId};
use mcsr::columns::RefItem;
use mcsr::date::Date;
use mcsr::date_time::DateTime;
use mcsr::graph_db::{GlobalCsrTrait, LocalEdge, LocalVertex};
use mcsr::graph_db_impl::CsrDB;
use mcsr::types::DefaultId;
use mcsr::types::LabelId as StoreLabelId;
use pegasus::configure_with_default;
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::apis::graph::PKV;
use crate::apis::partitioner::QueryPartitions;
use crate::apis::{
    from_fn, register_graph, Details, Direction, DynDetails, Edge, PropertyValue, QueryParams, ReadGraph,
    Statement, Vertex, ID,
};
use crate::errors::{GraphProxyError, GraphProxyResult};
use crate::{filter_limit, filter_sample_limit, limit_n, sample_limit};

lazy_static! {
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
    static ref GRAPH_PROXY: Arc<CSRStore> = initialize();
}

const CSR_STORE_PK: KeyId = 0;
pub const LABEL_SHIFT_BITS: usize =
    8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<StoreLabelId>());

pub struct CSRStore {
    store: &'static CsrDB<usize, usize>,
}

fn initialize() -> Arc<CSRStore> {
    lazy_static::initialize(&CSR);
    Arc::new(CSRStore { store: &CSR })
}

fn _init_csr() -> CsrDB<usize, usize> {
    CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID).unwrap()
}

impl ReadGraph for CSRStore {
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let label_ids = encode_storage_label(&params.labels);
        let props = params.columns.clone();
        let partitions = params
            .partitions
            .as_ref()
            .ok_or(GraphProxyError::query_store_error("Empty Partitions on CsrStore"))?;
        let (idx, num) = match partitions {
            QueryPartitions::WholePartitions(_) => (0, 1),
            QueryPartitions::PartialPartition(idx, num, _) => (*idx, *num),
        };

        let result = self
            .store
            .get_partitioned_vertices(label_ids.as_ref(), idx, num)
            .map(move |v| to_runtime_vertex(v, props.clone()));
        Ok(filter_sample_limit!(result, params.filter, params.sample_ratio, params.limit))
    }

    fn index_scan_vertex(
        &self, _label: LabelId, _primary_key: &PKV, _params: &QueryParams,
    ) -> GraphProxyResult<Option<Vertex>> {
        Err(GraphProxyError::unsupported_error(
            "Experiment storage does not support index_scan_vertex for now",
        ))?
    }

    fn scan_edge(&self, params: &QueryParams) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        let label_ids = encode_storage_label(&params.labels);
        let props = params.columns.clone();
        let partitions = params
            .partitions
            .as_ref()
            .ok_or(GraphProxyError::query_store_error("Empty Partitions on CsrStore"))?;
        let (idx, num) = match partitions {
            QueryPartitions::WholePartitions(_) => (0, 1),
            QueryPartitions::PartialPartition(idx, num, _) => (*idx, *num),
        };
        let partition_id = self.store.partition as u8;

        let result = self
            .store
            .get_partitioned_edges(label_ids.as_ref(), idx, num)
            .map(move |e| to_runtime_edge(e, None, props.clone(), partition_id));
        Ok(filter_sample_limit!(result, params.filter, params.sample_ratio, params.limit))
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(local_vertex) = self.store.get_vertex(*id as DefaultId) {
                let v = to_runtime_vertex(local_vertex, params.columns.clone());
                result.push(v);
            }
        }
        Ok(filter_limit!(result.into_iter(), params.filter, params.limit))
    }

    fn get_edge(
        &self, _ids: &[ID], _params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        Err(GraphProxyError::unsupported_error(
            "Experiment storage does not support index_scan_vertex for now",
        ))?
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Vertex>>> {
        let edge_label_ids = encode_storage_label(params.labels.as_ref());
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let graph = self.store;

        let stmt = from_fn(move |v: ID| {
            let iter = match direction {
                Direction::Out => graph.get_out_vertices(v as DefaultId, edge_label_ids.as_ref()),
                Direction::In => graph.get_in_vertices(v as DefaultId, edge_label_ids.as_ref()),
                Direction::Both => graph.get_both_vertices(v as DefaultId, edge_label_ids.as_ref()),
            }
            .map(move |v| to_empty_vertex(v));
            Ok(filter_limit!(iter, filter, limit))
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Edge>>> {
        let edge_label_ids = encode_storage_label(&params.labels);
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let graph = self.store;
        let props = params.columns.clone();

        let stmt = from_fn(move |v: ID| {
            let props = props.clone();
            let partition_id = graph.partition as u8;
            let iter = match direction {
                Direction::Out => graph.get_out_edges(v as DefaultId, edge_label_ids.as_ref()),
                Direction::In => graph.get_in_edges(v as DefaultId, edge_label_ids.as_ref()),
                Direction::Both => graph.get_both_edges(v as DefaultId, edge_label_ids.as_ref()),
            }
            .map(move |e| to_runtime_edge(e, Some(v), props.clone(), partition_id));
            Ok(filter_limit!(iter, filter, limit))
        });
        Ok(stmt)
    }

    fn get_primary_key(&self, id: &ID) -> GraphProxyResult<Option<PKV>> {
        let outer_id = (*id << LABEL_SHIFT_BITS) >> LABEL_SHIFT_BITS;
        let pk_val = Object::from(outer_id);
        Ok(Some((CSR_STORE_PK.into(), pk_val).into()))
    }
}

#[allow(dead_code)]
pub fn create_csr_store() {
    lazy_static::initialize(&GRAPH_PROXY);
    register_graph(GRAPH_PROXY.clone());
}

#[inline]
fn to_runtime_vertex(
    v: LocalVertex<'static, DefaultId, DefaultId>, prop_keys: Option<Vec<NameOrId>>,
) -> Vertex {
    // For vertices, we query properties via vid
    let id = v.get_id() as ID;
    let label = encode_runtime_label(v.get_label());
    let details = LazyVertexDetails::new(v, prop_keys);
    Vertex::new(id, Some(label), DynDetails::lazy(details))
}

#[inline]
fn to_empty_vertex(v: LocalVertex<'static, DefaultId, DefaultId>) -> Vertex {
    let id = v.get_id() as ID;
    let label = encode_runtime_label(v.get_label());
    Vertex::new(id, Some(label), DynDetails::Empty)
}

#[inline]
fn to_runtime_edge(
    e: LocalEdge<'static, DefaultId, DefaultId>, v: Option<ID>, prop_keys: Option<Vec<NameOrId>>,
    partition_id: u8,
) -> Edge {
    let src_id = e.get_src_id() as i64;
    let dst_id = e.get_dst_id() as i64;
    let src_label = e.get_src_label() as i64;
    let dst_label = e.get_dst_label() as i64;
    let label = e.get_label();
    let offset = e.get_offset() as i64;
    let edge_id = ((partition_id as i64) << 56)
        + (src_label << 48)
        + ((label as i64) << 40)
        + (dst_label << 32)
        + offset;
    let mut e = if v.is_none() || v.unwrap() == src_id {
        Edge::new(
            edge_id,
            Some(encode_runtime_label(label)),
            src_id,
            dst_id,
            DynDetails::lazy(LazyEdgeDetails::new(e, prop_keys)),
        )
    } else {
        Edge::with_from_src(
            edge_id,
            Some(encode_runtime_label(label)),
            src_id,
            dst_id,
            false,
            DynDetails::lazy(LazyEdgeDetails::new(e, prop_keys)),
        )
    };
    e.set_src_label(encode_runtime_label(src_label as StoreLabelId));
    e.set_dst_label(encode_runtime_label(dst_label as StoreLabelId));
    e
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
struct LazyVertexDetails {
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<LocalVertex<'static, DefaultId, DefaultId>>,
}

impl_as_any!(LazyVertexDetails);

impl LazyVertexDetails {
    pub fn new(v: LocalVertex<'static, DefaultId, DefaultId>, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyVertexDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_vertex_ptr(&self) -> Option<*mut LocalVertex<'static, DefaultId, DefaultId>> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl fmt::Debug for LazyVertexDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyVertexDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyVertexDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Str(key) = key {
            if let Some(ptr) = self.get_vertex_ptr() {
                unsafe {
                    if key == "id" {
                        let mask = (1_usize << LABEL_SHIFT_BITS) - 1;
                        let original_id = ((*ptr).get_id() & mask) as i64;
                        Some(PropertyValue::Owned(Object::Primitive(Primitives::Long(original_id))))
                    } else {
                        (*ptr)
                            .get_property(key)
                            .map(|prop| PropertyValue::Borrowed(to_borrow_object(prop)))
                    }
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_id in exp_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        // the case of get_all_properties from vertex;
        let props = if let Some(ptr) = self.get_vertex_ptr() {
            unsafe {
                if let Some(prop_key_vals) = (*ptr).get_all_properties() {
                    let mut all_props: HashMap<NameOrId, Object> = prop_key_vals
                        .into_iter()
                        .map(|(prop_key, prop_val)| (prop_key.into(), to_object(prop_val)))
                        .collect();
                    let mask = (1_usize << LABEL_SHIFT_BITS) - 1;
                    let original_id = ((*ptr).get_id() & mask) as i64;
                    all_props.insert(
                        NameOrId::Str("id".to_string()),
                        Object::Primitive(Primitives::Long(original_id)),
                    );
                    Some(all_props)
                } else {
                    None
                }
            }
        } else {
            None
        };
        props
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
    }
}

impl Drop for LazyVertexDetails {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

/// LazyEdgeDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyEdgeDetails need to be shuffled.
#[allow(dead_code)]
struct LazyEdgeDetails {
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property,
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<LocalEdge<'static, DefaultId, DefaultId>>,
}

impl_as_any!(LazyEdgeDetails);

impl LazyEdgeDetails {
    pub fn new(e: LocalEdge<'static, DefaultId, DefaultId>, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(e));
        LazyEdgeDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_edge_ptr(&self) -> Option<*mut LocalEdge<'static, DefaultId, DefaultId>> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl fmt::Debug for LazyEdgeDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyEdgeDetails")
            .field("prop_keys", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyEdgeDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Str(key) = key {
            let ptr = self.get_edge_ptr();
            if let Some(ptr) = ptr {
                unsafe {
                    (*ptr)
                        .get_property(key)
                        .map(|prop| PropertyValue::Borrowed(to_borrow_object(prop)))
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_id in experiments store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        // the case of get_all_properties from vertex;
        let props = if let Some(ptr) = self.get_edge_ptr() {
            unsafe {
                if let Some(prop_key_vals) = (*ptr).get_all_properties() {
                    let all_props: HashMap<NameOrId, Object> = prop_key_vals
                        .into_iter()
                        .map(|(prop_key, prop_val)| (prop_key.into(), to_object(prop_val)))
                        .collect();
                    Some(all_props)
                } else {
                    None
                }
            }
        } else {
            None
        };
        props
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
    }
}

impl Drop for LazyEdgeDetails {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

#[inline]
fn to_object<'a>(ref_item: RefItem<'a>) -> Object {
    match ref_item {
        RefItem::Int32(v) => Object::Primitive(Primitives::Integer(*v)),
        RefItem::UInt32(v) => Object::Primitive(Primitives::Integer(i32::try_from(*v).unwrap())),
        RefItem::Int64(v) => Object::Primitive(Primitives::Long(*v)),
        RefItem::UInt64(v) => Object::Primitive(Primitives::Long(i64::try_from(*v).unwrap())),
        RefItem::Double(v) => Object::Primitive(Primitives::Float(*v)),
        RefItem::Date(v) => Object::Primitive(Primitives::Long(encode_date(v))),
        RefItem::DateTime(v) => Object::Primitive(Primitives::Long(encode_datetime(v))),
        RefItem::String(v) => Object::String(v.clone()),
        _ => Object::None,
    }
}

#[inline]
fn to_borrow_object<'a>(ref_item: RefItem<'a>) -> BorrowObject<'a> {
    match ref_item {
        RefItem::Int32(v) => BorrowObject::Primitive(Primitives::Integer(*v)),
        RefItem::UInt32(v) => BorrowObject::Primitive(Primitives::Integer(i32::try_from(*v).unwrap())),
        RefItem::Int64(v) => BorrowObject::Primitive(Primitives::Long(*v)),
        RefItem::UInt64(v) => BorrowObject::Primitive(Primitives::Long(i64::try_from(*v).unwrap())),
        RefItem::Double(v) => BorrowObject::Primitive(Primitives::Float(*v)),
        RefItem::Date(v) => BorrowObject::Primitive(Primitives::Long(encode_date(v))),
        RefItem::DateTime(v) => BorrowObject::Primitive(Primitives::Long(encode_datetime(v))),
        RefItem::String(v) => BorrowObject::String(v),
        _ => BorrowObject::None,
    }
}

#[inline]
fn encode_date(date: &Date) -> i64 {
    date.year() as i64 * 10000000000000
        + date.month() as i64 * 100000000000
        + date.day() as i64 * 1000000000
}

#[inline]
fn encode_datetime(datetime: &DateTime) -> i64 {
    datetime.year() as i64 * 10000000000000
        + datetime.month() as i64 * 100000000000
        + datetime.day() as i64 * 1000000000
        + datetime.hour() as i64 * 10000000
        + datetime.minute() as i64 * 100000
        + datetime.second() as i64 * 1000
        + datetime.millisecond() as i64
}

#[inline]
fn encode_runtime_label(l: StoreLabelId) -> LabelId {
    l as LabelId
}

/// Transform string-typed labels into a id-typed labels.
/// `is_true_label` records whether the label is an actual label, or already transformed into
/// an id-type.
#[inline]
fn encode_storage_label(labels: &Vec<LabelId>) -> Option<Vec<StoreLabelId>> {
    if labels.is_empty() {
        None
    } else {
        Some(
            labels
                .iter()
                .map(|label| *label as StoreLabelId)
                .collect::<Vec<StoreLabelId>>(),
        )
    }
}
