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
use std::sync::Arc;

use ahash::HashMap;
use dyn_type::{BorrowObject, DateTimeFormats, Object, Primitives};
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
use crate::apis::{
    from_fn, ClusterInfo, Details, Direction, DynDetails, Edge, PropertyValue, QueryParams, ReadGraph,
    Statement, Vertex, ID,
};
use crate::errors::{GraphProxyError, GraphProxyResult};
use crate::{filter_limit, filter_sample_limit, limit_n, sample_limit};

lazy_static! {
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
}

const CSR_STORE_PK: KeyId = 0;
pub const LABEL_SHIFT_BITS: usize =
    8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<StoreLabelId>());

pub fn create_csr_store(cluster_info: Arc<dyn ClusterInfo>) -> Arc<CSRStore> {
    lazy_static::initialize(&CSR);
    Arc::new(CSRStore { store: &CSR, cluster_info })
}

pub struct CSRStore {
    store: &'static CsrDB<usize, usize>,
    cluster_info: Arc<dyn ClusterInfo>,
}

fn _init_csr() -> CsrDB<usize, usize> {
    CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID, None).unwrap()
}

impl ReadGraph for CSRStore {
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let label_ids = encode_storage_label(&params.labels);
        let props = params.columns.clone();

        let worker_index = self.cluster_info.get_worker_index()?;
        let workers_num = self.cluster_info.get_local_worker_num()?;

        let result = self
            .store
            .get_partitioned_vertices(label_ids.as_ref(), worker_index, workers_num)
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

        let worker_index = self.cluster_info.get_worker_index()?;
        let workers_num = self.cluster_info.get_local_worker_num()?;
        let partition_id = self.store.partition as u8;

        let result = self
            .store
            .get_partitioned_edges(label_ids.as_ref(), worker_index, workers_num)
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

    fn count_vertex(&self, params: &QueryParams) -> GraphProxyResult<u64> {
        if params.filter.is_some() {
            // the filter cannot be pushed down to store,
            // so we need to scan all vertices with filter and then count
            Ok(self.scan_vertex(params)?.count() as u64)
        } else {
            let worker_index = self.cluster_info.get_worker_index()?;
            let workers_num = self.cluster_info.get_local_worker_num()?;
            if worker_index % workers_num == 0 {
                let label_ids = encode_storage_label(&params.labels);
                let count = self
                    .store
                    .count_all_vertices(label_ids.as_ref());
                Ok(count as u64)
            } else {
                Ok(0)
            }
        }
    }

    fn count_edge(&self, params: &QueryParams) -> GraphProxyResult<u64> {
        if params.filter.is_some() {
            Ok(self.scan_edge(params)?.count() as u64)
        } else {
            let worker_index = self.cluster_info.get_worker_index()?;
            let workers_num = self.cluster_info.get_local_worker_num()?;
            if worker_index % workers_num == 0 {
                let label_ids = encode_storage_label(&params.labels);
                let count = self.store.count_all_edges(label_ids.as_ref());
                Ok(count as u64)
            } else {
                Ok(0)
            }
        }
    }
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
    inner: LocalVertex<'static, DefaultId, DefaultId>,
}

impl_as_any!(LazyVertexDetails);

impl LazyVertexDetails {
    pub fn new(v: LocalVertex<'static, DefaultId, DefaultId>, prop_keys: Option<Vec<NameOrId>>) -> Self {
        LazyVertexDetails { prop_keys, inner: v }
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
            if key == "id" {
                let mask = (1_usize << LABEL_SHIFT_BITS) - 1;
                let original_id = (self.inner.get_id() & mask) as i64;
                Some(PropertyValue::Owned(Object::Primitive(Primitives::Long(original_id))))
            } else {
                self.inner
                    .get_property(key)
                    .map(|prop| to_property_value(prop))
            }
        } else {
            info!("Have not support getting property by prop_id in exp_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        // the case of get_all_properties from vertex;
        if let Some(prop_key_vals) = self.inner.get_all_properties() {
            let mut all_props: HashMap<NameOrId, Object> = prop_key_vals
                .into_iter()
                .map(|(prop_key, prop_val)| (prop_key.into(), to_object(prop_val)))
                .collect();
            let mask = (1_usize << LABEL_SHIFT_BITS) - 1;
            let original_id = (self.inner.get_id() & mask) as i64;
            all_props
                .insert(NameOrId::Str("id".to_string()), Object::Primitive(Primitives::Long(original_id)));
            Some(all_props)
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
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
    inner: LocalEdge<'static, DefaultId, DefaultId>,
}

impl_as_any!(LazyEdgeDetails);

impl LazyEdgeDetails {
    pub fn new(e: LocalEdge<'static, DefaultId, DefaultId>, prop_keys: Option<Vec<NameOrId>>) -> Self {
        LazyEdgeDetails { prop_keys, inner: e }
    }
}

impl fmt::Debug for LazyEdgeDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyEdgeDetails")
            .field("prop_keys", &self.prop_keys)
            // .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyEdgeDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Str(key) = key {
            self.inner
                .get_property(key)
                .map(|prop| to_property_value(prop))
        } else {
            info!("Have not support getting property by prop_id in experiments store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        if let Some(prop_key_vals) = self.inner.get_all_properties() {
            let all_props: HashMap<NameOrId, Object> = prop_key_vals
                .into_iter()
                .map(|(prop_key, prop_val)| (prop_key.into(), to_object(prop_val)))
                .collect();
            Some(all_props)
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
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
        RefItem::Date(v) => {
            if let Some(date) = encode_date(v) {
                Object::DateFormat(DateTimeFormats::Date(date))
            } else {
                Object::None
            }
        }
        RefItem::DateTime(v) => {
            if let Some(date_time) = encode_datetime(v) {
                Object::DateFormat(DateTimeFormats::DateTime(date_time))
            } else {
                Object::None
            }
        }
        RefItem::String(v) => Object::String(v.clone()),
        _ => Object::None,
    }
}

#[inline]
fn to_property_value<'a>(ref_item: RefItem<'a>) -> PropertyValue {
    match ref_item {
        RefItem::Int32(v) => BorrowObject::Primitive(Primitives::Integer(*v)).into(),
        RefItem::UInt32(v) => {
            BorrowObject::Primitive(Primitives::Integer(i32::try_from(*v).unwrap())).into()
        }
        RefItem::Int64(v) => BorrowObject::Primitive(Primitives::Long(*v)).into(),
        RefItem::UInt64(v) => BorrowObject::Primitive(Primitives::Long(i64::try_from(*v).unwrap())).into(),
        RefItem::Double(v) => BorrowObject::Primitive(Primitives::Float(*v)).into(),
        RefItem::Date(v) => {
            if let Some(date) = encode_date(v) {
                Object::DateFormat(DateTimeFormats::Date(date)).into()
            } else {
                BorrowObject::None.into()
            }
        }
        RefItem::DateTime(v) => {
            if let Some(date_time) = encode_datetime(v) {
                Object::DateFormat(DateTimeFormats::DateTime(date_time)).into()
            } else {
                BorrowObject::None.into()
            }
        }
        RefItem::String(v) => BorrowObject::String(v).into(),
        _ => BorrowObject::None.into(),
    }
}

#[inline]
fn encode_date(date: &Date) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), date.day())
}

#[inline]
fn encode_datetime(datetime: &DateTime) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::from_timestamp_millis(datetime.to_i64())
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
