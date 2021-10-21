//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use maxgraph_store::api::prelude::*;
use maxgraph_store::schema::prelude::{PropId, DataType};
use maxgraph_store::api::graph_schema::Schema;
use std::sync::Arc;
use std::ffi::{CStr, CString};
use itertools::Itertools;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use dataflow::message::{PropertyEntity, ValuePayload};
use dataflow::message::primitive::Read;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;

pub type GraphHandle = *const ::libc::c_void;
pub type GraphId = i64;
type GetVertexIterator = *const ::libc::c_void;
type GetAllVerticesIterator = *const ::libc::c_void;
type VertexHandle = i64;
pub(crate) type PropertyId = i32;
pub(crate) type FFILabelId = i32;
pub(crate) type FFIPartitionId = i32;
type PropertiesIterator = *const ::libc::c_void;
type OutEdgeIterator = *const ::libc::c_void;
type InEdgeIterator = *const ::libc::c_void;
type GetAllEdgesIterator = *const ::libc::c_void;
pub type SchemaHandle = *const ::libc::c_void;

/// -1 means failed, 0 means success
type FFIState = i32;

const STATE_SUCCESS: i32 = 0;
const STATE_FAILED: i32 = -1;


#[repr(C)]
#[derive(Debug)]
pub struct EdgeHandle {
    src: i64,
    dst: i64,
    offset: i64,
}

impl EdgeHandle {
    pub fn new() -> Self {
        EdgeHandle {
            src: 0,
            dst: 0,
            offset: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum PropertyType {
    Bool = 1,
    Char = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Bytes = 9,
    IntList = 10,
    LongList = 11,
    FloatList = 12,
    DoubleList = 13,
    StringList = 14,
}

#[repr(C)]
pub struct NativeProperty {
    id: i32,
    r#type: PropertyType,
    data: *const u8,
    len: i64,
}

#[repr(C)]
pub struct WriteNativeProperty {
    id: i32,
    r#type: PropertyType,
    data: *const u8,
    len: i64,
}

extern {
    fn get_graph_handle(graph_id: GraphId, channel_num: FFIPartitionId) -> GraphHandle;
    fn free_graph_handle(handle: GraphHandle);

    fn get_vertices(graph: GraphHandle, partition_id: FFIPartitionId, label: *const FFILabelId, ids: *const VertexId, count: i32) -> GetVertexIterator;
    fn free_get_vertex_iterator(iter: GetVertexIterator);
    fn get_vertices_next(iter: GetVertexIterator, v_out: *mut VertexHandle) -> FFIState;

    fn get_all_vertices(graph: GraphHandle, partition_id: FFIPartitionId, labels: *const FFILabelId, label_count: i32, limit: i64) -> GetAllVerticesIterator;
    fn free_get_all_vertices_iterator(iter: GetAllVerticesIterator);
    fn get_all_vertices_next(iter: GetAllVerticesIterator, v_out: *mut VertexHandle) -> FFIState;

    fn get_vertex_id(graph: GraphHandle, v: VertexHandle) -> VertexId;
    fn get_vertex_label(graph: GraphHandle, v: VertexHandle) -> LabelId;
    fn get_vertex_property(graph: GraphHandle, v: VertexHandle, id: PropertyId, p_out: *mut NativeProperty) -> FFIState;
    fn get_vertex_properties(graph: GraphHandle, v: VertexHandle) -> PropertiesIterator;

    fn free_properties_iterator(iter: PropertiesIterator);
    fn properties_next(iter: PropertiesIterator, p_out: *mut NativeProperty) -> FFIState;

    fn get_out_edges(graph: GraphHandle, partition_id: FFIPartitionId, src_id: VertexId, labels: *const FFILabelId, label_count: i32, limit: i64) -> OutEdgeIterator;
    fn free_out_edge_iterator(iter: OutEdgeIterator);
    fn out_edge_next(iter: OutEdgeIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_in_edges(graph: GraphHandle, partition_id: FFIPartitionId, dst_id: VertexId, labels: *const FFILabelId, label_count: i32, limit: i64) -> InEdgeIterator;
    fn free_in_edge_iterator(iter: InEdgeIterator);
    fn in_edge_next(iter: InEdgeIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_all_edges(graph: GraphHandle, partition_id: FFIPartitionId, labels: *const FFILabelId, label_count: i32, limit: i64) -> GetAllEdgesIterator;
    fn free_get_all_edges_iterator(iter: GetAllEdgesIterator);
    fn get_all_edges_next(iter: GetAllEdgesIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_edge_src_id(graph: GraphHandle, e: *const EdgeHandle) -> VertexId;
    fn get_edge_dst_id(graph: GraphHandle, e: *const EdgeHandle) -> VertexId;
    fn get_edge_id(graph: GraphHandle, e: *const EdgeHandle) -> EdgeId;
    fn get_edge_src_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_dst_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_property(graph: GraphHandle, e: *const EdgeHandle, id: PropertyId, p_out: *mut NativeProperty) -> FFIState;
    fn get_edge_properties(graph: GraphHandle, e: *const EdgeHandle) -> PropertiesIterator;

    fn get_property_as_bool(property: *const NativeProperty, out: *mut bool) -> FFIState;
    fn get_property_as_char(property: *const NativeProperty, out: *mut u8) -> FFIState;
    fn get_property_as_short(property: *const NativeProperty, out: *mut i16) -> FFIState;
    fn get_property_as_int(property: *const NativeProperty, out: *mut i32) -> FFIState;
    fn get_property_as_long(property: *const NativeProperty, out: *mut i64) -> FFIState;
    fn get_property_as_float(property: *const NativeProperty, out: *mut f32) -> FFIState;
    fn get_property_as_double(property: *const NativeProperty, out: *mut f64) -> FFIState;
    fn get_property_as_string(property: *const NativeProperty, out: *mut *const u8, out_len: *mut i32) -> FFIState;
    fn get_property_as_bytes(property: *const NativeProperty, out: *mut *const u8, out_len: *mut i32) -> FFIState;
    fn get_property_as_int_list(property: *const NativeProperty, out: *mut *const i32, out_len: *mut i32) -> FFIState;
    fn get_property_as_long_list(property: *const NativeProperty, out: *mut *const i64, out_len: *mut i32) -> FFIState;
    fn get_property_as_float_list(property: *const NativeProperty, out: *mut *const f32, out_len: *mut i32) -> FFIState;
    fn get_property_as_double_list(property: *const NativeProperty, out: *mut *const f64, out_len: *mut i32) -> FFIState;
    fn get_property_as_string_list(property: *const NativeProperty, out: *mut *const *const u8, out_len: *mut *const i32, out_num: *mut i32) -> FFIState;

    fn free_property(p: *const NativeProperty);

    pub(crate) fn get_schema(graph: GraphHandle) -> SchemaHandle;
    fn get_property_id(schema: SchemaHandle, name: *const ::libc::c_char, out: *mut PropertyId) -> FFIState;
    fn get_property_type(schema: SchemaHandle, label: LabelId, id: PropertyId, out: *mut PropertyType) -> FFIState;
    fn get_property_name(schema: SchemaHandle, id: PropertyId, out: *const *const ::libc::c_char) -> FFIState;
    fn get_label_id(schema: SchemaHandle, name: *const ::libc::c_char, out: *mut LabelId) -> FFIState;
    fn get_label_name(schema: SchemaHandle, label: LabelId, out: *const *const ::libc::c_char) -> FFIState;
    fn free_schema(schema: SchemaHandle);

    fn free_string(s: *const ::libc::c_char);

    fn get_partition_id(graph: GraphHandle, vid: VertexId) -> i32;
    fn get_process_partition_list(graph: GraphHandle,
                                  partition_list: *const *const ::libc::c_int,
                                  partition_count: *const ::libc::c_int);
    fn free_partition_list(partition_list: *const ::libc::c_int);

    fn get_vertex_id_from_primary_key(graph: GraphHandle, label_id: LabelId, key: *const ::libc::c_char, internal_id: &mut VertexId, partition_id: &mut PartitionId) -> FFIState;

    fn get_outer_id(graph: GraphHandle, vid: VertexId) -> VertexId;

    fn test1(test: NativeProperty);
}

pub struct FFIGraphStore {
    graph: GraphHandle,
}

impl FFIGraphStore {
    pub fn new(graph_id: GraphId, worker_num: i32) -> Self {
        info!("create native graph {:?}", graph_id);
        let graph = unsafe { get_graph_handle(graph_id, worker_num) };
        info!("create native graph done {:?}", graph_id);
        FFIGraphStore {
            graph,
        }
    }

    pub fn get_partition_manager(&self) -> VineyardPartitionManager {
        VineyardPartitionManager::new(self.graph)
    }
}

impl Drop for FFIGraphStore {
    fn drop(&mut self) {
        unsafe { free_graph_handle(self.graph); }
    }
}

#[inline]
fn get_limit_value(limit: i64) -> i64 {
    if limit == 0 {
        i64::max_value()
    } else {
        limit
    }
}

impl GlobalGraphQuery for FFIGraphStore {
    type V = GlobalVertex;
    type E = FFIEdge;
    type VI = GlobalVertexIter;
    type EI = GlobalEdgeIter;

    fn get_out_vertex_ids(&self, si: i64, src_ids: Vec<(PartitionId, Vec<VertexId>)>,
                          edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                          dedup_prop_ids: Option<&Vec<u32>>, limit: usize) -> Box<Iterator<Item=(i64, Self::VI)>> {
        CommonEdgeQuery::new(self.graph, src_ids, edge_labels, limit as i64)
            .run(|graph, partition_id, src_id, labels, label_count, limit| {
                let iter = unsafe { get_out_edges(graph, partition_id, src_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                let ret = FFIGetOutVertexIdsIter::new(FFIOutEdgeIter::new(graph, iter));
                GlobalVertexIter::Out(ret)
            })
    }

    fn get_out_edges(&self, si: i64, src_ids: Vec<(PartitionId, Vec<VertexId>)>,
                     edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                     dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>,
                     limit: usize) -> Box<Iterator<Item=(VertexId, Self::EI)>> {
        CommonEdgeQuery::new(self.graph, src_ids, edge_labels, limit as i64)
            .run(|graph, partition_id, src_id, labels, label_count, limit| {
                let iter = unsafe { get_out_edges(graph, partition_id, src_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                let ret = FFIOutEdgeIter::new(graph, iter);
                GlobalEdgeIter::Out(ret)
            })
    }

    fn get_in_vertex_ids(&self, si: i64, dst_ids: Vec<(PartitionId, Vec<VertexId>)>,
                         edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                         dedup_prop_ids: Option<&Vec<u32>>, limit: usize) -> Box<Iterator<Item=(i64, Self::VI)>> {
        CommonEdgeQuery::new(self.graph, dst_ids, edge_labels, limit as i64)
            .run(|graph, partition_id, dst_id, labels, label_count, limit| {
                let iter = unsafe { get_in_edges(graph, partition_id, dst_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                let ret = FFIGetInVertexIdsIter::new(FFIInEdgeIter::new(graph, iter));
                GlobalVertexIter::In(ret)
            })
    }

    fn get_in_edges(&self, si: i64, dst_ids: Vec<(PartitionId, Vec<VertexId>)>,
                    edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                    dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>,
                    limit: usize) -> Box<Iterator<Item=(VertexId, Self::EI)>> {
        CommonEdgeQuery::new(self.graph, dst_ids, edge_labels, limit as i64)
            .run(|graph, partition_id, dst_id, labels, label_count, limit| {
                let iter = unsafe { get_in_edges(graph, partition_id, dst_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                let ret = FFIInEdgeIter::new(graph, iter);
                GlobalEdgeIter::In(ret)
            })
    }

    fn count_out_edges(&self, si: i64, src_ids: Vec<(PartitionId, Vec<VertexId>)>,
                       edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<Iterator<Item=(VertexId, usize)>> {
        CommonEdgeQuery::new(self.graph, src_ids, edge_labels, i64::max_value())
            .run(|graph, partition_id, src_id, labels, label_count, limit| {
                let iter = unsafe { get_out_edges(graph, partition_id, src_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                FFIOutEdgeIter::new(graph, iter).count()
            })
    }

    fn count_in_edges(&self, si: i64, dst_ids: Vec<(PartitionId, Vec<VertexId>)>,
                      edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<Iterator<Item=(i64, usize)>> {
        CommonEdgeQuery::new(self.graph, dst_ids, edge_labels, i64::max_value())
            .run(|graph, partition_id, dst_id, labels, label_count, limit| {
                let iter = unsafe { get_in_edges(graph, partition_id, dst_id, labels.as_ptr(), label_count, get_limit_value(limit)) };
                FFIInEdgeIter::new(graph, iter).count()
            })
    }

    fn get_vertex_properties(&self, si: i64, ids: Vec<(PartitionId, Vec<(Option<LabelId>, Vec<VertexId>)>)>, output_prop_ids: Option<&Vec<u32>>) -> Self::VI {
        let graph = self.graph;
        let iter = ids.into_iter().flat_map(move |(partition_id, ids)| {
            ids.into_iter().flat_map(move |(label, ids)| {
                if let Some(l) = label {
                    let label_val = l as FFILabelId;
                    let iter = unsafe { get_vertices(graph, partition_id as FFIPartitionId, &label_val as *const FFILabelId, ids.as_ptr(), ids.len() as i32) };
                    FFIGetVertexIter::new(graph, iter)
                } else {
                    let iter = unsafe { get_vertices(graph, partition_id as FFIPartitionId, std::ptr::null(), ids.as_ptr(), ids.len() as i32) };
                    FFIGetVertexIter::new(graph, iter)
                }
            })
        });
        GlobalVertexIter::Normal(Box::new(iter))
    }

    fn get_edge_properties(&self, si: i64, ids: Vec<(u32, Vec<(Option<u32>, Vec<i64>)>)>, output_prop_ids: Option<&Vec<u32>>) -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(&self, si: i64, labels_ref: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::VI {
        let graph = self.graph;
        let labels: Vec<FFILabelId> = labels_ref.iter().map(|v| *v as FFILabelId).collect();
        let label_count = labels.len() as i32;
        let iter = partition_ids.clone().into_iter().map(|v| v as FFIPartitionId).flat_map(move |partition_id| {
            let curr_labels = labels.clone();
            let iter = unsafe { get_all_vertices(graph, partition_id, curr_labels.as_ptr(), label_count, get_limit_value(limit as i64)) };
            FFIGetAllVerticesIter::new(graph, iter)
        });
        GlobalVertexIter::Normal(Box::new(iter))
    }

    fn get_all_edges(&self, si: i64, labels_ref: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::EI {
        let graph = self.graph;
        let labels: Vec<FFILabelId> = labels_ref.iter().map(|v| *v as FFILabelId).collect();
        let label_count = labels.len() as i32;
        let iter = partition_ids.clone().into_iter().map(|v| v as FFIPartitionId).flat_map(move |partition_id| {
            let curr_labels = labels.clone();
            let iter = unsafe { get_all_edges(graph, partition_id, curr_labels.as_ptr(), label_count, get_limit_value(limit as i64)) };
            FFIAllEdgesIter::new(graph, iter)
        });
        GlobalEdgeIter::Common(Box::new(iter))
    }

    fn count_all_vertices(&self, si: i64, labels_ref: &Vec<LabelId>, condition: Option<&Condition>, partition_ids: &Vec<PartitionId>) -> u64 {
        let mut count = 0;
        let labels: Vec<FFILabelId> = labels_ref.iter().map(|v| *v as FFILabelId).collect();
        partition_ids.iter().foreach(|partition_id| {
            let label_count = labels.len() as i32;
            let iter = unsafe { get_all_vertices(self.graph, *partition_id as FFIPartitionId, labels.as_ptr(), label_count, i64::max_value()) };
            count += FFIGetAllVerticesIter::new(self.graph, iter).count();
        });
        count as u64
    }

    fn count_all_edges(&self, si: i64, labels_ref: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        let mut ret = 0;
        let labels: Vec<FFILabelId> = labels_ref.iter().map(|v| *v as FFILabelId).collect();
        for partition_id in partition_ids {
            let iter = unsafe { get_all_edges(self.graph, *partition_id as FFIPartitionId, labels.as_ptr(), labels.len() as i32, i64::max_value()) };
            ret += FFIAllEdgesIter::new(self.graph, iter).count();
        }
        ret as u64
    }

    fn translate_vertex_id(&self, vertex_id: i64) -> i64 {
        unsafe {
            get_outer_id(self.graph, vertex_id)
        }
    }

    fn get_schema(&self, si: i64) -> Option<Arc<Schema>> {
        let schema = unsafe { get_schema(self.graph) };
        let ret = FFISchema::new(schema);
        Some(Arc::new(ret))
    }
}

unsafe impl Send for FFIGraphStore {}

unsafe impl Sync for FFIGraphStore {}

////////////// global apis /////////////////


pub enum GlobalEdgeIter {
    Out(FFIOutEdgeIter),
    In(FFIInEdgeIter),
    Common(Box<Iterator<Item=FFIEdge>>),
}

impl Iterator for GlobalEdgeIter {
    type Item = FFIEdge;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            GlobalEdgeIter::Out(ref mut iter) => iter.next(),
            GlobalEdgeIter::In(ref mut iter) => iter.next(),
            GlobalEdgeIter::Common(ref mut iter) => iter.next(),
        }
    }
}

unsafe impl Send for GlobalEdgeIter {}

unsafe impl Sync for GlobalEdgeIter {}

struct CommonEdgeQuery {
    graph: GraphHandle,
    ids: Vec<(PartitionId, Vec<VertexId>)>,
    labels: Vec<FFILabelId>,
    label_count: i32,
    limit: i64,
}

impl CommonEdgeQuery {
    pub fn new(graph: GraphHandle, ids: Vec<(PartitionId, Vec<VertexId>)>, labels: &Vec<LabelId>, limit: i64) -> Self {
        CommonEdgeQuery {
            graph,
            ids,
            labels: labels.iter().map(|v| *v as FFILabelId).collect(),
            label_count: labels.len() as i32,
            limit,
        }
    }

    pub fn run<T, F: 'static + Copy + Fn(GraphHandle, FFIPartitionId, VertexId, Vec<FFILabelId>, i32, i64) -> T>(self, process: F) -> Box<Iterator<Item=(VertexId, T)>> {
        let graph = self.graph;
        let label_count = self.label_count;
        let limit = self.limit;
        let curr_labels = self.labels.clone();
        let ret = self.ids.into_iter().flat_map(move |(partition_id, ids)| {
            let process_labels = curr_labels.clone();
            ids.into_iter().map(move |id| {
                let ret = process(graph, partition_id as FFIPartitionId, id, process_labels.clone(), label_count, limit);
                (id, ret)
            })
        });
        Box::new(ret)
    }
}

pub enum GlobalVertexIter {
    Normal(Box<Iterator<Item=FFIVertex>>),
    Out(FFIGetOutVertexIdsIter),
    In(FFIGetInVertexIdsIter),
}

impl Iterator for GlobalVertexIter {
    type Item = GlobalVertex;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            GlobalVertexIter::Normal(ref mut iter) => {
                let v = iter.next()?;
                Some(GlobalVertex::Normal(v))
            }
            GlobalVertexIter::Out(ref mut iter) => iter.next(),
            GlobalVertexIter::In(ref mut iter) => iter.next(),
        }
    }
}

unsafe impl Send for GlobalVertexIter {}

unsafe impl Sync for GlobalVertexIter {}

pub enum GlobalVertex {
    Normal(FFIVertex),
    IdOnly(IdOnlyVertex),
}

impl Vertex for GlobalVertex {
    type PI = FFIPropertiesIter;

    fn get_id(&self) -> i64 {
        match *self {
            GlobalVertex::Normal(ref v) => v.get_id(),
            GlobalVertex::IdOnly(ref v) => v.get_id(),
        }
    }

    fn get_label_id(&self) -> u32 {
        match *self {
            GlobalVertex::Normal(ref v) => v.get_label_id(),
            GlobalVertex::IdOnly(ref v) => v.get_label_id(),
        }
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if prop_id >= INVALID_PROP_ID {
            return None;
        }
        match *self {
            GlobalVertex::Normal(ref v) => v.get_property(prop_id),
            GlobalVertex::IdOnly(ref v) => v.get_property(prop_id),
        }
    }

    fn get_properties(&self) -> Self::PI {
        match *self {
            GlobalVertex::Normal(ref v) => v.get_properties(),
            GlobalVertex::IdOnly(ref v) => v.get_properties(),
        }
    }
}

////////////// vertex apis /////////////////

pub const INVALID_PROP_ID: u32 = 9999;

pub struct FFIVertex {
    graph: GraphHandle,
    handle: VertexHandle,
}

impl Vertex for FFIVertex {
    type PI = FFIPropertiesIter;

    fn get_id(&self) -> i64 {
        unsafe { get_vertex_id(self.graph, self.handle) }
    }

    fn get_label_id(&self) -> u32 {
        unsafe { get_vertex_label(self.graph, self.handle) }
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if prop_id >= INVALID_PROP_ID {
            return None;
        }
        let mut property = NativeProperty::default();
        let state = unsafe { get_vertex_property(self.graph, self.handle, prop_id as PropertyId, &mut property) };
        if state == STATE_SUCCESS {
            return property.to_property();
        }
        None
    }

    fn get_properties(&self) -> Self::PI {
        let iter = unsafe { get_vertex_properties(self.graph, self.handle) };
        FFIPropertiesIter::new(iter)
    }
}

impl FFIVertex {
    pub fn new(graph: GraphHandle, handle: VertexHandle) -> Self {
        FFIVertex {
            graph,
            handle,
        }
    }
}

unsafe impl Sync for FFIVertex {}

unsafe impl Send for FFIVertex {}

pub struct IdOnlyVertex {
    id: VertexId,
    label: LabelId,
}

impl IdOnlyVertex {
    pub fn new(id: VertexId, label: LabelId) -> Self {
        IdOnlyVertex {
            id,
            label,
        }
    }
}

impl Vertex for IdOnlyVertex {
    type PI = FFIPropertiesIter;

    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_label_id(&self) -> u32 {
        self.label
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        None
    }

    fn get_properties(&self) -> Self::PI {
        FFIPropertiesIter::empty()
    }
}

unsafe impl Send for IdOnlyVertex {}

unsafe impl Sync for IdOnlyVertex {}

struct FFIGetVertexIter {
    graph: GraphHandle,
    iter: GetVertexIterator,
}

impl FFIGetVertexIter {
    pub fn new(graph: GraphHandle, iter: GetVertexIterator) -> Self {
        FFIGetVertexIter {
            graph,
            iter,
        }
    }
}

impl Iterator for FFIGetVertexIter {
    type Item = FFIVertex;

    fn next(&mut self) -> Option<Self::Item> {
        let mut vertex_handle = 0;
        let state = unsafe { get_vertices_next(self.iter, &mut vertex_handle as *mut VertexHandle) };
        if state == STATE_SUCCESS {
            let v = FFIVertex::new(self.graph, vertex_handle);
            return Some(v);
        }
        None
    }
}

impl Drop for FFIGetVertexIter {
    fn drop(&mut self) {
        unsafe { free_get_vertex_iterator(self.iter); }
    }
}

struct FFIGetAllVerticesIter {
    graph: GraphHandle,
    iter: GetAllVerticesIterator,
}

impl FFIGetAllVerticesIter {
    pub fn new(graph: GraphHandle, iter: GetAllVerticesIterator) -> Self {
        FFIGetAllVerticesIter {
            graph,
            iter,
        }
    }
}

impl Iterator for FFIGetAllVerticesIter {
    type Item = FFIVertex;

    fn next(&mut self) -> Option<Self::Item> {
        let mut vertex_handle = 0;
        let state = unsafe { get_all_vertices_next(self.iter, &mut vertex_handle as *mut VertexHandle) };
        if state == STATE_SUCCESS {
            let v = FFIVertex::new(self.graph, vertex_handle);
            return Some(v);
        }
        None
    }
}

impl Drop for FFIGetAllVerticesIter {
    fn drop(&mut self) {
        unsafe {
            free_get_all_vertices_iterator(self.iter);
        }
    }
}

pub struct FFIGetOutVertexIdsIter {
    iter: FFIOutEdgeIter,
}

impl FFIGetOutVertexIdsIter {
    pub fn new(iter: FFIOutEdgeIter) -> Self {
        FFIGetOutVertexIdsIter {
            iter,
        }
    }
}

impl Iterator for FFIGetOutVertexIdsIter {
    type Item = GlobalVertex;

    fn next(&mut self) -> Option<Self::Item> {
        let e = self.iter.next()?;
        let v = IdOnlyVertex::new(e.get_dst_id(), e.get_dst_label_id());
        Some(GlobalVertex::IdOnly(v))
    }
}

pub struct FFIGetInVertexIdsIter {
    iter: FFIInEdgeIter,
}

impl FFIGetInVertexIdsIter {
    pub fn new(iter: FFIInEdgeIter) -> Self {
        FFIGetInVertexIdsIter {
            iter,
        }
    }
}

impl Iterator for FFIGetInVertexIdsIter {
    type Item = GlobalVertex;

    fn next(&mut self) -> Option<Self::Item> {
        let e = self.iter.next()?;
        let v = IdOnlyVertex::new(e.get_src_id(), e.get_src_label_id());
        Some(GlobalVertex::IdOnly(v))
    }
}


//////////// edge apis //////////////
pub struct FFIEdge {
    graph: GraphHandle,
    handle: EdgeHandle,
}

impl FFIEdge {
    pub fn new(graph: GraphHandle, handle: EdgeHandle) -> Self {
        FFIEdge {
            graph,
            handle,
        }
    }
}

impl Edge for FFIEdge {
    type PI = FFIPropertiesIter;

    fn get_label_id(&self) -> u32 {
        unsafe { get_edge_label(self.graph, &self.handle) }
    }

    fn get_src_label_id(&self) -> u32 {
        unsafe { get_edge_src_label(self.graph, &self.handle) }
    }

    fn get_dst_label_id(&self) -> u32 {
        unsafe { get_edge_dst_label(self.graph, &self.handle) }
    }

    fn get_src_id(&self) -> i64 {
        unsafe { get_edge_src_id(self.graph, &self.handle) }
    }

    fn get_dst_id(&self) -> i64 {
        unsafe { get_edge_dst_id(self.graph, &self.handle) }
    }

    fn get_edge_id(&self) -> i64 {
        unsafe { get_edge_id(self.graph, &self.handle) }
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        let mut property = NativeProperty::default();
        let state = unsafe { get_edge_property(self.graph, &self.handle, prop_id as PropertyId, &mut property) };
        if state == STATE_SUCCESS {
            return property.to_property();
        }
        None
    }

    fn get_properties(&self) -> Self::PI {
        let iter = unsafe { get_edge_properties(self.graph, &self.handle) };
        FFIPropertiesIter::new(iter)
    }
}

unsafe impl Sync for FFIEdge {}

unsafe impl Send for FFIEdge {}

pub struct FFIOutEdgeIter {
    graph: GraphHandle,
    iter: OutEdgeIterator,
}

impl FFIOutEdgeIter {
    pub fn new(graph: GraphHandle, iter: OutEdgeIterator) -> Self {
        FFIOutEdgeIter {
            graph,
            iter,
        }
    }
}

impl Iterator for FFIOutEdgeIter {
    type Item = FFIEdge;

    fn next(&mut self) -> Option<Self::Item> {
        let mut edge_handle = EdgeHandle::new();
        let state = unsafe { out_edge_next(self.iter, &mut edge_handle as *mut EdgeHandle) };
        if state == STATE_SUCCESS {
            let e = FFIEdge::new(self.graph, edge_handle);
            return Some(e);
        }
        None
    }
}

impl Drop for FFIOutEdgeIter {
    fn drop(&mut self) {
        unsafe { free_out_edge_iterator(self.iter); }
    }
}

pub struct FFIInEdgeIter {
    graph: GraphHandle,
    iter: InEdgeIterator,
}

impl FFIInEdgeIter {
    pub fn new(graph: GraphHandle, iter: InEdgeIterator) -> Self {
        FFIInEdgeIter {
            graph,
            iter,
        }
    }
}

impl Iterator for FFIInEdgeIter {
    type Item = FFIEdge;

    fn next(&mut self) -> Option<Self::Item> {
        let mut edge_handle = EdgeHandle::new();
        let state = unsafe { in_edge_next(self.iter, &mut edge_handle as *mut EdgeHandle) };
        if state == STATE_SUCCESS {
            let e = FFIEdge::new(self.graph, edge_handle);
            return Some(e);
        }
        None
    }
}

impl Drop for FFIInEdgeIter {
    fn drop(&mut self) {
        unsafe { free_in_edge_iterator(self.iter); }
    }
}

struct FFIAllEdgesIter {
    graph: GraphHandle,
    iter: GetAllEdgesIterator,
}

impl FFIAllEdgesIter {
    pub fn new(graph: GraphHandle, iter: GetAllEdgesIterator) -> Self {
        FFIAllEdgesIter {
            graph,
            iter,
        }
    }
}

impl Iterator for FFIAllEdgesIter {
    type Item = FFIEdge;

    fn next(&mut self) -> Option<Self::Item> {
        let mut edge_handle = EdgeHandle::new();
        let state = unsafe { get_all_edges_next(self.iter, &mut edge_handle as *mut EdgeHandle) };
        if state == STATE_SUCCESS {
            let e = FFIEdge::new(self.graph, edge_handle);
            return Some(e);
        }
        None
    }
}

impl Drop for FFIAllEdgesIter {
    fn drop(&mut self) {
        unsafe { free_get_all_edges_iterator(self.iter); }
    }
}

//////////// properties apis //////////////

pub struct FFIPropertiesIter {
    iter: PropertiesIterator,
}

impl FFIPropertiesIter {
    pub fn new(iter: PropertiesIterator) -> Self {
        FFIPropertiesIter {
            iter,
        }
    }

    pub fn empty() -> Self {
        FFIPropertiesIter {
            iter: std::ptr::null(),
        }
    }
}

impl Drop for FFIPropertiesIter {
    fn drop(&mut self) {
        unsafe { free_properties_iterator(self.iter); }
    }
}

impl Iterator for FFIPropertiesIter {
    type Item = (PropId, Property);

    fn next(&mut self) -> Option<Self::Item> {
        let mut property = NativeProperty::default();
        let state = unsafe { properties_next(self.iter, &mut property) };
        if state == STATE_SUCCESS {
            let ret = property.to_property()?;
            return Some((property.id as PropId, ret));
        }
        None
    }
}

impl NativeProperty {
    fn default() -> Self {
        NativeProperty {
            id: 0,
            r#type: PropertyType::Int,
            data: std::ptr::null(),
            len: 0,
        }
    }

    pub fn to_property(&self) -> Option<Property> {
        let property = self.as_ptr();
        match self.r#type {
            PropertyType::Bool => {
                let mut v = false;
                let res = unsafe { get_property_as_bool(property, &mut v as *mut bool) };
                if res == STATE_SUCCESS {
                    return Some(Property::Bool(v));
                }
            }
            PropertyType::Char => {
                let mut v = 0;
                let res = unsafe { get_property_as_char(property, &mut v as *mut u8) };
                if res == STATE_SUCCESS {
                    return Some(Property::Char(v));
                }
            }
            PropertyType::Short => {
                let mut v = 0;
                let res = unsafe { get_property_as_short(property, &mut v as *mut i16) };
                if res == STATE_SUCCESS {
                    return Some(Property::Short(v));
                }
            }
            PropertyType::Int => {
                let mut v = 0;
                let res = unsafe { get_property_as_int(property, &mut v as *mut i32) };
                if res == STATE_SUCCESS {
                    return Some(Property::Int(v));
                }
            }
            PropertyType::Long => {
                let mut v = 0;
                let res = unsafe { get_property_as_long(property, &mut v as *mut i64) };
                if res == STATE_SUCCESS {
                    return Some(Property::Long(v));
                }
            }
            PropertyType::Float => {
                let mut v = 0.0;
                let res = unsafe { get_property_as_float(property, &mut v as *mut f32) };
                if res == STATE_SUCCESS {
                    return Some(Property::Float(v));
                }
            }
            PropertyType::Double => {
                let mut v = 0.0;
                let res = unsafe { get_property_as_double(property, &mut v as *mut f64) };
                if res == STATE_SUCCESS {
                    return Some(Property::Double(v));
                }
            }
            PropertyType::String => {
                let mut v: *const u8 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_string(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let s = unsafe {
                        let buf = std::slice::from_raw_parts(v, len as usize);
                        std::str::from_utf8(buf).ok().map(|s| s.to_owned())
                    };
                    return s.map(|s| Property::String(s));
                }
            }
            PropertyType::Bytes => {
                let mut v: *const u8 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_bytes(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { std::slice::from_raw_parts(v, len as usize) }.to_vec();
                    return Some(Property::Bytes(ret));
                }
            }
            PropertyType::IntList => {
                let mut v: *const i32 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_int_list(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { get_list_from_c_ptr(v, len) };
                    return Some(Property::ListInt(ret));
                }
            }
            PropertyType::LongList => {
                let mut v: *const i64 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_long_list(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { get_list_from_c_ptr(v, len) };
                    return Some(Property::ListLong(ret));
                }
            }
            PropertyType::FloatList => {
                let mut v: *const f32 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_float_list(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { get_list_from_c_ptr(v, len) };
                    return Some(Property::ListFloat(ret));
                }
            }
            PropertyType::DoubleList => {
                let mut v: *const f64 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_double_list(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { get_list_from_c_ptr(v, len) };
                    return Some(Property::ListDouble(ret));
                }
            }
            PropertyType::StringList => {
                let mut v: *const *const u8 = std::ptr::null();
                let mut len: *const i32 = std::ptr::null();
                let mut count = 0;
                let res = unsafe { get_property_as_string_list(property, &mut v, &mut len, &mut count) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { get_string_list_from_c_ptr(v, len, count) };
                    return ret.map(|x| Property::ListString(x));
                }
            }
        }
        None
    }

    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
}

unsafe fn get_list_from_c_ptr<T: Copy>(ptr: *const T, len: i32) -> Vec<T> {
    let mut ret = Vec::with_capacity(len as usize);
    for offset in 0..len {
        ret.push(*ptr.offset(offset as isize));
    }
    ret
}

unsafe fn get_string_list_from_c_ptr(ptr: *const *const u8, len: *const i32, count: i32) -> Option<Vec<String>> {
    let mut ret = Vec::with_capacity(count as usize);
    for offset in 0..count {
        let s = *ptr.offset(offset as isize);
        let l = *len.offset(offset as isize);
        let buf = std::slice::from_raw_parts(s, l as usize);
        let tmp = std::str::from_utf8(buf).ok()?;
        ret.push(tmp.to_owned());
    }
    Some(ret)
}

impl Drop for NativeProperty {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe { free_property(self.as_ptr()); }
        }
    }
}

#[repr(C)]
union PropertyUnion {
    b: bool,
    c: u8,
    s: i16,
    i: i32,
    l: i64,
    f: f32,
    d: f64,
}

impl WriteNativeProperty {
    fn default() -> Self {
        WriteNativeProperty {
            id: 0,
            r#type: PropertyType::Int,
            data: std::ptr::null(),
            len: 0,
        }
    }

    pub fn from_property(propid: PropId, property: Property) -> Self {
        let (prop_type, mut data, data_len) = {
            match property {
                Property::Bool(v) => {
                    let u = PropertyUnion { b: v };
                    (PropertyType::Bool, vec![], unsafe { u.l })
                }
                Property::Char(v) => {
                    let u = PropertyUnion { c: v };
                    (PropertyType::Char, vec![], unsafe { u.l })
                }
                Property::Short(v) => {
                    let u = PropertyUnion { s: v };
                    (PropertyType::Short, vec![], unsafe { u.l })
                }
                Property::Int(v) => {
                    let u = PropertyUnion { i: v };
                    (PropertyType::Int, vec![], unsafe { u.l })
                }
                Property::Long(v) => {
                    let u = PropertyUnion { l: v };
                    (PropertyType::Long, vec![], unsafe { u.l })
                }
                Property::Float(v) => {
                    let u = PropertyUnion { f: v };
                    (PropertyType::Float, vec![], unsafe { u.l })
                }
                Property::Double(v) => {
                    let u = PropertyUnion { d: v };
                    (PropertyType::Double, vec![], unsafe { u.l })
                }
                Property::Bytes(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::Bytes, vecdata, len)
                }
                Property::String(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::String, vecdata, len)
                }
                Property::Date(v) => {
                    panic!("property is Date");
                }
                Property::ListInt(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::IntList, vecdata, len)
                }
                Property::ListLong(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::LongList, vecdata, len)
                }
                Property::ListFloat(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::FloatList, vecdata, len)
                }
                Property::ListDouble(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::DoubleList, vecdata, len)
                }
                Property::ListString(ref v) => {
                    let vecdata = property.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::StringList, vecdata, len)
                }
                Property::ListBytes(v) => {
                    panic!("property is ListBytes");
                }

                Property::Null => {
                    panic!("property is null");
                }
                _ => unimplemented!()
            }
        };
        data.shrink_to_fit();
        let data_ptr = data.as_ptr();
        _info!("build native property id {:?} type {:?} data {:?} len {:?}", propid, &prop_type, &data, data_len);

        ::std::mem::forget(data);
        WriteNativeProperty {
            id: propid as i32,
            r#type: prop_type,
            data: data_ptr,
            len: data_len as i64,
        }
    }

    pub fn from_prop_entity(prop: &PropertyEntity) -> Self {
        let value = prop.get_value();
        let (prop_type, mut data, data_len) = {
            match value {
                ValuePayload::Bool(v) => {
                    let u = PropertyUnion { b: *v };
                    (PropertyType::Bool, vec![], unsafe { u.l })
                }
                ValuePayload::Char(v) => {
                    let u = PropertyUnion { c: *v };
                    (PropertyType::Char, vec![], unsafe { u.l })
                }
                ValuePayload::Short(v) => {
                    let u = PropertyUnion { s: *v };
                    (PropertyType::Short, vec![], unsafe { u.l })
                }
                ValuePayload::Int(v) => {
                    let u = PropertyUnion { i: *v };
                    (PropertyType::Int, vec![], unsafe { u.l })
                }
                ValuePayload::Long(v) => {
                    let u = PropertyUnion { l: *v };
                    (PropertyType::Long, vec![], unsafe { u.l })
                }
                ValuePayload::Float(v) => {
                    let u = PropertyUnion { f: f32::parse_bytes(v) };
                    (PropertyType::Float, vec![], unsafe { u.l })
                }
                ValuePayload::Double(v) => {
                    let u = PropertyUnion { d: f64::parse_bytes(v) };
                    (PropertyType::Double, vec![], unsafe { u.l })
                }
                ValuePayload::Bytes(v) => {
                    let data = vec![0; v.len()];
                    let len = data.len() as i64;
                    (PropertyType::Bytes, data, len)
                }
                ValuePayload::String(v) => {
                    let bytes = v.as_bytes();
                    let len = bytes.len() as i64;
                    (PropertyType::String, bytes.to_vec(), len)
                }
                _ => {
                    unimplemented!("not support property value {:?}", prop)
                }
            }
        };
        data.shrink_to_fit();
        let data_ptr = data.as_ptr();
        _info!("build native property id {:?} type {:?} data {:?} len {:?} from property entity", prop.get_propid(), &prop_type, &data, data_len);

        ::std::mem::forget(data);
        WriteNativeProperty {
            id: prop.get_propid(),
            r#type: prop_type,
            data: data_ptr,
            len: data_len as i64,
        }
    }

    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
}

impl Drop for WriteNativeProperty {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let data_len = self.len as usize;
            match self.r#type {
                PropertyType::Bool |
                PropertyType::Char |
                PropertyType::Short |
                PropertyType::Int |
                PropertyType::Long |
                PropertyType::Float |
                PropertyType::Double => {
                    unsafe {
                        drop(Vec::from_raw_parts(self.data as *mut u8, 0, 0));
                    }
                }
                _ => {
                    unsafe {
                        drop(Vec::from_raw_parts(self.data as *mut u8, data_len, data_len));
                    }
                }
            }
        }
    }
}

impl PropertyType {
    pub fn from_data_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::Bool => PropertyType::Bool,
            DataType::Char => PropertyType::Char,
            DataType::Short => PropertyType::Short,
            DataType::Int => PropertyType::Int,
            DataType::Long => PropertyType::Long,
            DataType::Float => PropertyType::Float,
            DataType::Double => PropertyType::Double,
            DataType::String => PropertyType::String,
            DataType::Bytes => PropertyType::Bytes,
            DataType::ListInt => PropertyType::IntList,
            DataType::ListLong => PropertyType::LongList,
            DataType::ListFloat => PropertyType::FloatList,
            DataType::ListDouble => PropertyType::DoubleList,
            DataType::ListString => PropertyType::StringList,
            _ => {
                unimplemented!("Unsupported data type {:?}", data_type)
            }
        }
    }

    pub fn to_data_type(&self) -> DataType {
        match *self {
            PropertyType::Bool => DataType::Bool,
            PropertyType::Char => DataType::Char,
            PropertyType::Short => DataType::Short,
            PropertyType::Int => DataType::Int,
            PropertyType::Long => DataType::Long,
            PropertyType::Float => DataType::Float,
            PropertyType::Double => DataType::Double,
            PropertyType::String => DataType::String,
            PropertyType::Bytes => DataType::Bytes,
            PropertyType::IntList => DataType::ListInt,
            PropertyType::LongList => DataType::ListLong,
            PropertyType::FloatList => DataType::ListFloat,
            PropertyType::DoubleList => DataType::ListDouble,
            PropertyType::StringList => DataType::ListString,
        }
    }
}

//////////// schema apis //////////////
struct FFISchema {
    schema: SchemaHandle,
}

impl FFISchema {
    pub fn new(schema: SchemaHandle) -> Self {
        FFISchema {
            schema,
        }
    }
}

impl Schema for FFISchema {
    fn get_prop_id(&self, name: &str) -> Option<u32> {
        let c_name = CString::new(name).unwrap();
        let mut ret = 0;
        let state = unsafe { get_property_id(self.schema, c_name.as_ptr(), &mut ret) };
        if state == STATE_SUCCESS {
            return Some(ret as u32);
        }
        None
    }

    fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<DataType> {
        let mut t = PropertyType::Bool;
        let state = unsafe { get_property_type(self.schema, label, prop_id as PropertyId, &mut t) };
        if state == STATE_SUCCESS {
            return Some(t.to_data_type());
        }
        None
    }

    fn get_prop_name(&self, prop_id: u32) -> Option<String> {
        let name: *const ::libc::c_char = std::ptr::null();
        let state = unsafe { get_property_name(self.schema, prop_id as PropertyId, &name) };
        info!("get prop name: state = {:?}, expected: {:?}", state, STATE_SUCCESS);
        if state == STATE_SUCCESS {
            let c_name = unsafe { CStr::from_ptr(name) };
            let ret = c_name.to_owned().into_string().ok();
            info!("get prop name: c_name = {:?} -> {:?}", c_name, ret);
            unsafe { free_string(name); }
            return ret;
        }
        None
    }

    fn get_label_id(&self, name: &str) -> Option<u32> {
        let mut id = 0;
        let c_name = CString::new(name).unwrap();
        let state = unsafe { get_label_id(self.schema, c_name.as_ptr(), &mut id) };
        if state == STATE_SUCCESS {
            return Some(id);
        }
        None
    }

    fn get_label_name(&self, label: u32) -> Option<String> {
        let name: *const ::libc::c_char = std::ptr::null();
        let state = unsafe { get_label_name(self.schema, label, &name) };
        info!("get label name: state = {:?}, expected: {:?}", state, STATE_SUCCESS);
        if state == STATE_SUCCESS {
            let c_name = unsafe { CStr::from_ptr(name) };
            let ret = c_name.to_str().ok().map(|s| s.to_owned());
            info!("get label name: c_name = {:?} -> {:?}", c_name, ret);
            unsafe { free_string(name); }
            return ret;
        }
        None
    }

    fn to_proto(&self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Drop for FFISchema {
    fn drop(&mut self) {
        unsafe { free_schema(self.schema); }
    }
}

unsafe impl Send for FFISchema {}

unsafe impl Sync for FFISchema {}

pub struct VineyardPartitionManager {
    graph: GraphHandle,
}

impl VineyardPartitionManager {
    pub fn new(graph: GraphHandle) -> Self {
        VineyardPartitionManager {
            graph,
        }
    }
}

impl GraphPartitionManager for VineyardPartitionManager {
    fn get_partition_id(&self, vid: i64) -> i32 {
        unsafe {
            get_partition_id(self.graph, vid)
        }
    }

    // TODO(bingqing): check with vineyard
    fn get_server_id(&self, _pid: u32) -> Option<u32> {
        unimplemented!()
    }

    fn get_process_partition_list(&self) -> Vec<u32> {
        let mut partition_id_list = Vec::new();
        let mut partition_count: i32 = 0;
        unsafe {
            let pvalue_list: *const ::libc::c_int = std::ptr::null();
            let partition_list: *const *const ::libc::c_int = &pvalue_list;
            info!("start read partition list");
            get_process_partition_list(self.graph, partition_list, &mut partition_count);
            for i in 0..partition_count as usize {
                info!("receive partition: {:?}", pvalue_list.offset(i as isize) as PartitionId);
                partition_id_list.push(*pvalue_list.offset(i as isize) as PartitionId)
            }
            info!("partition_id_list: {:?}", partition_id_list);
            free_partition_list(pvalue_list);
        }

        partition_id_list
    }

    fn get_vertex_id_by_primary_key(&self, label_id: u32, key: &String) -> Option<(u32, i64)> {
        let mut partition_id = 0;
        let mut vertex_id = 0;
        let c_key = CString::new(key.as_str()).unwrap();
        unsafe {
            let ret = get_vertex_id_from_primary_key(self.graph, label_id, c_key.as_ptr(), &mut vertex_id, &mut partition_id);
            if 0 != ret {
                error!("get vertex id from primary key {:?} with label id {:?} failed", key, label_id);
                return None;
            }
        }
        return Some((partition_id as u32, vertex_id));
    }
}

unsafe impl Send for VineyardPartitionManager {}

unsafe impl Sync for VineyardPartitionManager {}

#[test]
fn zzzzzz() {
    let pid_list = vec![1, 2, 3];
    unsafe {
        let graph_handle = get_graph_handle(1, 1);
        let partition_manager = VineyardPartitionManager::new(graph_handle);
        let partition_id_list = partition_manager.get_process_partition_list();
        println!("partition id list {:?}", partition_id_list);

        // get_graph_handle(1);
        // free_graph_handle(std::ptr::null());

        // let p = NativeProperty { data: std::ptr::null(), len: 10, r#type: PropertyType::Double};

        // test1(p);
    }
}
