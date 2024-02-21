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

use std::ffi::CStr;
use std::os::raw::{c_char, c_void};
use std::str;
use std::sync::Arc;

use groot_store::db::api::partition_graph::PartitionGraph;
use groot_store::db::api::partition_snapshot::PartitionSnapshot;
use groot_store::db::api::types::{Property, PropertyReader, PropertyValue, RocksEdge, RocksVertex};
use groot_store::db::api::{
    EdgeId, EdgeKind, GraphConfigBuilder, GraphError, LabelId, PropertyId, Records, SerialId, SnapshotId,
    VertexId,
};
use groot_store::db::graph::entity::{PropertiesIter, PropertyImpl, RocksEdgeImpl, RocksVertexImpl};
use groot_store::db::graph::store::GraphStore;
use groot_store::db::wrapper::wrapper_partition_graph::{WrapperPartitionGraph, WrapperPartitionSnapshot};

use crate::store::graph::{FfiPartitionGraph, PartitionGraphHandle};

pub type PartitionSnapshotHandle = *const c_void;
pub type ErrorHandle = *const c_void;
pub type VertexHandle = *const c_void;
pub type VertexIteratorHandle = *const c_void;
pub type EdgeHandle = *const c_void;
pub type EdgeIteratorHandle = *const c_void;
pub type PropertyHandle = *const c_void;
pub type PropertyIteratorHandle = *const c_void;

pub type FfiSnapshot = WrapperPartitionSnapshot<GraphStore>;
pub type FfiVertex = RocksVertexImpl;
pub type FfiEdge = RocksEdgeImpl;
pub type FfiVertexIterator = Records<FfiVertex>;
pub type FfiEdgeIterator = Records<FfiEdge>;
pub type FfiProperty = PropertyImpl;
pub type FfiPropertyIterator = PropertiesIter<'static>;

#[repr(C)]
pub struct StringSlice {
    data: *const u8,
    len: usize,
}

impl StringSlice {
    pub fn new(data: *const u8, len: usize) -> StringSlice {
        StringSlice { data, len }
    }

    pub fn null() -> StringSlice {
        StringSlice { data: std::ptr::null(), len: 0 }
    }
}

/// Partition Snapshot FFIs

#[no_mangle]
pub extern "C" fn OpenPartitionGraph(store_path: *const c_char) -> PartitionGraphHandle {
    trace!("Open partition graph");
    unsafe {
        let slice = CStr::from_ptr(store_path).to_bytes();
        let store_path_str = str::from_utf8(slice).unwrap();
        let mut config_builder = GraphConfigBuilder::new();
        config_builder.set_storage_engine("rocksdb");
        config_builder.add_storage_option("store.data.path", store_path_str);
        let config = config_builder.build();
        let graph_store = Arc::new(GraphStore::open(&config).unwrap());
        let partition_graph = WrapperPartitionGraph::new(graph_store);
        Box::into_raw(Box::new(partition_graph)) as PartitionGraphHandle
    }
}

#[no_mangle]
pub extern "C" fn GetSnapshot(
    handle: PartitionGraphHandle, snapshot_id: SnapshotId,
) -> PartitionSnapshotHandle {
    trace!("Get snapshot");
    let graph_store_ptr = unsafe { &*(handle as *const FfiPartitionGraph) };
    let snapshot = graph_store_ptr.get_snapshot(snapshot_id);
    Box::into_raw(Box::new(snapshot)) as PartitionSnapshotHandle
}

#[no_mangle]
pub extern "C" fn GetVertex(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, label_id: LabelId,
    error: *mut ErrorHandle,
) -> VertexHandle {
    trace!("Get vertex");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_vertex(vertex_id, label_option(label_id), None) {
            Ok(Some(data)) => {
                let data_hdl = Box::new(data);
                return Box::into_raw(data_hdl) as VertexHandle;
            }
            Ok(None) => {}
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
        }
        ::std::ptr::null()
    }
}

#[no_mangle]
pub extern "C" fn GetEdge(
    partition_snapshot: PartitionSnapshotHandle, edge_id: EdgeId, edge_relation: &EdgeKind,
    error: *mut ErrorHandle,
) -> EdgeHandle {
    trace!("Get edge");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_edge(edge_id, edge_relation_option(edge_relation), None) {
            Ok(Some(data)) => {
                let data_hdl = Box::new(data);
                return Box::into_raw(data_hdl) as EdgeHandle;
            }
            Ok(None) => {}
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
        }
        ::std::ptr::null()
    }
}

#[no_mangle]
pub extern "C" fn ScanVertex(
    partition_snapshot: PartitionSnapshotHandle, label_id: LabelId, error: *mut ErrorHandle,
) -> VertexIteratorHandle {
    trace!("Scan vertex");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.scan_vertex(label_option(label_id), None, None) {
            Ok(data) => Box::into_raw(data) as VertexIteratorHandle,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                ::std::ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn ScanEdge(
    partition_snapshot: PartitionSnapshotHandle, edge_relation: &EdgeKind, error: *mut ErrorHandle,
) -> EdgeIteratorHandle {
    trace!("Scan edge");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.scan_edge(
            edge_relation_option(edge_relation).map(|r| r.get_edge_label_id()),
            None,
            None,
        ) {
            Ok(data) => Box::into_raw(data) as EdgeIteratorHandle,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                ::std::ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetOutEdges(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_label_id: LabelId,
    error: *mut ErrorHandle,
) -> EdgeIteratorHandle {
    trace!("Get out edges");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_out_edges(vertex_id, label_option(edge_label_id), None, None) {
            Ok(data) => Box::into_raw(data) as EdgeIteratorHandle,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                ::std::ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetInEdges(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_label_id: LabelId,
    error: *mut ErrorHandle,
) -> EdgeIteratorHandle {
    trace!("Get in edges");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_in_edges(vertex_id, label_option(edge_label_id), None, None) {
            Ok(data) => Box::into_raw(data) as EdgeIteratorHandle,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                std::ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetOutDegree(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_relation: &EdgeKind,
    error: *mut ErrorHandle,
) -> usize {
    trace!("Get out degree");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_out_degree(vertex_id, Some(edge_relation.get_edge_label_id())) {
            Ok(degree) => degree,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                usize::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetInDegree(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_relation: &EdgeKind,
    error: *mut ErrorHandle,
) -> usize {
    trace!("Get in degree");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        match handler.get_in_degree(vertex_id, Some(edge_relation.get_edge_label_id())) {
            Ok(degree) => degree,
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                usize::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetKthOutEdge(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_relation: &EdgeKind,
    k: SerialId, error: *mut ErrorHandle,
) -> EdgeHandle {
    trace!("Get kth out edge");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        let mut ret: EdgeHandle = ::std::ptr::null();
        match handler.get_kth_out_edge(vertex_id, edge_relation, k, None) {
            Ok(Some(data)) => {
                let data_hdl = Box::new(data);
                ret = Box::into_raw(data_hdl) as EdgeHandle;
            }
            Ok(None) => {}
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
        }
        ret
    }
}

#[no_mangle]
pub extern "C" fn GetKthInEdge(
    partition_snapshot: PartitionSnapshotHandle, vertex_id: VertexId, edge_relation: &EdgeKind,
    k: SerialId, error: *mut ErrorHandle,
) -> EdgeHandle {
    trace!("Get kth in edge");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        let mut ret: EdgeHandle = ::std::ptr::null();
        match handler.get_kth_in_edge(vertex_id, edge_relation, k, None) {
            Ok(Some(data)) => {
                let data_hdl = Box::new(data);
                ret = Box::into_raw(data_hdl) as EdgeHandle;
            }
            Ok(None) => {}
            Err(e) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
        }
        ret
    }
}

#[no_mangle]
pub extern "C" fn GetSnapshotId(partition_snapshot: PartitionSnapshotHandle) -> SnapshotId {
    trace!("Get snapshot id");
    unsafe {
        let handler = &*(partition_snapshot as *const FfiSnapshot);
        handler.get_snapshot_id()
    }
}

/// Vertex FFIs

#[no_mangle]
pub extern "C" fn VertexIteratorNext(
    vertex_iterator_handle: VertexIteratorHandle, error: *mut ErrorHandle,
) -> VertexHandle {
    trace!("Vertex iterator next");
    unsafe {
        let handler = &mut *(vertex_iterator_handle as *mut FfiVertexIterator);
        match handler.next() {
            Some(Ok(data)) => {
                let data_hdl = Box::new(data);
                return Box::into_raw(data_hdl) as VertexHandle;
            }
            Some(Err(e)) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
            None => {}
        }
        ::std::ptr::null()
    }
}

#[no_mangle]
pub extern "C" fn GetVertexId(vertex_handle: VertexHandle) -> VertexId {
    trace!("Get vertex id");
    unsafe {
        let handler = &*(vertex_handle as *const FfiVertex);
        handler.get_vertex_id()
    }
}

#[no_mangle]
pub extern "C" fn GetVertexLabelId(vertex_handle: VertexHandle) -> LabelId {
    trace!("Get vertex label id");
    unsafe {
        let handler = &*(vertex_handle as *const FfiVertex);
        handler.get_label_id()
    }
}

#[no_mangle]
pub extern "C" fn GetVertexProperty(
    vertex_handle: VertexHandle, property_id: PropertyId,
) -> PropertyHandle {
    trace!("Get vertex property");
    unsafe {
        let handler = &*(vertex_handle as *const FfiVertex);
        match handler.get_property(property_id) {
            Some(prop) => {
                let prop_hdl = Box::new(prop);
                Box::into_raw(prop_hdl) as PropertyHandle
            }
            None => ::std::ptr::null(),
        }
    }
}

#[no_mangle]
pub extern "C" fn GetVertexPropertyIterator(vertex_handle: VertexHandle) -> PropertyIteratorHandle {
    trace!("Get vertex property iterator");
    unsafe {
        let handler = &*(vertex_handle as *const FfiVertex);
        let iter_hdl = Box::new(handler.get_property_iterator());
        Box::into_raw(iter_hdl) as PropertyIteratorHandle
    }
}

/// Edge FFIs

#[no_mangle]
pub extern "C" fn EdgeIteratorNext(
    edge_iterator_handle: EdgeIteratorHandle, error: *mut ErrorHandle,
) -> EdgeHandle {
    trace!("Edge iterator next");
    unsafe {
        let handler = &mut *(edge_iterator_handle as *mut FfiEdgeIterator);
        match handler.next() {
            Some(Ok(data)) => {
                let data_hdl = Box::new(data);
                return Box::into_raw(data_hdl) as EdgeHandle;
            }
            Some(Err(e)) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
            None => {}
        }
        ::std::ptr::null()
    }
}

#[no_mangle]
pub extern "C" fn GetEdgeId(edge_handle: EdgeHandle) -> EdgeId {
    trace!("Get edge id");
    unsafe {
        let handler = &*(edge_handle as *const FfiEdge);
        handler.get_edge_id().clone()
    }
}

#[no_mangle]
pub extern "C" fn GetEdgeRelation(edge_handle: EdgeHandle) -> EdgeKind {
    trace!("Get edge relation");
    unsafe {
        let handler = &*(edge_handle as *const FfiEdge);
        handler.get_edge_relation().clone()
    }
}

#[no_mangle]
pub extern "C" fn GetEdgeProperty(edge_handle: EdgeHandle, property_id: PropertyId) -> PropertyHandle {
    trace!("Get edge property");
    unsafe {
        let handler = &*(edge_handle as *const FfiEdge);
        match handler.get_property(property_id) {
            Some(prop) => {
                let prop_hdl = Box::new(prop);
                Box::into_raw(prop_hdl) as PropertyHandle
            }
            None => ::std::ptr::null(),
        }
    }
}

#[no_mangle]
pub extern "C" fn GetEdgePropertyIterator(edge_handle: EdgeHandle) -> PropertyIteratorHandle {
    trace!("Get edge property iterator");
    unsafe {
        let handler = &*(edge_handle as *const FfiEdge);
        let iter_hdl = Box::new(handler.get_property_iterator());
        Box::into_raw(iter_hdl) as PropertyIteratorHandle
    }
}

/// Property FFIs

#[no_mangle]
pub extern "C" fn PropertyIteratorNext(
    property_iterator_handle: PropertyIteratorHandle, error: *mut ErrorHandle,
) -> PropertyHandle {
    trace!("Property iterator next");
    unsafe {
        let handler = &mut *(property_iterator_handle as *mut FfiPropertyIterator);
        match handler.next() {
            Some(Ok(data)) => {
                let data_hdl = Box::new(data);
                return Box::into_raw(data_hdl) as PropertyHandle;
            }
            Some(Err(e)) => {
                let error_hdl = Box::new(e);
                *error = Box::into_raw(error_hdl) as ErrorHandle;
            }
            None => {}
        }
        ::std::ptr::null()
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyId(property_handle: PropertyHandle) -> PropertyId {
    trace!("Get property id");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        handler.get_property_id()
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyAsInt32(property_handle: PropertyHandle, error: *mut ErrorHandle) -> i32 {
    trace!("Get property as int32");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        match handler.get_property_value() {
            PropertyValue::Int(data) => *data,
            PropertyValue::Null => {
                let error_hdl = Box::new(GraphError::Internal("None Record!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                i32::MAX
            }
            _ => {
                let error_hdl = Box::new(GraphError::Internal("Wrong Record Type Int32!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                i32::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyAsInt64(property_handle: PropertyHandle, error: *mut ErrorHandle) -> i64 {
    trace!("Get property as int64");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        match handler.get_property_value() {
            PropertyValue::Long(data) => *data,
            PropertyValue::Null => {
                let error_hdl = Box::new(GraphError::Internal("None Record!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                i64::MAX
            }
            _ => {
                let error_hdl = Box::new(GraphError::Internal("Wrong Record Type Int64!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                i64::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyAsFloat(property_handle: PropertyHandle, error: *mut ErrorHandle) -> f32 {
    trace!("Get property as float");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        match handler.get_property_value() {
            PropertyValue::Float(data) => *data,
            PropertyValue::Null => {
                let error_hdl = Box::new(GraphError::Internal("None Record!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                f32::MAX
            }
            _ => {
                let error_hdl = Box::new(GraphError::Internal("Wrong Record Type Float!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                f32::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyAsDouble(property_handle: PropertyHandle, error: *mut ErrorHandle) -> f64 {
    trace!("Get property as double");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        match handler.get_property_value() {
            PropertyValue::Double(data) => *data,
            PropertyValue::Null => {
                let error_hdl = Box::new(GraphError::Internal("None Record!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                f64::MAX
            }
            _ => {
                let error_hdl =
                    Box::new(GraphError::Internal("Wrong Record Type Double!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                f64::MAX
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn GetPropertyAsString(
    property_handle: PropertyHandle, error: *mut ErrorHandle,
) -> StringSlice {
    trace!("Get property as string");
    unsafe {
        let handler = &*(property_handle as *const FfiProperty);
        match handler.get_property_value() {
            PropertyValue::String(data) => StringSlice::new(data.as_ptr(), data.len()),
            PropertyValue::Null => {
                let error_hdl = Box::new(GraphError::Internal("None Record!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                StringSlice::null()
            }
            _ => {
                let error_hdl =
                    Box::new(GraphError::Internal("Wrong Record Type Double!".parse().unwrap()));
                *error = Box::into_raw(error_hdl) as ErrorHandle;
                StringSlice::null()
            }
        }
    }
}

/// Error FFIs

#[no_mangle]
pub extern "C" fn GetErrorInfo(error_handle: ErrorHandle) -> StringSlice {
    trace!("Get error info");
    unsafe {
        let handler = &*(error_handle as *const GraphError);
        let info = format!("{:?}", handler);
        StringSlice::new(info.as_ptr(), info.len())
    }
}

/// Handle Release FFIs

#[no_mangle]
pub extern "C" fn ReleasePartitionGraphHandle(ptr: PartitionGraphHandle) {
    trace!("Release partition graph handle");
    let handler = ptr as *mut FfiPartitionGraph;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleasePartitionSnapshotHandle(ptr: PartitionSnapshotHandle) {
    trace!("Release partition snapshot handle");
    let handler = ptr as *mut FfiSnapshot;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleaseErrorHandle(ptr: ErrorHandle) {
    trace!("Release error handle");
    let handler = ptr as *mut GraphError;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleaseVertexHandle(ptr: VertexHandle) {
    trace!("Release vertex handle");
    let handler = ptr as *mut FfiVertex;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleaseVertexIteratorHandle(ptr: VertexIteratorHandle) {
    trace!("Release vertex iterator handle");
    let handler = ptr as *mut FfiVertexIterator;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleaseEdgeHandle(ptr: EdgeHandle) {
    trace!("Release edge handle");
    let handler = ptr as *mut FfiEdge;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleaseEdgeIteratorHandle(ptr: EdgeIteratorHandle) {
    trace!("Release edge iterator handle");
    let handler = ptr as *mut FfiEdgeIterator;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleasePropertyHandle(ptr: PropertyHandle) {
    trace!("Release property handle");
    let handler = ptr as *mut FfiProperty;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

#[no_mangle]
pub extern "C" fn ReleasePropertyIteratorHandle(ptr: PropertyIteratorHandle) {
    trace!("Release property iterator handle");
    let handler = ptr as *mut FfiPropertyIterator;
    unsafe {
        drop(Box::from_raw(handler));
    }
}

/// Internal functions

fn label_option(label_id: LabelId) -> Option<LabelId> {
    if label_id != LabelId::MAX {
        Some(label_id)
    } else {
        None
    }
}

fn edge_relation_option(edge_relation: &EdgeKind) -> Option<&EdgeKind> {
    if edge_relation.get_edge_label_id() != LabelId::MAX {
        Some(edge_relation)
    } else {
        None
    }
}
