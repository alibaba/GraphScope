//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use std::ffi::{c_void, CStr};
use std::os::raw::c_char;

use pegasus_common::codec::{Decode, Encode};

use crate::common::{DefaultId, InternalId, Label, LabelId, INVALID_LABEL_ID};
use crate::config::{GraphDBConfig, JsonConf};
use crate::graph_db::{GlobalStoreUpdate, MutableGraphDB};
use crate::ldbc::{EdgeTypeTuple, LDBCEdgeParser, LDBCParser, LDBCParserJsonSer, LDBCVertexParser};
use crate::parser::{EdgeMeta, ParserTrait};
use crate::table::Row;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResultCode {
    Success = 0,
    SerdeError = 1,
    ParseError = 2,
    WriteGraphError = 3,
    DumpGraphError = 4,
    OtherError = 5,
}

impl Default for ResultCode {
    fn default() -> Self {
        ResultCode::Success
    }
}

fn serialize_properties<D: Encode>(data: D) -> FfiPropertyBytes {
    let mut bytes: Vec<u8> = Vec::new();
    if data.write_to(&mut bytes).is_ok() {
        let len = bytes.len();
        let ptr = bytes.as_ptr() as *const c_void;
        std::mem::forget(bytes);

        FfiPropertyBytes { ptr, len: len as i64 }
    } else {
        FfiPropertyBytes::default()
    }
}

fn deserialize_properties<D: Decode + Default>(bytes: FfiPropertyBytes) -> D {
    if !bytes.ptr.is_null() {
        let bytes: Vec<u8> =
            unsafe { Vec::from_raw_parts(bytes.ptr as *mut u8, bytes.len as usize, bytes.len as usize) };
        let data = D::read_from(&mut bytes.as_slice());
        std::mem::forget(bytes);
        if data.is_ok() {
            data.unwrap()
        } else {
            D::default()
        }
    } else {
        D::default()
    }
}

#[repr(C)]
pub struct FfiPropertyBytes {
    ptr: *const c_void,
    len: i64,
}

impl Default for FfiPropertyBytes {
    fn default() -> Self {
        Self { ptr: std::ptr::null() as *const c_void, len: 0 }
    }
}

#[no_mangle]
pub extern "C" fn destroy_property_bytes(bytes: FfiPropertyBytes) {
    if !bytes.ptr.is_null() {
        let _: Vec<u8> =
            unsafe { Vec::from_raw_parts(bytes.ptr as *mut u8, bytes.len as usize, bytes.len as usize) };
    }
}

#[repr(C)]
pub struct FfiVertexData {
    id: i64,
    primary_label: i32,
    secondary_label: i32,
    property_bytes: FfiPropertyBytes,
    code: ResultCode,
}

impl FfiVertexData {
    fn with_error(code: ResultCode) -> Self {
        FfiVertexData {
            id: -1,
            primary_label: INVALID_LABEL_ID as i32,
            secondary_label: INVALID_LABEL_ID as i32,
            property_bytes: Default::default(),
            code,
        }
    }
}

#[repr(C)]
pub struct FfiEdgeData {
    label_id: i32,
    src_vertex_id: i64,
    src_label_id: i32,
    dst_vertex_id: i64,
    dst_label_id: i32,
    property_bytes: FfiPropertyBytes,
    code: ResultCode,
}

impl FfiEdgeData {
    fn with_error(code: ResultCode) -> Self {
        FfiEdgeData {
            label_id: INVALID_LABEL_ID as i32,
            src_vertex_id: 0,
            src_label_id: INVALID_LABEL_ID as i32,
            dst_vertex_id: 0,
            dst_label_id: INVALID_LABEL_ID as i32,
            property_bytes: Default::default(),
            code,
        }
    }
}

#[repr(C)]
pub struct FfiEdgeTypeTuple {
    etype: *const c_char,
    src_vertex_type: *const c_char,
    dst_vertex_type: *const c_char,
}

impl From<FfiEdgeTypeTuple> for EdgeTypeTuple {
    fn from(tuple: FfiEdgeTypeTuple) -> EdgeTypeTuple {
        EdgeTypeTuple::new(
            cstr_to_string(tuple.etype),
            cstr_to_string(tuple.src_vertex_type),
            cstr_to_string(tuple.dst_vertex_type),
        )
    }
}

fn cstr_to_string(cstr: *const c_char) -> String {
    if !cstr.is_null() {
        let str_result = unsafe { CStr::from_ptr(cstr) }.to_str();
        if let Ok(str) = str_result {
            str.to_string()
        } else {
            "".to_string()
        }
    } else {
        "".to_string()
    }
}

fn string_to_cstr(str: String) -> *const c_char {
    let str_result = std::ffi::CString::new(str);
    if let Ok(str) = str_result {
        let str_ptr = str.as_ptr();
        // **NOTE** must release the string pointer on the C side.
        std::mem::forget(str);

        str_ptr
    } else {
        std::ptr::null::<c_char>()
    }
}

fn destroy_ptr<M>(ptr: *const c_void) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ptr as *mut M);
        }
    }
}

fn get_ptr<D>(data: D) -> *const c_void {
    let data = Box::new(data);
    let ptr = Box::into_raw(data) as *const c_void;

    ptr
}

fn get_boxed_data<D>(ptr: *const c_void) -> Box<D> {
    unsafe { Box::from_raw(ptr as *mut D) }
}

#[no_mangle]
pub extern "C" fn init_parser_from_file(metadata_file: *const c_char) -> *const c_void {
    let json = cstr_to_string(metadata_file);
    if let Ok(ldbc_json) = LDBCParserJsonSer::from_json_file(json) {
        get_ptr(LDBCParser::from(ldbc_json))
    } else {
        std::ptr::null::<c_void>()
    }
}

#[no_mangle]
pub extern "C" fn init_parser_from_json(metadata_json: *const c_char) -> *const c_void {
    let json = cstr_to_string(metadata_json);
    if let Ok(ldbc_json) = LDBCParserJsonSer::from_json(json) {
        get_ptr(LDBCParser::from(ldbc_json))
    } else {
        std::ptr::null::<c_void>()
    }
}

#[no_mangle]
pub extern "C" fn get_schema_json_from_parser(ptr_parser: *const c_void) -> *const c_char {
    let parser = get_boxed_data::<LDBCParser>(ptr_parser);
    let schema = parser.get_graph_schema();
    let result = if let Ok(schema_json) = schema.to_json() {
        string_to_cstr(schema_json)
    } else {
        std::ptr::null::<c_char>()
    };
    std::mem::forget(parser);

    result
}

#[no_mangle]
pub extern "C" fn destroy_parser(ptr_parser: *const c_void) {
    destroy_ptr::<LDBCParser>(ptr_parser)
}

#[no_mangle]
pub extern "C" fn get_vertex_partition(vertex_id: i64, number_partitions: i64) -> i64 {
    vertex_id % number_partitions
}

#[no_mangle]
pub extern "C" fn get_vertex_parser(
    ptr_parser: *const c_void, vertex_type: *const c_char,
) -> *const c_void {
    let parser = get_boxed_data::<LDBCParser>(ptr_parser);
    let vtype = cstr_to_string(vertex_type);
    let result = if let Ok(vertex_parser) = parser.get_vertex_parser::<DefaultId>(&vtype) {
        get_ptr(vertex_parser)
    } else {
        std::ptr::null::<c_void>()
    };
    std::mem::forget(parser);

    result
}

#[no_mangle]
pub extern "C" fn destroy_vertex_parser(ptr_parser: *const c_void) {
    destroy_ptr::<LDBCVertexParser>(ptr_parser)
}

/// Encode a row of vertex data into the internally formatted data
#[no_mangle]
pub extern "C" fn encode_vertex(
    ptr_parser: *const c_void, row_str: *const c_char, delim: i16,
) -> FfiVertexData {
    let vertex_parser = get_boxed_data::<LDBCVertexParser>(ptr_parser);
    let content = cstr_to_string(row_str);
    let delim_char = char::from(delim as u8);
    let row_iter = content.split(delim_char);
    let row_iter_cloned = row_iter.clone();

    let result = if let Ok(vertex_meta) = vertex_parser.parse_vertex_meta(row_iter) {
        if let Ok(properties) = vertex_parser.parse_properties(row_iter_cloned) {
            FfiVertexData {
                id: vertex_meta.global_id as i64,
                primary_label: vertex_meta.label[0] as i32,
                secondary_label: vertex_meta.label[1] as i32,
                property_bytes: serialize_properties(properties),
                code: Default::default(),
            }
        } else {
            FfiVertexData {
                id: vertex_meta.global_id as i64,
                primary_label: vertex_meta.label[0] as i32,
                secondary_label: vertex_meta.label[1] as i32,
                property_bytes: FfiPropertyBytes::default(),
                code: Default::default(),
            }
        }
    } else {
        FfiVertexData::with_error(ResultCode::ParseError)
    };

    std::mem::forget(vertex_parser);
    result
}

#[no_mangle]
pub extern "C" fn get_edge_parser(ptr_parser: *const c_void, edge_type: FfiEdgeTypeTuple) -> *const c_void {
    let parser = unsafe { Box::from_raw(ptr_parser as *mut LDBCParser) };
    let etype = edge_type.into();
    let result = if let Ok(edge_parser) = parser.get_edge_parser::<DefaultId>(&etype) {
        get_ptr(edge_parser)
    } else {
        std::ptr::null::<c_void>()
    };
    std::mem::forget(parser);

    result
}

#[no_mangle]
pub extern "C" fn destroy_edge_parser(ptr_parser: *const c_void) {
    destroy_ptr::<LDBCEdgeParser>(ptr_parser)
}

/// Encode a row of edge data into the internally formatted data
#[no_mangle]
pub extern "C" fn encode_edge(
    ptr_parser: *const c_void, row_str: *const c_char, delim: i16,
) -> FfiEdgeData {
    let edge_parser = get_boxed_data::<LDBCEdgeParser>(ptr_parser);
    let content = cstr_to_string(row_str);
    let delim_char = char::from(delim as u8);
    let row_iter = content.split(delim_char);
    let row_iter_cloned = row_iter.clone();
    let result = if let Ok(edge_meta) = edge_parser.parse_edge_meta(row_iter) {
        if let Ok(properties) = edge_parser.parse_properties(row_iter_cloned) {
            FfiEdgeData {
                label_id: edge_meta.label_id as i32,
                src_vertex_id: edge_meta.src_global_id as i64,
                src_label_id: edge_meta.src_label_id as i32,
                dst_vertex_id: edge_meta.dst_global_id as i64,
                dst_label_id: edge_meta.dst_label_id as i32,
                property_bytes: serialize_properties(properties),
                code: Default::default(),
            }
        } else {
            FfiEdgeData {
                label_id: edge_meta.label_id as i32,
                src_vertex_id: edge_meta.src_global_id as i64,
                src_label_id: edge_meta.src_label_id as i32,
                dst_vertex_id: edge_meta.dst_global_id as i64,
                dst_label_id: edge_meta.dst_label_id as i32,
                property_bytes: FfiPropertyBytes::default(),
                code: Default::default(),
            }
        }
    } else {
        FfiEdgeData::with_error(ResultCode::ParseError)
    };
    std::mem::forget(edge_parser);

    result
}

#[no_mangle]
pub extern "C" fn init_graph_loader(local_dir: *const c_char, partition: i64) -> *const c_void {
    let root_dir = cstr_to_string(local_dir);
    let graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
        .root_dir(&root_dir)
        .partition(partition as usize)
        .new();

    get_ptr(graph)
}

/// Initialize a buffer to write vertex
#[no_mangle]
pub extern "C" fn init_write_vertex(batch_size: i64) -> *const c_void {
    get_ptr(Vec::<(DefaultId, Label, Row)>::with_capacity(batch_size as usize))
}

/// Write a vertex (with property deserializing) into the buffer
#[no_mangle]
pub extern "C" fn write_vertex(buffer: *const c_void, data: FfiVertexData) -> bool {
    if data.code == ResultCode::Success {
        let mut container = get_boxed_data::<Vec<(DefaultId, Label, Row)>>(buffer);
        container.push((
            data.id as DefaultId,
            [data.primary_label as LabelId, data.secondary_label as LabelId],
            deserialize_properties::<Row>(data.property_bytes),
        ));
        std::mem::forget(container);

        true
    } else {
        false
    }
}

/// Finalize write vertices by flushing the buffer into the graph
#[no_mangle]
pub extern "C" fn finalize_write_vertex(graph_loader: *const c_void, buffer: *const c_void) -> i64 {
    let mut graph = get_boxed_data::<MutableGraphDB<DefaultId, InternalId>>(graph_loader);
    let container = get_boxed_data::<Vec<(DefaultId, Label, Row)>>(buffer);
    let num_vertices =
        if let Ok(count) = graph.add_vertex_batches(container.into_iter()) { count as i64 } else { 0 };
    std::mem::forget(graph);

    num_vertices
}

/// Initialize a buffer to write edge
#[no_mangle]
pub extern "C" fn init_write_edge(batch_size: i64) -> *const c_void {
    get_ptr(Vec::<(EdgeMeta<DefaultId>, Row)>::with_capacity(batch_size as usize))
}

/// Write an edge (with property deserializing) into the buffer
#[no_mangle]
pub extern "C" fn write_edge(buffer: *const c_void, data: FfiEdgeData) -> bool {
    if data.code == ResultCode::Success {
        let mut container = get_boxed_data::<Vec<(EdgeMeta<DefaultId>, Row)>>(buffer);
        container.push((
            EdgeMeta {
                src_global_id: data.src_vertex_id as DefaultId,
                src_label_id: data.src_label_id as LabelId,
                dst_global_id: data.dst_vertex_id as DefaultId,
                dst_label_id: data.dst_label_id as LabelId,
                label_id: data.label_id as LabelId,
            },
            deserialize_properties::<Row>(data.property_bytes),
        ));
        std::mem::forget(container);

        true
    } else {
        false
    }
}

/// Finalize write vertices by flushing the buffer into the graph
#[no_mangle]
pub extern "C" fn finalize_write_edge(
    graph_loader: *const c_void, buffer: *const c_void, curr_partition: i64, num_partitions: i64,
) -> i64 {
    let mut graph = get_boxed_data::<MutableGraphDB<DefaultId, InternalId>>(graph_loader);
    let container = get_boxed_data::<Vec<(EdgeMeta<DefaultId>, Row)>>(buffer);
    let mut count = 0;
    for (edge_meta, row) in container.into_iter() {
        if edge_meta.src_global_id as i64 % num_partitions == curr_partition {
            graph.add_vertex(edge_meta.src_global_id, [edge_meta.src_label_id, INVALID_LABEL_ID]);
        } else {
            graph.add_corner_vertex(edge_meta.src_global_id, edge_meta.src_label_id);
        }
        if edge_meta.dst_global_id as i64 % num_partitions == curr_partition {
            graph.add_vertex(edge_meta.dst_global_id, [edge_meta.dst_label_id, INVALID_LABEL_ID]);
        } else {
            graph.add_corner_vertex(edge_meta.dst_global_id, edge_meta.dst_label_id);
        }
        let result = if row.is_empty() {
            graph.add_edge(edge_meta.src_global_id, edge_meta.dst_global_id, edge_meta.label_id)
        } else {
            graph
                .add_edge_with_properties(
                    edge_meta.src_global_id,
                    edge_meta.dst_global_id,
                    edge_meta.label_id,
                    row,
                )
                .is_ok()
        };
        if result {
            count += 1;
        }
    }
    std::mem::forget(graph);

    count as i64
}

#[no_mangle]
pub extern "C" fn finalize_graph_loading(graph_loader: *const c_void) -> ResultCode {
    let graph = get_boxed_data::<MutableGraphDB<DefaultId, InternalId>>(graph_loader);
    if graph.export().is_ok() {
        ResultCode::Success
    } else {
        ResultCode::DumpGraphError
    }
}
