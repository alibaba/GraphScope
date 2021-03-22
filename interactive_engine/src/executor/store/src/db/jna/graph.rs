#![allow(non_snake_case)]
use std::os::raw::{c_char, c_void};
use std::ffi::CStr;
use std::str;
use crate::db::api::{GraphConfigBuilder, SnapshotId, GraphResult, GraphStorage, Vertex, TypeDef, EdgeId, EdgeKind, GraphError};
use crate::db::jna::jna_response::JnaResponse;
use crate::db::api::PropertyMap;
use crate::db::proto::common::{OpTypePb, OperationBatchPb, OperationPb, DataOperationPb, VertexIdPb, LabelIdPb, EdgeIdPb, EdgeKindPb, TypeDefPb, DdlOperationPb, ConfigPb, CreateVertexTypePb, AddEdgeKindPb};
use crate::db::graph::store::GraphStore;
use crate::db::common::bytes::util::parse_pb;
use std::sync::Once;

pub type GraphHandle = *const c_void;

static INIT: Once = Once::new();

#[no_mangle]
pub extern fn openGraphStore(config_bytes: *const u8, len: usize) -> GraphHandle {
    let buf = unsafe { ::std::slice::from_raw_parts(config_bytes, len) };
    let proto = parse_pb::<ConfigPb>(buf).expect("parse config pb failed");
    let mut config_builder = GraphConfigBuilder::new();
    config_builder.set_storage_engine("rocksdb");
    config_builder.set_storage_options(proto.get_configs().clone());
    let config = config_builder.build();
    let path = config.get_storage_option("store.data.path")
        .expect("invalid config, missing store.data.path");
    INIT.call_once(|| {
        if let Some(config_file) = config.get_storage_option("log4rs.config") {
            log4rs::init_file(config_file, Default::default()).expect("init log4rs failed");
            info!("log4rs inited, config file: {}", config_file);
        } else {
            println!("No valid log4rs.config, rust won't print logs");
        }
    });
    let handle = Box::new(GraphStore::open(&config, path).unwrap());
    let ret = Box::into_raw(handle);
    ret as GraphHandle
}

#[no_mangle]
pub extern fn closeGraphStore(handle: GraphHandle) -> bool {
    let graph_store_ptr = handle as *mut GraphStore;
    unsafe {
        Box::from_raw(graph_store_ptr);
    }
    true
}

#[no_mangle]
pub extern fn getGraphDefBlob(ptr: GraphHandle) -> Box<JnaResponse> {
    unsafe {
        let graph_store_ptr = &*(ptr as *const GraphStore);
        match graph_store_ptr.get_graph_def_blob() {
            Ok(blob) => {
                let mut response = JnaResponse::new_success();
                if let Err(e) = response.data(blob) {
                    response.success(false);
                    let msg = format!("{:?}", e);
                    response.err_msg(&msg);
                }
                response
            },
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            },
        }
    }
}

#[no_mangle]
pub extern fn ingestData(ptr: GraphHandle, path: *const c_char) -> Box<JnaResponse> {
    unsafe {
        let graph_store_ptr = &*(ptr as *const GraphStore);
        let slice =  CStr::from_ptr(path).to_bytes();
        let path_str = str::from_utf8(slice).unwrap();
        match graph_store_ptr.ingest(path_str) {
            Ok(_) => {
                JnaResponse::new_success()
            },
            Err(e) => {
                let msg = format!("{:?}", e);
                JnaResponse::new_error(&msg)
            },
        }
    }
}

#[no_mangle]
pub extern fn writeBatch(ptr: GraphHandle, snapshot_id: i64, data: *const u8, len: usize) -> Box<JnaResponse> {
    unsafe {
        let graph_store_ptr = &*(ptr as *const GraphStore);
        let buf = ::std::slice::from_raw_parts(data, len);
        let ret = match do_write_batch(graph_store_ptr, snapshot_id, buf) {
            Ok(has_ddl) => {
                let mut response = JnaResponse::new_success();
                response.has_ddl(has_ddl);
                response
            },
            Err(e) => {
                let err_msg = format!("{:?}", e);
                JnaResponse::new_error(&err_msg)
            }
        };
        return ret;
    }
}

fn do_write_batch<G: GraphStorage>(graph: &G, snapshot_id: SnapshotId, buf: &[u8]) -> GraphResult<bool> {
    let proto = parse_pb::<OperationBatchPb>(buf)?;
    let mut has_ddl = false;
    let operations = proto.get_operations();
    if operations.is_empty() {
        return Ok(false);
    }
    for op in operations {
        match op.get_opType() {
            OpTypePb::MARKER => {},
            // Data
            OpTypePb::OVERWRITE_VERTEX => overwrite_vertex(graph, snapshot_id, op)?,
            OpTypePb::UPDATE_VERTEX => update_vertex(graph, snapshot_id, op)?,
            OpTypePb::DELETE_VERTEX => delete_vertex(graph, snapshot_id, op)?,
            OpTypePb::OVERWRITE_EDGE => overwrite_edge(graph, snapshot_id, op)?,
            OpTypePb::UPDATE_EDGE => update_edge(graph, snapshot_id, op)?,
            OpTypePb::DELETE_EDGE => delete_edge(graph, snapshot_id, op)?,
            // Ddl
            OpTypePb::CREATE_VERTEX_TYPE => {
                if create_vertex_type(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
            OpTypePb::CREATE_EDGE_TYPE => {
                if create_edge_type(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
            OpTypePb::ADD_EDGE_KIND => {
                if add_edge_kind(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
            OpTypePb::DROP_VERTEX_TYPE => {
                if drop_vertex_type(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
            OpTypePb::DROP_EDGE_TYPE => {
                if drop_edge_type(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
            OpTypePb::REMOVE_EDGE_KIND => {
                if remove_edge_kind(graph, snapshot_id, op)? {
                    has_ddl = true;
                }
            },
        };
    }
    Ok(has_ddl)
}

fn create_vertex_type<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let create_vertex_type_pb = parse_pb::<CreateVertexTypePb>(ddl_operation_pb.get_ddlBlob())?;
    let table_id = create_vertex_type_pb.get_tableIdx();
    let typedef_pb = create_vertex_type_pb.get_typeDef();
    let label_id = typedef_pb.get_labelId().get_id();
    let typedef = TypeDef::from_proto(&typedef_pb)?;
    graph.create_vertex_type(snapshot_id, schema_version, label_id, &typedef, table_id)
}

fn drop_vertex_type<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let label_id_pb = parse_pb::<LabelIdPb>(ddl_operation_pb.get_ddlBlob())?;
    let label_id = label_id_pb.get_id();
    graph.drop_vertex_type(snapshot_id, schema_version, label_id)
}

fn create_edge_type<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let typedef_pb = parse_pb::<TypeDefPb>(ddl_operation_pb.get_ddlBlob())?;
    let label_id = typedef_pb.get_labelId().get_id();
    let typedef = TypeDef::from_proto(&typedef_pb)?;
    graph.create_edge_type(snapshot_id, schema_version, label_id, &typedef)
}

fn drop_edge_type<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let label_id_pb = parse_pb::<LabelIdPb>(ddl_operation_pb.get_ddlBlob())?;
    let label_id = label_id_pb.get_id();
    graph.drop_edge_type(snapshot_id, schema_version, label_id)
}

fn add_edge_kind<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let add_edge_kind_pb = parse_pb::<AddEdgeKindPb>(ddl_operation_pb.get_ddlBlob())?;
    let table_id = add_edge_kind_pb.get_tableIdx();
    let edge_kind_pb = add_edge_kind_pb.get_edgeKind();
    let edge_kind = EdgeKind::from_proto(&edge_kind_pb);
    graph.add_edge_kind(snapshot_id, schema_version, &edge_kind, table_id)
}

fn remove_edge_kind<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<bool> {
    let ddl_operation_pb = parse_pb::<DdlOperationPb>(op.get_dataBytes())?;
    let schema_version = ddl_operation_pb.get_schemaVersion();
    let edge_kind_pb = parse_pb::<EdgeKindPb>(ddl_operation_pb.get_ddlBlob())?;
    let edge_kind = EdgeKind::from_proto(&edge_kind_pb);
    graph.remove_edge_kind(snapshot_id, schema_version, &edge_kind)
}

fn overwrite_vertex<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let vertex_id_pb = parse_pb::<VertexIdPb>(data_operation_pb.get_keyBlob())?;
    let vertex_id = vertex_id_pb.get_id();

    let label_id_pb = parse_pb::<LabelIdPb>(data_operation_pb.get_locationBlob())?;
    let label_id = label_id_pb.get_id();

    let property_map = PropertyMap::from_proto(data_operation_pb.get_props());
    graph.insert_overwrite_vertex(snapshot_id, vertex_id, label_id, &property_map)
}

fn update_vertex<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let vertex_id_pb = parse_pb::<VertexIdPb>(data_operation_pb.get_keyBlob())?;
    let vertex_id = vertex_id_pb.get_id();

    let label_id_pb = parse_pb::<LabelIdPb>(data_operation_pb.get_locationBlob())?;
    let label_id = label_id_pb.get_id();

    let property_map = PropertyMap::from_proto(data_operation_pb.get_props());
    graph.insert_update_vertex(snapshot_id, vertex_id, label_id, &property_map)
}

fn delete_vertex<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let vertex_id_pb = parse_pb::<VertexIdPb>(data_operation_pb.get_keyBlob())?;
    let vertex_id = vertex_id_pb.get_id();

    let label_id_pb = parse_pb::<LabelIdPb>(data_operation_pb.get_locationBlob())?;
    let label_id = label_id_pb.get_id();

    graph.delete_vertex(snapshot_id, vertex_id, label_id)
}

fn overwrite_edge<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let edge_id_pb = parse_pb::<EdgeIdPb>(data_operation_pb.get_keyBlob())?;
    let edge_id = EdgeId::from_proto(&edge_id_pb);

    let edge_kind_pb = parse_pb::<EdgeKindPb>(data_operation_pb.get_locationBlob())?;
    let edge_kind = EdgeKind::from_proto(&edge_kind_pb);

    let property_map = PropertyMap::from_proto(data_operation_pb.get_props());
    graph.insert_overwrite_edge(snapshot_id, edge_id, &edge_kind, &property_map)
}

fn update_edge<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let edge_id_pb = parse_pb::<EdgeIdPb>(data_operation_pb.get_keyBlob())?;
    let edge_id = EdgeId::from_proto(&edge_id_pb);

    let edge_kind_pb = parse_pb::<EdgeKindPb>(data_operation_pb.get_locationBlob())?;
    let edge_kind = EdgeKind::from_proto(&edge_kind_pb);

    let property_map = PropertyMap::from_proto(data_operation_pb.get_props());
    graph.insert_update_edge(snapshot_id, edge_id, &edge_kind, &property_map)
}

fn delete_edge<G: GraphStorage>(graph: &G, snapshot_id: i64, op: &OperationPb) -> GraphResult<()> {
    let data_operation_pb = parse_pb::<DataOperationPb>(op.get_dataBytes())?;

    let edge_id_pb = parse_pb::<EdgeIdPb>(data_operation_pb.get_keyBlob())?;
    let edge_id = EdgeId::from_proto(&edge_id_pb);

    let edge_kind_pb = parse_pb::<EdgeKindPb>(data_operation_pb.get_locationBlob())?;
    let edge_kind = EdgeKind::from_proto(&edge_kind_pb);

    graph.delete_edge(snapshot_id, edge_id, &edge_kind)
}
