use crate::db::common::bytes::util::{UnsafeBytesWriter, UnsafeBytesReader};
use crate::db::common::bytes::transform;
use crate::db::api::*;
use super::table_manager::TableId;

pub fn vertex_key(table_id: TableId, id: VertexId, ts: SnapshotId) -> [u8; 24] {
    let mut ret = [0; 24];
    let mut writer = UnsafeBytesWriter::new(&mut ret);
    let prefix = vertex_table_prefix(table_id);
    writer.write_i64(0, prefix.to_be());
    writer.write_i64(8, id.to_be());
    writer.write_i64(16, (!ts).to_be());
    ret
}

/// return (vertex_id, ts)
pub fn parse_vertex_key(key: &[u8]) -> GraphResult<(VertexId, SnapshotId)> {
    if key.len() != 24 {
        let msg = format!("invalid key, key len is {}", key.len());
        let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, parse_vertex_key);
        error!("parse_vertex_key failed. error: {:?}", err);
        return Err(err);
    }
    let reader = UnsafeBytesReader::new(key);
    let vertex_id = reader.read_i64(8).to_be();
    let ts = !reader.read_i64(16).to_be();
    Ok((vertex_id, ts))
}

/// A storage will contain vertex tables and edge tables, and it uses different prefix to identify
/// them. Edge table has two parts: forward edges and backward edges, so they should be distinguished.
/// If an edge table's id is X, we use 2 * X to identify the forward edges and 2 * X + 1 to identify
/// the backward edges. And to avoid duplication of vertex tables' prefixes and edge tables', we using
/// 2 * X to identify vertex table. As any type has an unique table id, all prefixes will be different.
fn vertex_table_prefix(id: TableId) -> i64 {
    id << 1
}

pub fn vertex_table_prefix_key(table_id: TableId) -> [u8; 8] {
    let prefix = vertex_table_prefix(table_id);
    transform::i64_to_arr(prefix.to_be())
}

pub fn edge_table_prefix(table_id: TableId, direction: EdgeDirection) -> i64 {
    match direction {
        EdgeDirection::Out => table_id << 1,
        EdgeDirection::In => table_id << 1 | 1,
        EdgeDirection::Both => unreachable!(),
    }
}

pub fn edge_key(table_id: TableId, id: EdgeId, direction: EdgeDirection, ts: SnapshotId) -> [u8; 40]  {
    let mut ret = [0u8; 40];
    let mut writer = UnsafeBytesWriter::new(&mut ret);
    let (x, y, z, w) = match direction {
        EdgeDirection::In => {
            (table_id << 1 | 1, id.dst_id, id.src_id, id.inner_id)
        },
        EdgeDirection::Out => {
            (table_id << 1, id.src_id, id.dst_id, id.inner_id)
        }
        _ => unreachable!(),
    };
    writer.write_i64(0, x.to_be());
    writer.write_i64(8, y.to_be());
    writer.write_i64(16, z.to_be());
    writer.write_i64(24, w.to_be());
    writer.write_i64(32, (!ts).to_be());
    ret
}

pub fn edge_table_prefix_key(table_id: TableId, direction: EdgeDirection) -> [u8; 8] {
    let prefix = edge_table_prefix(table_id, direction);
    transform::i64_to_arr(prefix.to_be())
}

/// return (edge_id, ts)
pub fn parse_edge_key(key: &[u8]) -> (EdgeId, SnapshotId) {
    let reader = UnsafeBytesReader::new(key);
    let prefix = reader.read_i64(0).to_be();
    let id1 = reader.read_i64(8).to_be();
    let id2 = reader.read_i64(16).to_be();
    let id = reader.read_i64(24).to_be();
    let ts = !reader.read_i64(32).to_be();
    if (prefix & 1) == 0 {
        // out direction
        (EdgeId::new(id1, id2, id), ts)
    } else {
        (EdgeId::new(id2, id1, id), ts)
    }
}

pub fn edge_prefix(id: TableId, vertex_id: VertexId, direction: EdgeDirection) -> [u8; 16] {
    let prefix = edge_table_prefix(id, direction);
    let mut ret = [0; 16];
    let mut writer = UnsafeBytesWriter::new(&mut ret);
    writer.write_i64(0, prefix.to_be());
    writer.write_i64(8, vertex_id.to_be());
    ret
}
