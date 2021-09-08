#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use crate::db::api::*;
use crate::db::storage::ExternalStorage;
use crate::db::storage::rocksdb::RocksDB;
use crate::db::util::lock::GraphMutexLock;
use super::vertex::*;
use super::edge::*;
use super::types::*;
use super::codec::*;
use super::property::*;
use super::meta::*;
use super::bin::*;
use protobuf::Message;
use crate::db::api::GraphErrorCode::{InvalidData, TypeNotFound};
use crate::db::graph::table_manager::Table;

pub struct GraphStore {
    config: GraphConfig,
    meta: Meta,
    vertex_manager: VertexTypeManager,
    edge_manager: EdgeTypeManager,
    storage: Arc<dyn ExternalStorage>,
    // ensure all modification to graph is in ascending order of snapshot_id
    si_guard: AtomicIsize,
    lock: GraphMutexLock<()>,
}

impl GraphStorage for GraphStore {
    type V = VertexImpl;
    type E = EdgeImpl;

    fn get_vertex(&self, si: SnapshotId, id: VertexId, label: Option<LabelId>) -> GraphResult<Option<VertexWrapper<Self::V>>> {
        if let Some(l) = label {
            let res = self.vertex_manager.get_type(si, l).and_then(|info| {
                self.do_get_vertex(si, id, &info)
            });
            res_unwrap!(res, get_vertex, si, id, label)
        } else {
            let mut iter = self.vertex_manager.get_all(si);
            while let Some(info) = iter.next() {
                let res = self.do_get_vertex(si, id, &info);
                if let Some(v) = res_unwrap!(res, get_vertex, si, id, label)? {
                    return Ok(Some(v));
                }
            }
            Ok(None)
        }
    }

    fn get_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: Option<&EdgeKind>) -> GraphResult<Option<EdgeWrapper<Self::E>>> {
        if let Some(t) = edge_kind {
            let res = self.edge_manager.get_edge_kind(si, t).and_then(|info| {
                self.do_get_edge(si, id, info, EdgeDirection::Out)
            });
            res_unwrap!(res, get_edge, si, id, edge_kind)
        } else {
            let mut iter = self.edge_manager.get_all_edges(si);
            while let Some(info) = iter.next() {
                let mut type_iter = info.into_iter();
                while let Some(t) = type_iter.next() {
                    let res = self.do_get_edge(si, id, t, EdgeDirection::Out);
                    if let Some(e) = res_unwrap!(res, get_edge, si, id, edge_kind)? {
                        return Ok(Some(e));
                    }
                }
            }
            Ok(None)
        }
    }

    fn query_vertices<'a>(&'a self, si: i64, label: Option<i32>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn VertexResultIter<V=Self::V> + 'a>> {
        if let Some(label) = label {
            self.do_query_vertices(si, label, condition)
        } else {
            let mut info_iter = self.vertex_manager.get_all(si);
            let mut iters = Vec::new();
            while let Some(info) = info_iter.next() {
                let res = SingleLabelVertexIter::create(si, info, self.storage.as_ref(), condition.clone());
                match res_unwrap!(res, query_vertices, si, label)? {
                    Some(iter) => iters.push(iter),
                    None => {},
                }
            }
            let ret = MultiLabelsVertexIter::new(iters);
            Ok(Box::new(ret))
        }
    }

    fn get_out_edges<'a>(&'a self, si: i64, src_id: i64, label: Option<i32>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>> {
        let res = self.do_query_edges(si, src_id, label, EdgeDirection::Out, condition);
        res_unwrap!(res, get_out_edges, si, src_id, label)
    }

    fn get_in_edges<'a>(&'a self, si: i64, dst_id: i64, label: Option<i32>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>> {
        let res = self.do_query_edges(si, dst_id, label, EdgeDirection::In, condition);
        res_unwrap!(res, get_in_edges, si, dst_id, label)
    }

    fn query_edges<'a>(&'a self, si: i64, label: Option<i32>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>> {
        let res = self.do_query_edges(si, 0, label, EdgeDirection::Both, condition);
        res_unwrap!(res, query_edges, si, label)
    }

    fn create_vertex_type(&self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef, table_id: i64) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), create_vertex_type)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if self.vertex_manager.contains_type(si, label_id) {
            let msg = format!("vertex#{} already exists", label_id);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_vertex_type);
            return Err(err);
        }
        self.meta.create_vertex_type(si, schema_version, label_id, type_def, table_id).and_then(|table| {
            let codec = Codec::from(type_def);
            self.vertex_manager.create_type(si, label_id, codec, table)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn create_edge_type(&self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), create_edge_type)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if self.edge_manager.contains_edge(label_id) {
            let msg = format!("edge#{} already exists", label_id);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_edge);
            return Err(err);
        }
        self.meta.create_edge_type(si, schema_version, label_id, type_def).and_then(|_| {
            self.edge_manager.create_edge_type(si, label_id, type_def)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn add_edge_kind(&self, si: i64, schema_version: i64, edge_kind: &EdgeKind, table_id: i64) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), add_edge_kind)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if self.edge_manager.contains_edge_kind(si, edge_kind) {
            let msg = format!("{:?} already exists", edge_kind);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add_edge_kind, si, edge_kind);
            return Err(err);
        }
        self.meta.add_edge_kind(si, schema_version, edge_kind, table_id).and_then(|table| {
            self.edge_manager.add_edge_kind(si, edge_kind)?;
            let info = self.edge_manager.get_edge_kind(si, edge_kind)?;
            info.online_table(table)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn drop_vertex_type(&self, si: i64, schema_version: i64, label_id: LabelId) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), drop_vertex_type, si, label_id)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta.drop_vertex_type(si, schema_version, label_id).and_then(|_| {
            self.vertex_manager.drop_type(si, label_id)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn drop_edge_type(&self, si: i64, schema_version: i64, label_id: LabelId) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), drop_edge_type, si, label_id)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta.drop_edge_type(si, schema_version, label_id).and_then(|_| {
            self.edge_manager.drop_edge_type(si, label_id)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn remove_edge_kind(&self, si: i64, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), remove_edge_kind, si, edge_kind)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta.remove_edge_kind(si, schema_version, edge_kind).and_then(|_| {
            self.edge_manager.remove_edge_kind(si, edge_kind)
        }).map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn insert_overwrite_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let res = self.vertex_manager.get_type(si, label).and_then(|info| {
            self.do_insert_vertex_data(si, info, id, properties)
        }).map(|_| self.update_si_guard(si));
        res_unwrap!(res, insert_overwrite_vertex, si, id, label)
    }

    fn insert_update_vertex(&self, si: i64, id: i64, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.vertex_manager.get_type(si, label), si, id, label)?;
        match res_unwrap!(self.get_vertex_data(si, id, &info), insert_update_vertex, si, id, label)? {
            Some(data) => {
                let version = get_codec_version(data);
                let decoder = info.get_decoder(si, version)?;
                let mut old = decoder.decode_all(data);
                merge_updates(&mut old, properties);
                let res = self.do_insert_vertex_data(si, info, id, &old).map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_vertex, si, id, label)
            }
            None => {
                let res = self.do_insert_vertex_data(si, info, id, properties).map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_vertex, si, id, label)
            }
        }
    }

    fn delete_vertex(&self, si: i64, id: i64, label: LabelId) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.vertex_manager.get_type(si, label), si, id, label)?;
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = vertex_key(table.id, id, ts);
            let res = self.storage.put(&key, &[]);
            return res_unwrap!(res, delete_vertex, si, id, label);
        }
        self.update_si_guard(si);
        Ok(())
    }

    fn insert_overwrite_edge(&self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let direction = if forward {
            EdgeDirection::Out
        } else {
            EdgeDirection::In
        };
        let res = self.edge_manager.get_edge_kind(si, edge_kind).and_then(|info| {
            self.do_insert_edge_data(si, id, info, direction, properties)
        }).map(|_| self.update_si_guard(si));
        res_unwrap!(res, insert_overwrite_edge, si, id, edge_kind)
    }

    fn insert_update_edge(&self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.edge_manager.get_edge_kind(si, edge_kind), insert_update_edge, si, id, edge_kind)?;
        let direction = if forward {
            EdgeDirection::Out
        } else {
            EdgeDirection::In
        };
        let data_res = self.get_edge_data(si, id, &info, direction);
        match res_unwrap!(data_res, insert_update_edge, si, id, edge_kind)? {
            Some(data) => {
                let version = get_codec_version(data);
                let decoder = info.get_decoder(si, version)?;
                let mut old = decoder.decode_all(data);
                merge_updates(&mut old, properties);
                let res = self.do_insert_edge_data(si, id, info, direction, &old).map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_edge, si, id, edge_kind)
            }
            None => {
                let res = self.do_insert_edge_data(si, id, info, direction, properties).map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_edge, si, id, edge_kind)
            }
        }
    }

    fn delete_edge(&self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool) -> GraphResult<()> {
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.edge_manager.get_edge_kind(si, edge_kind), si, id, edge_kind)?;
        let direction = if forward {
            EdgeDirection::Out
        } else {
            EdgeDirection::In
        };
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = edge_key(table.id, id, direction, ts);
            res_unwrap!(self.storage.put(&key, &[]), delete_edge, si, id, edge_kind)?;
        }
        self.update_si_guard(si);
        Ok(())
    }

    fn gc(&self, _si: i64) {
        unimplemented!()
    }

    fn get_graph_def_blob(&self) -> GraphResult<Vec<u8>> {
        let graph_def = self.meta.get_graph_def().lock()?;
        let pb = graph_def.to_proto()?;
        pb.write_to_bytes().map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))
    }

    fn prepare_data_load(&self, si: i64, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), prepare_data_load)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta.prepare_data_load(si, schema_version, target, table_id)?;
        Ok(true)
    }

    fn commit_data_load(&self, si: i64, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), prepare_data_load)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta.commit_data_load(si, schema_version, target, table_id)?;
        if target.src_label_id > 0 {
            let edge_kind = EdgeKind::new(target.label_id, target.src_label_id, target.dst_label_id);
            let info = self.edge_manager.get_edge_kind(si, &edge_kind)?;
            info.online_table(Table::new(si, table_id))?;
            info!("online edge. target {:?}, tableId {}, si {}", target, table_id, si);
        } else {
            let info = self.vertex_manager.get_type(si, target.label_id)?;
            info.online_table(Table::new(si, table_id))?;
            info!("online vertex. labelId {}, tableId {}, si {}", target.label_id, table_id, si);
        }
        Ok(true)
    }
}

impl GraphStore {
    pub fn open(config: &GraphConfig, path: &str) -> GraphResult<Self> {
        match config.get_storage_engine() {
            "rocksdb" => {
                let res = RocksDB::open(config.get_storage_options(), path).and_then(|db| {
                    let storage = Arc::new(db);
                    Self::init(config, storage)
                });
                res_unwrap!(res, open, config, path)
            }
            "alibtree" => {
                let msg = format!("alibtree is not supported yet");
                let err = gen_graph_err!(GraphErrorCode::NotSupported, msg, open, config, path);
                Err(err)
            }
            unknown => {
                let msg = format!("unknown storage {}", unknown);
                let err = gen_graph_err!(GraphErrorCode::NotSupported, msg, open, config, path);
                Err(err)
            }
        }
    }

    fn init(config: &GraphConfig, storage: Arc<dyn ExternalStorage>) -> GraphResult<Self> {
        let meta = Meta::new(storage.clone());
        let (vertex_manager, edge_manager) = res_unwrap!(meta.recover(), init)?;
        let ret = GraphStore {
            config: config.clone(),
            meta,
            vertex_manager,
            edge_manager,
            storage,
            si_guard: AtomicIsize::new(0),
            lock: GraphMutexLock::new(()),
        };
        Ok(ret)
    }

    fn get_vertex_data(&self, si: SnapshotId, id: VertexId, info: &VertexTypeInfoRef) -> GraphResult<Option<&[u8]>> {
        if let Some(table) = info.get_table(si) {
            let key = vertex_key(table.id, id, si - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..16] == key[0..16] && v.len() >= 4 {
                    let ret = unsafe { std::mem::transmute(v) };
                    return Ok(Some(ret));
                }
            }
        }
        Ok(None)
    }

    fn do_get_vertex(&self, si: SnapshotId, id: VertexId, info: &VertexTypeInfoRef) -> GraphResult<Option<VertexWrapper<VertexImpl>>> {
        let data = self.get_vertex_data(si, id, info)?;
        if let Some(v) = data {
            let version = get_codec_version(v);
            let decoder = res_unwrap!(info.get_decoder(si, version), do_get_vertex)?;
            let ret = VertexImpl::new(id, info.get_label(), PropData::Owned(v.to_vec()), decoder);
            return Ok(Some(VertexWrapper::new(ret)));
        }
        Ok(None)
    }

    fn do_query_vertices<'a>(&'a self, si: SnapshotId, label: LabelId, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn VertexResultIter<V=VertexImpl> + 'a>> {
        let res = self.vertex_manager.get_type(si, label)
            .and_then(|type_info| SingleLabelVertexIter::create(si, type_info, self.storage.as_ref(), condition));
        match res_unwrap!(res, do_query_vertices, si, label) {
            Ok(iter_option) => {
                match iter_option {
                    Some(iter) => Ok(Box::new(iter)),
                    None => Ok(Box::new(EmptyResultIter)),
                }
            },
            Err(e) => {
                if let TypeNotFound = e.get_error_code()  {
                    Ok(Box::new(EmptyResultIter))
                } else {
                    Err(e)
                }
            },
        }
    }

    fn do_query_edges<'a>(&'a self, si: SnapshotId, id: VertexId, label: Option<LabelId>, direction: EdgeDirection, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=EdgeImpl> + 'a>> {
        let storage = self.storage.as_ref();
        if let Some(label) = label {
            let res = self.edge_manager.get_edge(si, label)
                .and_then(|info| SingleLabelEdgeIter::create(si, id, direction, info, storage, condition));
            match res_unwrap!(res, do_query_edges, si, id, label, direction) {
                Ok(iter) => Ok(Box::new(iter)),
                Err(e) => {
                    if let TypeNotFound = e.get_error_code() {
                        Ok(Box::new(EmptyResultIter))
                    } else {
                        Err(e)
                    }
                },
            }
        } else {
            let info_iter = self.edge_manager.get_all_edges(si);
            let res = MultiLabelsEdgeIter::create(si, id, direction, info_iter, storage, condition);
            let ret = res_unwrap!(res, do_query_edges, si, id, label)?;
            Ok(Box::new(ret))
        }
    }

    fn get_edge_data(&self, si: SnapshotId, id: EdgeId, info: &EdgeKindInfoRef, direction: EdgeDirection) -> GraphResult<Option<&[u8]>> {
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = edge_key(table.id, id, direction, ts);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..32] == key[0..32] && v.len() >= 4 {
                    let ret = unsafe { std::mem::transmute(v) };
                    return Ok(Some(ret));
                }
            }
        }
        Ok(None)
    }

    fn do_get_edge(&self, si: SnapshotId, id: EdgeId, info: EdgeKindInfoRef, direction: EdgeDirection) -> GraphResult<Option<EdgeWrapper<EdgeImpl>>> {
        let data = self.get_edge_data(si, id, &info, direction)?;
        if let Some(v) = data {
            let version = get_codec_version(v);
            let decoder = res_unwrap!(info.get_decoder(si, version))?;
            let edge_kind = info.get_type().clone();
            let ret = EdgeImpl::new(id, edge_kind, PropData::from(v.to_vec()), decoder);
            return Ok(Some(EdgeWrapper::new(ret)));
        }
        Ok(None)
    }

    fn do_insert_vertex_data(&self, si: SnapshotId, info: VertexTypeInfoRef, id: VertexId, properties: &dyn PropertyMap) -> GraphResult<()> {
        if let Some(table) = info.get_table(si) {
            let encoder = res_unwrap!(info.get_encoder(si), do_insert_vertex_data)?;
            let mut buf = Vec::new();
            return encoder.encode(properties, &mut buf).and_then(|_| {
                let ts = si - table.start_si;
                let key = vertex_key(table.id, id, ts);
                self.storage.put(&key, &buf)
            });
        }
        let msg = format!("table not found at {} of vertex#{}", si, info.get_label());
        let err = gen_graph_err!(GraphErrorCode::DataNotExists, msg, do_insert_vertex_data);
        Err(err)
    }

    fn do_insert_edge_data(&self, si: SnapshotId, edge_id: EdgeId, info: EdgeKindInfoRef, direction: EdgeDirection, properties: &dyn PropertyMap) -> GraphResult<()> {
        if let Some(table) = info.get_table(si) {
            let encoder = res_unwrap!(info.get_encoder(si), do_insert_edge_data)?;
            let mut buf = Vec::new();
            return encoder.encode(properties, &mut buf).and_then(|_| {
                let ts = si - table.start_si;
                let key = edge_key(table.id, edge_id, direction, ts);
                self.storage.put(&key, &buf)
            });
        }
        let msg = format!("table not found at {} of {:?}", si, info.get_type());
        let err = gen_graph_err!(GraphErrorCode::DataNotExists, msg, do_insert_edge_data);
        Err(err)
    }

    fn check_si_guard(&self, si: SnapshotId) -> GraphResult<()> {
        let guard = self.si_guard.load(Ordering::Relaxed) as SnapshotId;
        if si <  guard {
            let msg = format!("si#{} is less than current si_guard#{}", si, guard);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg);
            return Err(err);
        }
        Ok(())
    }

    fn update_si_guard(&self, si: SnapshotId) {
        self.si_guard.store(si as isize, Ordering::Relaxed);
    }

    pub fn ingest(&self, data_path: &str) -> GraphResult<()> {
        let p = [data_path];
        self.storage.load(&p)
    }

    pub fn get_graph_def(&self) -> GraphResult<GraphDef> {
        let graph_def = self.meta.get_graph_def().lock()?;
        Ok((&*graph_def).clone())
    }
}

fn merge_updates<'a>(old: &mut HashMap<PropId, ValueRef<'a>>, updates: &'a dyn PropertyMap) {
    for (prop_id, v) in updates.as_map() {
        old.insert(prop_id, v);
    }
}

pub struct EmptyResultIter;

impl VertexResultIter for EmptyResultIter {
    type V = VertexImpl;

    fn next(&mut self) -> Option<VertexWrapper<Self::V>> {
        None
    }

    fn ok(&self) -> GraphResult<()> {
        Ok(())
    }
}

impl EdgeResultIter for EmptyResultIter {
    type E = EdgeImpl;

    fn next(&mut self) -> Option<EdgeWrapper<Self::E>> {
        None
    }

    fn ok(&self) -> GraphResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests;
    use crate::db::util::fs;

    #[test]
    fn test_get_vertex() {
        let path = "test_get_vertex";
        do_test(path, |graph| tests::vertex::test_get_vertex(graph));
    }

    #[test]
    fn test_query_vertices() {
        let path = "test_query_vertices";
        do_test(path, |graph| tests::vertex::test_query_vertices(graph));
    }

    #[test]
    fn test_update_vertex() {
        let path = "test_update_vertex";
        do_test(path, |graph| tests::vertex::test_update_vertex(graph));
    }

    #[test]
    fn test_delete_vertex() {
        let path = "test_delete_vertex";
        do_test(path, |graph| tests::vertex::test_delete_vertex(graph));
    }

    #[test]
    fn test_drop_vertex_type() {
        let path = "test_drop_vertex_type";
        do_test(path, |graph| tests::vertex::test_drop_vertex_type(graph));
    }

    #[test]
    fn test_get_edge() {
        let path = "test_get_edge";
        do_test(path, |graph| tests::edge::test_get_edge(graph));
    }

    #[test]
    fn test_query_edges() {
        let path = "test_query_edges";
        do_test(path, |graph| tests::edge::test_query_edges(graph));
    }

    #[test]
    fn test_get_in_out_edges() {
        let path = "test_get_in_out_edges";
        do_test(path, |graph| tests::edge::test_get_in_out_edges(graph));
    }

    #[test]
    fn test_update_edge() {
        let path = "test_update_edge";
        do_test(path, |graph| tests::edge::test_update_edge(graph));
    }

    #[test]
    fn test_delete_edge() {
        let path = "test_delete_edge";
        do_test(path, |graph| tests::edge::test_delete_edge(graph));
    }

    #[test]
    fn test_drop_edge() {
        let path = "test_drop_edge";
        do_test(path, |graph| tests::edge::test_drop_edge(graph));
    }

    #[test]
    fn test_remove_edge_kind() {
        let path = "edge_kind";
        do_test(path, |graph| tests::edge::test_remove_edge_kind(graph));
    }

    #[test]
    fn test_si_guard() {
        let path = "test_si_guard";
        do_test(path, |graph| tests::graph::test_si_guard(graph));
    }

    fn do_test<F: Fn(GraphStore)>(path: &str, func: F) {
        let path = format!("store_test/{}", path);
        fs::rmr(&path).unwrap();
        let graph = create_empty_graph(&path);
        func(graph);
        fs::rmr(&path).unwrap();
    }

    pub fn create_empty_graph(path: &str) -> GraphStore {
        let mut builder = GraphConfigBuilder::new();
        builder.set_storage_engine("rocksdb");
        let config = builder.build();
        GraphStore::open(&config, path).unwrap()
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use super::super::bench;
    use crate::db::util::fs;

    #[ignore]
    #[test]
    fn bench_insert_overwrite_vertex() {
        let path = "bench_insert_overwrite_vertex";
        do_bench(path, |graph| bench::graph::bench_insert_vertex(graph));
    }

    #[ignore]
    #[test]
    fn bench_insert_overwrite_edge() {
        let path = "bench_insert_overwrite_edge";
        do_bench(path, |graph| bench::graph::bench_insert_edge(graph));
    }

    fn do_bench<F: Fn(GraphStore)>(path: &str, func: F) {
        let path = format!("store_bench/{}", path);
        fs::rmr(&path).unwrap();
        let graph = create_empty_graph(&path);
        func(graph);
        fs::rmr(&path).unwrap();
    }

    pub fn create_empty_graph(path: &str) -> GraphStore {
        let mut builder = GraphConfigBuilder::new();
        builder.set_storage_engine("rocksdb");
        let config = builder.build();
        GraphStore::open(&config, path).unwrap()
    }
}
