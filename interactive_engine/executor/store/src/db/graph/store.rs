#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use crate::db::api::*;
use crate::db::storage::{ExternalStorage, ExternalStorageBackup, RawBytes};
use crate::db::storage::rocksdb::{RocksDB};
use crate::db::util::lock::GraphMutexLock;
use super::types::*;
use super::codec::*;
use super::meta::*;
use super::bin::*;
use protobuf::Message;
use crate::db::api::GraphErrorCode::{InvalidData, TypeNotFound};
use crate::db::graph::table_manager::Table;
use crate::db::graph::entity::{RocksVertexImpl, RocksEdgeImpl};
use crate::db::graph::iter::{EdgeTypeScan, VertexTypeScan};
use crate::db::api::multi_version_graph::{MultiVersionGraph, GraphBackup};
use crate::db::api::condition::Condition;

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

pub struct GraphBackupEngine {
    engine: Box<dyn ExternalStorageBackup>,
}

impl GraphBackup for GraphBackupEngine {
    fn create_new_backup(&mut self) -> GraphResult<BackupId> {
        self.engine.create_new_backup()
    }

    fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()> {
        self.engine.delete_backup(backup_id)
    }

    fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()> {
        self.engine.restore_from_backup(restore_path, backup_id)
    }

    fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()> {
        self.engine.verify_backup(backup_id)
    }

    fn get_backup_list(&self) -> Vec<BackupId> {
        self.engine.get_backup_list()
    }
}

impl MultiVersionGraph for GraphStore {
    type V = RocksVertexImpl;
    type E = RocksEdgeImpl;

    fn get_vertex(&self,
                  snapshot_id: SnapshotId,
                  vertex_id: VertexId,
                  label_id: Option<LabelId>,
                  property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::V>> {
        if let Some(label_id) = label_id {
            self.get_vertex_from_label(snapshot_id, vertex_id, label_id, property_ids)
        } else {
            let mut iter = self.vertex_manager.get_all(snapshot_id as i64);
            while let Some(info) = iter.next() {
                if let Some(vertex) = self.get_vertex_from_label(snapshot_id, vertex_id, info.get_label() as LabelId, property_ids)? {
                    return Ok(Some(vertex));
                }
            }
            Ok(None)
        }
    }

    fn get_edge(&self,
                snapshot_id: SnapshotId,
                edge_id: EdgeId,
                edge_relation: Option<&EdgeKind>,
                property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::E>> {
        if let Some(relation) = edge_relation {
            self.get_edge_from_relation(snapshot_id, edge_id, relation, property_ids)
        } else {
            let mut iter = self.edge_manager.get_all_edges(snapshot_id as i64);
            while let Some(info) = iter.next() {
                let mut edge_kind_iter = info.into_iter();
                while let Some(edge_kind_info) = edge_kind_iter.next() {
                    if let Some(edge) = self.get_edge_from_relation(snapshot_id, edge_id, &edge_kind_info.get_type().into(), property_ids)? {
                        return Ok(Some(edge));
                    }
                }
            }
            Ok(None)
        }
    }

    fn scan_vertex(&self,
                   snapshot_id: SnapshotId,
                   label_id: Option<LabelId>,
                   _condition: Option<&Condition>,
                   _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::V>> {
        if let Some(label_id) = label_id {
            match self.vertex_manager.get_type_info(snapshot_id as i64, label_id as i32) {
                Ok(vertex_type_info) => {
                    let scan = VertexTypeScan::new(self.storage.clone(), snapshot_id, vertex_type_info);
                    Ok(scan.into_iter())
                }
                Err(e) => {
                    if let TypeNotFound = e.get_error_code() {
                        Ok(Box::new(::std::iter::empty()))
                    } else {
                        Err(e)
                    }
                }
            }
        } else {
            let mut vertex_type_info_iter = self.vertex_manager.get_all(snapshot_id as i64);
            let mut res: Records<Self::V> = Box::new(::std::iter::empty());
            while let Some(info) = vertex_type_info_iter.next_info() {
                let label_iter = VertexTypeScan::new(self.storage.clone(), snapshot_id, info).into_iter();
                res = Box::new(res.chain(label_iter));
            }
            Ok(res)
        }
    }

    fn scan_edge(&self,
                 snapshot_id: SnapshotId,
                 label_id: Option<LabelId>,
                 condition: Option<&Condition>,
                 property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, None, EdgeDirection::Both, label_id, condition, property_ids)
    }

    fn get_out_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, Some(vertex_id), EdgeDirection::Out, label_id, condition, property_ids)
    }

    fn get_in_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, Some(vertex_id), EdgeDirection::In, label_id, condition, property_ids)
    }

    fn get_out_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId,  label_id: Option<LabelId>) -> GraphResult<usize> {
        let edges_iter = self.get_out_edges(snapshot_id, vertex_id, label_id, None,
                                            Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_in_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId,  label_id: Option<LabelId>,) -> GraphResult<usize> {
        let edges_iter = self.get_in_edges(snapshot_id, vertex_id, label_id, None,
                                           Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_kth_out_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeKind, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        let mut edges_iter = self.get_out_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                                property_ids)?;
        edges_iter.nth(k as usize).transpose()
    }

    fn get_kth_in_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeKind, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        let mut edges_iter = self.get_in_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                               property_ids)?;
        edges_iter.nth(k as usize).transpose()
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

    fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<dyn GraphBackup>> {
        let engine = res_unwrap!(self.storage.open_backup_engine(backup_path), open_backup_engine, backup_path)?;
        let ret = GraphBackupEngine {
            engine
        };
        Ok(Box::from(ret))
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
                if k.len() == key.len() && k[0..16] == key[0..16] && v.len() >= 4 {
                    let ret = unsafe { std::mem::transmute(v) };
                    return Ok(Some(ret));
                }
            }
        }
        Ok(None)
    }

    fn get_edge_data(&self, si: SnapshotId, id: EdgeId, info: &EdgeKindInfoRef, direction: EdgeDirection) -> GraphResult<Option<&[u8]>> {
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = edge_key(table.id, id, direction, ts);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k.len() == key.len() && k[0..32] == key[0..32] && v.len() >= 4 {
                    let ret = unsafe { std::mem::transmute(v) };
                    return Ok(Some(ret));
                }
            }
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

    fn get_vertex_from_label(&self,
                             snapshot_id: SnapshotId,
                             vertex_id: VertexId,
                             label_id: LabelId,
                             _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<RocksVertexImpl>> {
        let snapshot_id = snapshot_id as i64;
        let vertex_type_info = self.vertex_manager.get_type_info(snapshot_id, label_id as i32)?;
        if let Some(table) = vertex_type_info.get_table(snapshot_id) {
            let key = vertex_key(table.id, vertex_id as i64, snapshot_id - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..16] == key[0..16] && v.len() > 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = vertex_type_info.get_decoder(snapshot_id, codec_version)?;
                    let vertex = RocksVertexImpl::new(vertex_id, vertex_type_info.get_label() as LabelId, Some(decoder), RawBytes::new(v));
                    return Ok(Some(vertex));
                }
            }
        }
        Ok(None)
    }

    fn get_edge_from_relation(&self,
                              snapshot_id: SnapshotId,
                              edge_id: EdgeId,
                              edge_relation: &EdgeKind,
                              _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<RocksEdgeImpl>> {
        let snapshot_id = snapshot_id as i64;
        let info = self.edge_manager.get_edge_kind(snapshot_id, &edge_relation.into())?;
        if let Some(table) = info.get_table(snapshot_id) {
            let key = edge_key(table.id, edge_id.into(), EdgeDirection::Out, snapshot_id - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..32] == key[0..32] && v.len() >= 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = info.get_decoder(snapshot_id, codec_version)?;
                    let edge = RocksEdgeImpl::new(edge_id, info.get_type().into(), Some(decoder), RawBytes::new(v));
                    return Ok(Some(edge));
                }
            }
        }
        Ok(None)
    }

    fn query_edges(&self,
                   snapshot_id: SnapshotId,
                   vertex_id: Option<VertexId>,
                   direction: EdgeDirection,
                   label_id: Option<LabelId>,
                   _condition: Option<&Condition>,
                   _property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<RocksEdgeImpl>> {
        if let Some(label_id) = label_id {
            match self.edge_manager.get_edge_info(snapshot_id as i64, label_id as i32) {
                Ok(edge_info) => {
                    let scan = EdgeTypeScan::new(self.storage.clone(), snapshot_id, edge_info, vertex_id, direction);
                    Ok(scan.into_iter())
                }
                Err(e) => {
                    if let TypeNotFound = e.get_error_code() {
                        Ok(Box::new(::std::iter::empty()))
                    } else {
                        Err(e)
                    }
                }
            }

        } else {
            let mut edge_info_iter = self.edge_manager.get_all_edges(snapshot_id as i64);
            let mut res: Records<RocksEdgeImpl> = Box::new(::std::iter::empty());
            while let Some(info) = edge_info_iter.next_info() {
                let label_iter = EdgeTypeScan::new(self.storage.clone(), snapshot_id, info, vertex_id, direction).into_iter();
                res = Box::new(res.chain(label_iter));
            }
            Ok(res)
        }
    }
}

fn merge_updates<'a>(old: &mut HashMap<PropertyId, ValueRef<'a>>, updates: &'a dyn PropertyMap) {
    for (prop_id, v) in updates.as_map() {
        old.insert(prop_id, v);
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

    #[test]
    fn test_backup_engine() {
        let test_dir = "store_test/test_backup_engine";
        fs::rmr(&test_dir).unwrap();
        let store_path = format!("{}/store", test_dir);
        let graph = create_empty_graph(&store_path);
        tests::backup::test_backup_engine(graph, test_dir);
        fs::rmr(&test_dir).unwrap();
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
