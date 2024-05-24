#![allow(dead_code)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;

use ::crossbeam_epoch as epoch;
use protobuf::Message;

use super::bin::*;
use super::codec::*;
use super::meta::*;
use super::types::*;
use crate::api::elem::Edge;
use crate::api::Condition;
use crate::api::ElemFilter;
use crate::api::PropId;
use crate::db::api::multi_version_graph::{GraphBackup, MultiVersionGraph};
use crate::db::api::types::RocksEdge;
use crate::db::api::GraphErrorCode::{InvalidData, TypeNotFound};
use crate::db::api::*;
use crate::db::common::bytes::transform;
use crate::db::graph::entity::{RocksEdgeImpl, RocksVertexImpl};
use crate::db::graph::iter::{EdgeTypeScan, VertexTypeScan};
use crate::db::graph::table_manager::Table;
use crate::db::storage::rocksdb::{RocksDB, RocksDBBackupEngine};
use crate::db::storage::RawBytes;
use crate::db::util::lock::GraphMutexLock;

pub struct GraphStore {
    config: GraphConfig,
    meta: Meta,
    vertex_manager: VertexTypeManager,
    edge_manager: EdgeTypeManager,
    storage: Arc<RocksDB>,
    data_root: String,
    data_download_root: String,
    // ensure all modification to graph is in ascending order of snapshot id
    si_guard: AtomicIsize,
    lock: GraphMutexLock<()>,
}

pub struct GraphBackupEngine {
    engine: Box<RocksDBBackupEngine>,
}

impl GraphBackup for GraphBackupEngine {
    fn create_new_backup(&mut self) -> GraphResult<BackupId> {
        self.engine.create_new_backup()
    }

    fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()> {
        self.engine.delete_backup(backup_id)
    }

    fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()> {
        self.engine
            .restore_from_backup(restore_path, backup_id)
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

    fn get_vertex(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::V>> {
        debug!("get_vertex {:?}, {:?}, {:?}", vertex_id, label_id, property_ids);
        if let Some(label_id) = label_id {
            self.get_vertex_from_label(si, vertex_id, label_id, property_ids)
        } else {
            let guard = epoch::pin();
            let map = self.vertex_manager.get_map(&guard);
            let map_ref = unsafe { map.deref() };
            let mut iter = map_ref.values();
            while let Some(info) = next_vertex_type_info(si, &mut iter) {
                if let Some(vertex) =
                    self.get_vertex_from_label(si, vertex_id, info.get_label() as LabelId, property_ids)?
                {
                    return Ok(Some(vertex));
                }
            }
            Ok(None)
        }
    }

    fn get_edge(
        &self, si: SnapshotId, edge_id: EdgeId, edge_relation: Option<&EdgeKind>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>> {
        debug!("get_edge {:?}", edge_id);
        if let Some(relation) = edge_relation {
            self.get_edge_from_relation(si, edge_id, relation, property_ids)
        } else {
            let guard = epoch::pin();
            let inner = self.edge_manager.get_inner(&guard);
            let edge_mgr = unsafe { inner.deref() };
            let mut iter = edge_mgr.get_all_edges();
            while let Some(info) = next_edge_info(si, &mut iter) {
                let edge_kinds = info.lock();
                let mut edge_kind_iter = edge_kinds.iter_kinds();
                while let Some(edge_kind_info) = edge_kind_iter.next() {
                    if edge_kind_info.is_alive_at(si) {
                        if let Some(edge) = self.get_edge_from_relation(
                            si,
                            edge_id,
                            &edge_kind_info.get_type().into(),
                            property_ids,
                        )? {
                            return Ok(Some(edge));
                        }
                    }
                }
            }
            Ok(None)
        }
    }

    fn scan_vertex(
        &self, si: SnapshotId, label_id: Option<LabelId>, condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::V>> {
        debug!("scan_vertex {:?}, {:?}, {:?}", label_id, condition, property_ids);
        let with_prop = property_ids.is_some();
        let mut iter = match label_id {
            Some(label_id) => {
                match self
                    .vertex_manager
                    .get_type_info(si as i64, label_id as i32)
                {
                    Ok(vertex_type_info) => {
                        let scan =
                            VertexTypeScan::new(self.storage.clone(), si, vertex_type_info, with_prop);
                        scan.into_iter()
                    }
                    Err(e) => {
                        if let TypeNotFound = e.get_error_code() {
                            Box::new(::std::iter::empty())
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            None => {
                let guard = epoch::pin();
                let map = self.vertex_manager.get_map(&guard);
                let map_ref = unsafe { map.deref() };
                let mut iter = map_ref.values();
                let mut res: Records<Self::V> = Box::new(::std::iter::empty());
                while let Some(info) = next_vertex_type_info(si, &mut iter) {
                    let label_iter =
                        VertexTypeScan::new(self.storage.clone(), si, info, with_prop).into_iter();
                    res = Box::new(res.chain(label_iter));
                }
                res
            }
        };

        if let Some(condition) = condition.cloned() {
            iter = Box::new(iter.filter(move |v| {
                v.is_ok()
                    && condition
                        .filter_vertex(v.as_ref().unwrap())
                        .unwrap_or(false)
            }))
        }
        let columns = Self::parse_columns(property_ids);
        Ok(Box::new(iter.map(move |v| match v {
            Ok(mut v) => {
                v.set_columns(columns.clone());
                Ok(v)
            }
            Err(e) => Err(e),
        })))
    }

    fn scan_edge(
        &self, si: SnapshotId, label_id: Option<LabelId>, condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>> {
        debug!("scan_edge {:?}", label_id);
        self.query_edges(si, None, EdgeDirection::Both, label_id, condition, property_ids)
    }
    fn get_out_edges(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>,
        condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>> {
        debug!("get_out_edges {:?}, {:?}", vertex_id, label_id);
        self.query_edges(si, Some(vertex_id), EdgeDirection::Out, label_id, condition, property_ids)
    }

    fn get_in_edges(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>,
        condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>> {
        debug!("get_in_edges {:?}, {:?}", vertex_id, label_id);
        self.query_edges(si, Some(vertex_id), EdgeDirection::In, label_id, condition, property_ids)
    }

    fn get_out_degree(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>,
    ) -> GraphResult<usize> {
        debug!("get_out_degree {:?}, {:?}", vertex_id, label_id);
        let edges_iter = self.get_out_edges(si, vertex_id, label_id, None, Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_in_degree(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>,
    ) -> GraphResult<usize> {
        debug!("get_in_degree {:?}, {:?}", vertex_id, label_id);
        let edges_iter = self.get_in_edges(si, vertex_id, label_id, None, Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_kth_out_edge(
        &self, si: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeKind, k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>> {
        debug!("get_kth_out_edge");
        let mut edges_iter =
            self.get_out_edges(si, vertex_id, Some(edge_relation.get_edge_label_id()), None, property_ids)?;
        edges_iter.nth(k as usize).transpose()
    }

    fn get_kth_in_edge(
        &self, si: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeKind, k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>> {
        debug!("get_kth_in_edge");
        let mut edges_iter =
            self.get_in_edges(si, vertex_id, Some(edge_relation.get_edge_label_id()), None, property_ids)?;
        edges_iter.nth(k as usize).transpose()
    }

    fn create_vertex_type(
        &self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef, table_id: i64,
    ) -> GraphResult<bool> {
        debug!("create_vertex_type");
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
        self.meta
            .create_vertex_type(si, schema_version, label_id, type_def, table_id)
            .and_then(|table| {
                let codec = Codec::from(type_def);
                self.vertex_manager
                    .create_type(si, label_id, codec, table)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn add_vertex_type_properties(
        &self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef, table_id: i64,
    ) -> GraphResult<bool> {
        debug!("add_vertex_type_properties");
        let _guard = res_unwrap!(self.lock.lock(), add_vertex_type_properties)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if !self.vertex_manager.contains_type(si, label_id) {
            let msg = format!("vertex#{} does not exist", label_id);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add_vertex_type_properties);
            return Err(err);
        }
        self.meta
            .add_vertex_type_properties(si, schema_version, label_id, type_def, table_id)
            .and_then(|table| {
                let codec = Codec::from(type_def);
                self.vertex_manager
                    .update_type(si, label_id, codec, table)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn create_edge_type(
        &self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef,
    ) -> GraphResult<bool> {
        debug!("create_edge_type");
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
        self.meta
            .create_edge_type(si, schema_version, label_id, type_def)
            .and_then(|_| {
                self.edge_manager
                    .create_edge_type(si, label_id, type_def)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn add_edge_type_properties(
        &self, si: i64, schema_version: i64, label_id: LabelId, type_def: &TypeDef,
    ) -> GraphResult<bool> {
        debug!("add_edge_type_properties");
        let _guard = res_unwrap!(self.lock.lock(), create_edge_type)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if !self.edge_manager.contains_edge(label_id) {
            let msg = format!("edge#{} does not exist", label_id);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_edge);
            return Err(err);
        }
        self.meta
            .add_edge_type_properties(si, schema_version, label_id, type_def)
            .and_then(|_| {
                self.edge_manager
                    .update_edge_type(si, label_id, type_def)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn add_edge_kind(
        &self, si: i64, schema_version: i64, edge_kind: &EdgeKind, table_id: i64,
    ) -> GraphResult<bool> {
        debug!("add_edge_kind");
        let _guard = res_unwrap!(self.lock.lock(), add_edge_kind)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        if self
            .edge_manager
            .contains_edge_kind(si, edge_kind)
        {
            let msg = format!("{:?} already exists", edge_kind);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add_edge_kind, si, edge_kind);
            return Err(err);
        }
        self.meta
            .add_edge_kind(si, schema_version, edge_kind, table_id)
            .and_then(|table| {
                self.edge_manager.add_edge_kind(si, edge_kind)?;
                let info = self.edge_manager.get_edge_kind(si, edge_kind)?;
                info.online_table(table)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn drop_vertex_type(&self, si: i64, schema_version: i64, label_id: LabelId) -> GraphResult<bool> {
        debug!("drop_vertex_type");
        let _guard = res_unwrap!(self.lock.lock(), drop_vertex_type, si, label_id)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta
            .drop_vertex_type(si, schema_version, label_id)
            .and_then(|_| self.vertex_manager.drop_type(si, label_id))
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn drop_edge_type(&self, si: i64, schema_version: i64, label_id: LabelId) -> GraphResult<bool> {
        debug!("drop_edge_type");
        let _guard = res_unwrap!(self.lock.lock(), drop_edge_type, si, label_id)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta
            .drop_edge_type(si, schema_version, label_id)
            .and_then(|_| self.edge_manager.drop_edge_type(si, label_id))
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn remove_edge_kind(&self, si: i64, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<bool> {
        debug!("remove_edge_kind");
        let _guard = res_unwrap!(self.lock.lock(), remove_edge_kind, si, edge_kind)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta
            .remove_edge_kind(si, schema_version, edge_kind)
            .and_then(|_| {
                self.edge_manager
                    .remove_edge_kind(si, edge_kind)
            })
            .map(|_| self.update_si_guard(si))?;
        Ok(true)
    }

    fn insert_overwrite_vertex(
        &self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("si {:?}, id {:?}, insert_overwrite_vertex", si, id);

        self.check_si_guard(si)?;
        let res = self
            .vertex_manager
            .get_type(si, label)
            .and_then(|info| self.do_insert_vertex_data(si, info.as_ref(), id, properties))
            .map(|_| self.update_si_guard(si));

        res_unwrap!(res, insert_overwrite_vertex, si, id, label)
    }

    fn insert_update_vertex(
        &self, si: i64, id: i64, label: LabelId, properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("insert_update_vertex");
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.vertex_manager.get_type(si, label), si, id, label)?;
        match res_unwrap!(self.get_vertex_data(si, id, info.as_ref()), insert_update_vertex, si, id, label)?
        {
            Some(data) => {
                let data = data.as_slice();
                let version = get_codec_version(data);
                let decoder = info.get_decoder(si, version)?;
                let mut old = decoder.decode_all(data);
                merge_updates(&mut old, properties);
                let res = self
                    .do_insert_vertex_data(si, info.as_ref(), id, &old)
                    .map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_vertex, si, id, label)
            }
            None => {
                let res = self
                    .do_insert_vertex_data(si, info.as_ref(), id, properties)
                    .map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_vertex, si, id, label)
            }
        }
    }

    fn clear_vertex_properties(
        &self, si: i64, id: i64, label: LabelId, prop_ids: &[PropertyId],
    ) -> GraphResult<()> {
        debug!("clear_vertex_properties");
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.vertex_manager.get_type(si, label), si, id, label)?;
        if let Some(data) = self.get_vertex_data(si, id, &info)? {
            let data = data.as_slice();
            let version = get_codec_version(data);
            let decoder = info.get_decoder(si, version)?;
            let mut old = decoder.decode_all(data);
            clear_props(&mut old, prop_ids);
            let res = self
                .do_insert_vertex_data(si, info.as_ref(), id, &old)
                .map(|_| self.update_si_guard(si));
            return res_unwrap!(res, clear_vertex_properties, si, id, label);
        }
        Ok(())
    }

    fn delete_vertex(&self, si: i64, id: i64, label: LabelId) -> GraphResult<()> {
        debug!("delete_vertex");
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

    fn insert_overwrite_edge(
        &self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("insert_overwrite_edge");
        self.check_si_guard(si)?;
        let direction = if forward { EdgeDirection::Out } else { EdgeDirection::In };
        let res = self
            .edge_manager
            .get_edge_kind(si, edge_kind)
            .and_then(|info| self.do_insert_edge_data(si, id, &info, direction, properties))
            .map(|_| self.update_si_guard(si));
        res_unwrap!(res, insert_overwrite_edge, si, id, edge_kind)
    }

    fn insert_update_edge(
        &self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("insert_update_edge, {:?}, {:?}, {}", id, edge_kind, forward);
        self.check_si_guard(si)?;

        // if edge id is not 0, it may be existed edge id, or next edge id to be created.

        let info = res_unwrap!(
            self.edge_manager.get_edge_kind(si, edge_kind),
            insert_update_edge,
            si,
            id,
            edge_kind
        )?;
        let direction = if forward { EdgeDirection::Out } else { EdgeDirection::In };

        let data_res = self.get_edge_data(si, id, &info, direction)?;

        match data_res {
            Some(data) => {
                let data = data.as_slice();
                let version = get_codec_version(data);
                let decoder = info.get_decoder(si, version)?;
                let mut old = decoder.decode_all(data);
                merge_updates(&mut old, properties);
                let res = self
                    .do_insert_edge_data(si, id, &info, direction, &old)
                    .map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_edge, si, id, edge_kind)
            }
            None => {
                let res = self
                    .do_insert_edge_data(si, id, &info, direction, properties)
                    .map(|_| self.update_si_guard(si));
                res_unwrap!(res, insert_update_edge, si, id, edge_kind)
            }
        }
    }

    fn clear_edge_properties(
        &self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool, prop_ids: &[PropertyId],
    ) -> GraphResult<()> {
        debug!("clear_edge_properties");
        self.check_si_guard(si)?;

        let mut complete_id = id;
        if id.inner_id == 0 {
            let edge_id =
                self.get_eid_by_vertex(si, edge_kind.edge_label_id, id.src_id, id.dst_id, forward);
            match edge_id {
                Some(edge_id) => {
                    complete_id = edge_id;
                }
                None => {
                    warn!("Skipped clearing edge properties");
                }
            }
        }

        let info = res_unwrap!(
            self.edge_manager.get_edge_kind(si, edge_kind),
            insert_update_edge,
            si,
            id,
            edge_kind
        )?;
        let direction = if forward { EdgeDirection::Out } else { EdgeDirection::In };
        if let Some(data) = self.get_edge_data(si, complete_id, &info, direction)? {
            let data = data.as_slice();
            let version = get_codec_version(data);
            let decoder = info.get_decoder(si, version)?;
            let mut old = decoder.decode_all(data);
            clear_props(&mut old, prop_ids);
            let res = self
                .do_insert_edge_data(si, complete_id, &info, direction, &old)
                .map(|_| self.update_si_guard(si));
            return res_unwrap!(res, clear_edge_properties, si, complete_id, edge_kind);
        }
        Ok(())
    }

    fn delete_edge(&self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool) -> GraphResult<()> {
        trace!("delete_edge {:?}, {:?}, {}", id, edge_kind, forward);
        self.check_si_guard(si)?;
        let mut complete_id = id;
        if id.inner_id == 0 {
            let edge_id =
                self.get_eid_by_vertex(si, edge_kind.edge_label_id, id.src_id, id.dst_id, forward);
            match edge_id {
                Some(edge_id) => {
                    complete_id = edge_id;
                }
                None => {
                    warn!("Skipped delete edge");
                }
            }
        }
        self.delete_edge_impl(si, complete_id, edge_kind, forward)
    }

    fn gc(&self, si: i64) -> GraphResult<()> {
        let vertex_tables = self.vertex_manager.gc(si)?;
        if !vertex_tables.is_empty() {
            info!("garbage collect vertex table {:?}", vertex_tables);
        }
        for vt in vertex_tables {
            let table_prefix = vertex_table_prefix(vt);
            self.delete_table_by_prefix(table_prefix, true)?;
        }
        let edge_tables = self.edge_manager.gc(si)?;
        if !edge_tables.is_empty() {
            info!("garbage collect edge table {:?}", edge_tables);
        }
        for et in edge_tables {
            let out_table_prefix = edge_table_prefix(et, EdgeDirection::Out);
            self.delete_table_by_prefix(out_table_prefix, false)?;
        }
        Ok(())
    }

    fn get_graph_def_blob(&self) -> GraphResult<Vec<u8>> {
        let graph_def = self.meta.get_graph_def().lock()?;
        let pb = graph_def.to_proto()?;
        pb.write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))
    }

    fn prepare_data_load(
        &self, si: i64, schema_version: i64, target: &DataLoadTarget, table_id: i64,
    ) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), prepare_data_load)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta
            .prepare_data_load(si, schema_version, target, table_id)?;
        Ok(true)
    }

    fn commit_data_load(
        &self, si: i64, schema_version: i64, target: &DataLoadTarget, table_id: i64, partition_id: i32,
        unique_path: &str,
    ) -> GraphResult<bool> {
        let _guard = res_unwrap!(self.lock.lock(), prepare_data_load)?;
        self.check_si_guard(si)?;
        if let Err(_) = self.meta.check_version(schema_version) {
            return Ok(false);
        }
        self.meta
            .commit_data_load(si, schema_version, target, table_id)?;
        let data_file_path =
            format!("{}/{}/part-r-{:0>5}.sst", self.data_download_root, unique_path, partition_id);
        info!("committing data load from path {}", data_file_path);

        if Path::new(data_file_path.as_str()).exists() {
            if let Ok(metadata) = fs::metadata(data_file_path.clone()) {
                let size = metadata.len();
                println!("Ingesting file: {} with size: {} bytes", data_file_path, size);
            }
            self.ingest(data_file_path.as_str())?
        }
        if target.src_label_id > 0 {
            let edge_kind = EdgeKind::new(target.label_id, target.src_label_id, target.dst_label_id);
            let info = self
                .edge_manager
                .get_edge_kind(si, &edge_kind)?;
            info.online_table(Table::new(si, table_id))?;
            info!("online edge. target {:?}, tableId {}, si {}", target, table_id, si);
        } else {
            let info = self
                .vertex_manager
                .get_type(si, target.label_id)?;
            info.online_table(Table::new(si, table_id))?;
            info!("online vertex. labelId {}, tableId {}, si {}", target.label_id, table_id, si);
        }
        Ok(true)
    }

    fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<dyn GraphBackup>> {
        let engine =
            res_unwrap!(self.storage.open_backup_engine(backup_path), open_backup_engine, backup_path)?;
        let ret = GraphBackupEngine { engine };
        Ok(Box::from(ret))
    }
}

impl GraphStore {
    pub fn open(config: &GraphConfig) -> GraphResult<Self> {
        let path = config
            .get_storage_option("store.data.path")
            .expect("invalid config, missing store.data.path");
        let parts: Vec<&str> = path.split("/").collect();
        if parts.get(parts.len() - 1) == Some(&"0") {
            info!("open graph store at {} with config {:?}", path, config);
        }
        match config.get_storage_engine() {
            "rocksdb" => {
                let res = RocksDB::open(config.get_storage_options()).and_then(|db| {
                    let storage = Arc::new(db);
                    Self::init(config, storage, path)
                });
                res_unwrap!(res, open, config, path)
            }
            "rocksdb_as_secondary" => {
                let res = RocksDB::open_as_secondary(config.get_storage_options()).and_then(|db| {
                    let storage = Arc::new(db);
                    Self::init(config, storage, path)
                });
                res_unwrap!(res, open, config, path)
            }
            unknown => {
                let msg = format!("unknown storage {}", unknown);
                let err = gen_graph_err!(GraphErrorCode::NotSupported, msg, open, config, path);
                Err(err)
            }
        }
    }

    pub fn try_catch_up_with_primary(&self) -> GraphResult<()> {
        self.storage.try_catch_up_with_primary()
    }

    pub fn compact(&self) -> GraphResult<()> {
        self.storage.compact()
    }

    pub fn reopen(&self, wait_sec: u64) -> GraphResult<()> {
        self.storage.reopen(wait_sec)
    }

    fn init(config: &GraphConfig, storage: Arc<RocksDB>, path: &str) -> GraphResult<Self> {
        let meta = Meta::new(storage.clone());
        let (vertex_manager, edge_manager) = res_unwrap!(meta.recover(), init)?;
        let data_root = path.to_string();
        let mut download_root = "".to_string();
        download_root = config
            .get_storage_option("store.data.download.path")
            .unwrap_or(&download_root)
            .clone();
        if download_root.is_empty() {
            download_root = format!("{}/../{}", data_root, "download");
        }

        let ret = GraphStore {
            config: config.clone(),
            meta,
            vertex_manager,
            edge_manager,
            storage,
            data_root: data_root,
            data_download_root: download_root,
            si_guard: AtomicIsize::new(0),
            lock: GraphMutexLock::new(()),
        };
        Ok(ret)
    }

    fn get_vertex_data(
        &self, si: SnapshotId, id: VertexId, info: &VertexTypeInfo,
    ) -> GraphResult<Option<Vec<u8>>> {
        debug!("get_vertex_data");
        if let Some(table) = info.get_table(si) {
            let key = vertex_key(table.id, id, si - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k.len() == key.len() && k[0..16] == key[0..16] && v.len() >= 4 {
                    let ret = v.to_vec();
                    return Ok(Some(ret));
                }
            }
        }
        Ok(None)
    }

    fn get_edge_data(
        &self, si: SnapshotId, id: EdgeId, info: &EdgeKindInfo, direction: EdgeDirection,
    ) -> GraphResult<Option<Vec<u8>>> {
        debug!("get_edge_data");
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = edge_key(table.id, id, direction, ts);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k.len() == key.len() && k[0..32] == key[0..32] && v.len() >= 4 {
                    let ret = v.to_vec();
                    return Ok(Some(ret));
                }
            }
        }
        Ok(None)
    }

    fn do_insert_vertex_data(
        &self, si: SnapshotId, info: &VertexTypeInfo, id: VertexId, properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("si {:?}, id {:?}, do_insert_vertex_data", si, id);

        if let Some(table) = info.get_table(si) {
            let encoder = res_unwrap!(info.get_encoder(si), do_insert_vertex_data)?;
            let mut buf = Vec::new();
            return encoder
                .encode(properties, &mut buf)
                .and_then(|_| {
                    let ts = si - table.start_si;
                    let key = vertex_key(table.id, id, ts);
                    self.storage.put(&key, &buf)
                });
        }
        let msg = format!("table not found at {} of vertex#{}", si, info.get_label());
        let err = gen_graph_err!(GraphErrorCode::DataNotExists, msg, do_insert_vertex_data);
        Err(err)
    }

    fn do_insert_edge_data(
        &self, si: SnapshotId, edge_id: EdgeId, info: &EdgeKindInfo, direction: EdgeDirection,
        properties: &dyn PropertyMap,
    ) -> GraphResult<()> {
        debug!("do_insert_edge_data {:?} {:?}", edge_id, direction);
        if let Some(table) = info.get_table(si) {
            let encoder = res_unwrap!(info.get_encoder(si), do_insert_edge_data)?;
            let mut buf = Vec::new();
            return encoder
                .encode(properties, &mut buf)
                .and_then(|_| {
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
        if si < guard {
            let msg = format!("si#{} is less than current si_guard#{}", si, guard);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg);
            return Err(err);
        }
        Ok(())
    }

    fn update_si_guard(&self, si: SnapshotId) {
        self.si_guard
            .store(si as isize, Ordering::Relaxed);
    }

    pub fn ingest(&self, data_path: &str) -> GraphResult<()> {
        let p = [data_path];
        self.storage.load(&p)
    }

    pub fn get_graph_def(&self) -> GraphResult<GraphDef> {
        let graph_def = self.meta.get_graph_def().lock()?;
        Ok((&*graph_def).clone())
    }

    fn get_vertex_from_label(
        &self, si: SnapshotId, vertex_id: VertexId, label_id: LabelId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<RocksVertexImpl>> {
        debug!("get_vertex_from_label {:?}, {:?}, {:?}", vertex_id, label_id, property_ids);
        let si = si as i64;
        let vertex_type_info = self
            .vertex_manager
            .get_type_info(si, label_id as i32)?;
        if let Some(table) = vertex_type_info.get_table(si) {
            let key = vertex_key(table.id, vertex_id as i64, si - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..16] == key[0..16] && v.len() > 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = vertex_type_info.get_decoder(si, codec_version)?;
                    let columns = Self::parse_columns(property_ids);
                    let vertex = RocksVertexImpl::with_columns(
                        vertex_id,
                        vertex_type_info.get_label() as LabelId,
                        Some(decoder),
                        RawBytes::new(v),
                        columns,
                    );
                    return Ok(Some(vertex));
                }
            }
        }
        Ok(None)
    }

    fn get_edge_from_relation(
        &self, si: SnapshotId, edge_id: EdgeId, edge_relation: &EdgeKind,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<RocksEdgeImpl>> {
        debug!("get_edge_from_relation, {:?}, {:?}", edge_id, edge_relation);
        let si = si as i64;
        let info = self
            .edge_manager
            .get_edge_kind(si, &edge_relation.into())?;
        if let Some(table) = info.get_table(si) {
            let key = edge_key(table.id, edge_id.into(), EdgeDirection::Out, si - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..32] == key[0..32] && v.len() >= 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = info.get_decoder(si, codec_version)?;
                    let columns = Self::parse_columns(property_ids);
                    let edge = RocksEdgeImpl::with_columns(
                        edge_id,
                        info.get_type().into(),
                        Some(decoder),
                        RawBytes::new(v),
                        columns,
                    );
                    return Ok(Some(edge));
                }
            }
        }
        Ok(None)
    }

    fn query_edges(
        &self, si: SnapshotId, vertex_id: Option<VertexId>, direction: EdgeDirection,
        label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<RocksEdgeImpl>> {
        debug!("query_edges {:?}, {:?}, {:?} {:?}", vertex_id, label_id, property_ids, direction);
        let with_prop = property_ids.is_some();
        let mut iter = match label_id {
            Some(label_id) => {
                match self
                    .edge_manager
                    .get_edge_info(si as i64, label_id as i32)
                {
                    Ok(edge_info) => {
                        let scan = EdgeTypeScan::new(
                            self.storage.clone(),
                            si,
                            edge_info,
                            vertex_id,
                            direction,
                            with_prop,
                        );
                        scan.into_iter()
                    }
                    Err(e) => {
                        if let TypeNotFound = e.get_error_code() {
                            Box::new(::std::iter::empty())
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            None => {
                let guard = epoch::pin();
                let inner = self.edge_manager.get_inner(&guard);
                let edge_mgr = unsafe { inner.deref() };
                let mut iter = edge_mgr.get_all_edges();
                let mut res: Records<RocksEdgeImpl> = Box::new(::std::iter::empty());
                while let Some(info) = next_edge_info(si, &mut iter) {
                    let label_iter =
                        EdgeTypeScan::new(self.storage.clone(), si, info, vertex_id, direction, with_prop)
                            .into_iter();
                    res = Box::new(res.chain(label_iter));
                }
                res
            }
        };
        if let Some(condition) = condition.cloned() {
            iter = Box::new(iter.filter(move |e| {
                e.is_ok()
                    && condition
                        .filter_edge(e.as_ref().unwrap())
                        .unwrap_or(false)
            }));
        }
        let columns = Self::parse_columns(property_ids);
        Ok(Box::new(iter.map(move |e| match e {
            Ok(mut e) => {
                e.set_columns(columns.clone());
                Ok(e)
            }
            Err(e) => Err(e),
        })))
    }

    pub fn delete_table_by_prefix(&self, table_prefix: i64, vertex_table: bool) -> GraphResult<()> {
        let end = if vertex_table { table_prefix + 1 } else { table_prefix + 2 };
        let end_key = transform::i64_to_arr(end.to_be());
        let start_key = transform::i64_to_arr(table_prefix.to_be());
        self.storage.delete_range(&start_key, &end_key)
    }

    fn parse_columns(property_ids: Option<&Vec<PropertyId>>) -> Option<HashSet<PropId>> {
        property_ids.map(|v| {
            v.to_owned()
                .into_iter()
                .map(|i| i as PropId)
                .collect()
        })
    }

    fn delete_edge_impl(
        &self, si: i64, id: EdgeId, edge_kind: &EdgeKind, forward: bool,
    ) -> GraphResult<()> {
        trace!("delete_edge impl {:?}, {:?}, {}", id, edge_kind, forward);
        self.check_si_guard(si)?;
        let info = res_unwrap!(self.edge_manager.get_edge_kind(si, edge_kind), si, id, edge_kind)?;
        let direction = if forward { EdgeDirection::Out } else { EdgeDirection::In };
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let key = edge_key(table.id, id, direction, ts);
            res_unwrap!(self.storage.put(&key, &[]), delete_edge, si, id, edge_kind)?;
        }
        self.update_si_guard(si);
        Ok(())
    }

    fn get_eid_by_vertex(
        &self, si: i64, label_id: LabelId, src_id: VertexId, dst_id: VertexId, forward: bool,
    ) -> Option<EdgeId> {
        let direction = if forward { EdgeDirection::Out } else { EdgeDirection::In };
        let edge: GraphResult<RocksEdgeImpl>;
        if forward {
            edge = self.get_edge_by_vertex(si, label_id, src_id, dst_id, direction);
        } else {
            edge = self.get_edge_by_vertex(si, label_id, dst_id, src_id, direction);
        }
        match edge {
            Ok(edge) => Some(*RocksEdge::get_edge_id(&edge)),
            Err(_) => None,
        }
    }

    fn get_edge_by_vertex(
        &self, si: i64, label_id: LabelId, src_id: VertexId, dst_id: VertexId, direction: EdgeDirection,
    ) -> GraphResult<RocksEdgeImpl> {
        let iter = self.query_edges(si, Some(src_id), direction, Some(label_id), None, None);
        debug!("get_edge_by_vertex {:?}, {}, {}, {:?}", label_id, src_id, dst_id, direction);
        let target_id = if direction == EdgeDirection::Out { dst_id } else { src_id };
        match iter {
            Ok(mut iter) => {
                while let Some(edge) = iter.next() {
                    match edge {
                        Ok(edge) => {
                            if edge.get_dst_id() == target_id {
                                return Ok(edge);
                            } else {
                                debug!("This edge doesn't match, continue")
                            }
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }
                let msg = format!(
                    "edge not found. labelId {}, srcId {}, dstId {}, direction {:?}",
                    label_id, src_id, dst_id, direction
                );
                error!("{}", msg);
                Err(gen_graph_err!(GraphErrorCode::DataNotExists, msg, get_edge_by_vertex))
            }
            Err(err) => Err(err),
        }
    }
}

fn merge_updates<'a>(old: &mut HashMap<PropertyId, ValueRef<'a>>, updates: &'a dyn PropertyMap) {
    for (prop_id, v) in updates.as_map() {
        old.insert(prop_id, v);
    }
}

fn clear_props(old: &mut HashMap<PropertyId, ValueRef>, prop_ids: &[PropertyId]) {
    for prop_id in prop_ids {
        old.remove(prop_id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests;
    use super::*;
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
        builder.add_storage_option("store.data.path", path);
        let config = builder.build();
        GraphStore::open(&config).unwrap()
    }
}

#[cfg(test)]
mod bench {
    use super::super::bench;
    use super::*;
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
        builder.add_storage_option("store.data.path", path);
        let config = builder.build();
        GraphStore::open(&config).unwrap()
    }
}
