#![allow(dead_code)]
use ::crossbeam_epoch as epoch;
use ::crossbeam_epoch::{Atomic, Owned, Guard};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::ops::Deref;
use std::collections::hash_map::Values;

use crate::db::api::*;
use crate::db::common::unsafe_util;
use super::super::table_manager::Table;
use super::super::codec::*;
use super::common::*;

pub struct EdgeKindInfo {
    edge_kind: EdgeKind,
    lifetime: LifeTime,
    info: TypeCommon,
}

impl EdgeKindInfo {
    pub fn get_type(&self) -> &EdgeKind {
        &self.edge_kind
    }

    pub fn get_table(&self, si: SnapshotId) -> Option<Table> {
        self.info.get_table(si)
    }

    pub fn online_table(&self, table: Table) -> GraphResult<()> {
        res_unwrap!(self.info.online_table(table), online_table)
    }

    pub fn gc(&self, si: SnapshotId) {
        self.info.gc(si)
    }

    pub fn get_decoder(&self, si: SnapshotId, version: CodecVersion) -> GraphResult<Decoder> {
        res_unwrap!(self.info.get_decoder(si, version), get_decoder, si, version)
    }

    pub fn get_encoder(&self, si: SnapshotId) -> GraphResult<Encoder> {
        res_unwrap!(self.info.get_encoder(si), get_encoder, si)
    }

    pub fn is_alive_at(&self, si: SnapshotId) -> bool {
        self.lifetime.is_alive_at(si)
    }

    fn new(si: SnapshotId, edge_kind: EdgeKind, codec_manager: Arc<CodecManager>) -> Self {
        EdgeKindInfo {
            edge_kind,
            lifetime: LifeTime::new(si),
            info: TypeCommon::init_with_codec_manager(codec_manager),
        }
    }
}

pub struct EdgeKindInfoRef {
    inner: &'static EdgeKindInfo,
    _guard: Guard,
}

impl EdgeKindInfoRef {
    pub fn new(info: &'static EdgeKindInfo, guard: Guard) -> Self {
        EdgeKindInfoRef {
            inner: info,
            _guard: guard,
        }
    }
}

impl Deref for EdgeKindInfoRef {
    type Target = EdgeKindInfo;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

#[derive(Clone)]
pub struct EdgeInfo {
    label: LabelId,
    lifetime: LifeTime,
    codec_manager: Arc<CodecManager>,
    kinds: Vec<Arc<EdgeKindInfo>>,
}

impl EdgeInfo {
    fn new(start_si: SnapshotId, label: LabelId) -> Self {
        EdgeInfo {
            label,
            lifetime: LifeTime::new(start_si),
            codec_manager: Arc::new(CodecManager::new()),
            kinds: Vec::new(),
        }
    }

    fn add_codec(&self, si: SnapshotId, codec: Codec) -> GraphResult<()> {
        res_unwrap!(self.codec_manager.add_codec(si, codec), add_codec)
    }

    fn add_edge_kind(&mut self, info: Arc<EdgeKindInfo>) {
        self.kinds.push(info);
    }

    fn is_alive_at(&self, si: SnapshotId) -> bool {
        self.lifetime.is_alive_at(si)
    }

    pub fn get_kinds(&self, si: SnapshotId) -> impl Iterator<Item=Arc<EdgeKindInfo>> + '_ {
        self.kinds.iter().filter_map(move |edge_kind| if edge_kind.is_alive_at(si) {
            Some(edge_kind.clone())
        } else {
            None
        })
    }
}

pub struct EdgeInfoRef {
    si: SnapshotId,
    inner: &'static EdgeInfo,
    _guard: Guard,
}

impl EdgeInfoRef {
    pub fn get_label(&self) -> LabelId {
        self.inner.label
    }

    pub fn into_iter(self) -> EdgeKindInfoIter {
        EdgeKindInfoIter {
            si: self.si,
            edge_info: self,
            cur: 0,
        }
    }

    fn new(si: SnapshotId, info: &'static EdgeInfo, guard: Guard) -> Self {
        EdgeInfoRef {
            si,
            inner: info,
            _guard: guard,
        }
    }
}

pub struct EdgeInfoIter {
    si: SnapshotId,
    inner: Values<'static, LabelId, Arc<EdgeInfo>>,
    guard: Guard,
}

impl EdgeInfoIter {
    pub fn next(&mut self) -> Option<EdgeInfoRef> {
        loop {
            let info = self.inner.next()?.as_ref();
            if info.is_alive_at(self.si) {
                return Some(EdgeInfoRef::new(self.si, info, epoch::pin()));
            }
        }
    }

    pub fn next_info(&mut self) -> Option<Arc<EdgeInfo>> {
        loop {
            let info = self.inner.next()?;
            if info.is_alive_at(self.si) {
                return Some(info.clone());
            }
        }
    }

    fn new(si: SnapshotId, iter: Values<'static, LabelId, Arc<EdgeInfo>>, guard: Guard) -> Self {
        EdgeInfoIter {
            si,
            inner: iter,
            guard,
        }
    }
}

pub struct EdgeKindInfoIter {
    si: SnapshotId,
    edge_info: EdgeInfoRef,
    cur: usize,
}

impl EdgeKindInfoIter {
    pub fn is_empty(&self) -> bool {
        self.cur < self.edge_info.inner.kinds.len()
    }

    pub fn next(&mut self) -> Option<EdgeKindInfoRef> {
        loop {
            let type_info = self.edge_info.inner.kinds.get(self.cur)?;
            self.cur += 1;
            if type_info.is_alive_at(self.si) {
                return Some(EdgeKindInfoRef::new(type_info.as_ref(), epoch::pin()));
            }
        }
    }
}

type EdgeInfoMap = HashMap<LabelId, Arc<EdgeInfo>>;
type EdgeKindMap = HashMap<EdgeKind, Vec<Arc<EdgeKindInfo>>>;

pub struct EdgeTypeManager {
    inner: Atomic<EdgeManagerInner>,
}

impl EdgeTypeManager {
    pub fn new() -> Self {
        EdgeTypeManager {
            inner: Atomic::new(EdgeManagerInner::new()),
        }
    }

    pub fn get_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<EdgeKindInfoRef> {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        let info = res_unwrap!(inner.get_edge_kind(si, kind), get_edge_kind, si, kind)?;
        let ret = EdgeKindInfoRef::new(info, guard);
        Ok(ret)
    }

    pub fn get_edge(&self, si: SnapshotId, label: LabelId) -> GraphResult<EdgeInfoRef> {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        let info = res_unwrap!(inner.get_edge(si, label), get_edge, si, label)?;
        let ret = EdgeInfoRef::new(si, info, guard);
        Ok(ret)
    }

    pub fn get_edge_info(&self, si: SnapshotId, label: LabelId) -> GraphResult<Arc<EdgeInfo>> {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        let ret = res_unwrap!(inner.get_edge_info(si, label), get_edge, si, label)?;
        Ok(ret)
    }

    pub fn get_all_edges(&self, si: SnapshotId) -> EdgeInfoIter {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        let iter = inner.get_all_edges();
        let ret = EdgeInfoIter::new(si, iter, guard);
        ret
    }

    pub fn contains_edge(&self, label: LabelId) -> bool {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        inner.contains_edge(label)
    }

    pub fn contains_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> bool {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        inner.contains_edge_kind(si, kind)
    }

    pub fn create_edge_type(&self, si: SnapshotId, label: LabelId, type_def: &TypeDef) -> GraphResult<()> {
        self.modify(|inner| {
            res_unwrap!(inner.create_edge_type(si, label, type_def), create_edge, si, label, type_def)
        })
    }

    pub fn drop_edge_type(&self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        self.modify(|inner| {
            res_unwrap!(inner.drop_edge_type(si, label), drop_edge, si, label)
        })
    }

    pub fn add_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        self.modify(|inner| {
            res_unwrap!(inner.add_edge_kind(si, kind), add_edge_kind, si, kind)
        })
    }

    pub fn remove_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        self.modify(|inner| {
            res_unwrap!(inner.remove_edge_kind(si, kind), remove_edge_kind, si, kind)
        })
    }

    pub fn gc(&self, si: SnapshotId) {
        self.modify(|inner| {
            inner.gc(si);
        })
    }

    fn modify<E, F: Fn(&mut EdgeManagerInner) -> E>(&self, f: F) -> E {
        let guard = epoch::pin();
        let inner = self.get_inner(&guard);
        let mut inner_clone = inner.clone();
        let res = f(&mut inner_clone);
        self.update(inner_clone);
        res
    }

    fn get_inner(&self, guard: &Guard) -> &'static EdgeManagerInner {
        unsafe {
            &*self.inner.load(Ordering::Relaxed, guard).as_raw()
        }
    }

    fn update(&self, inner: EdgeManagerInner) {
        self.inner.store(Owned::new(inner), Ordering::Relaxed);
    }
}

pub struct EdgeManagerBuilder {
    inner: EdgeManagerInner,
}

impl EdgeManagerBuilder {
    pub fn new() -> Self {
        EdgeManagerBuilder {
            inner: EdgeManagerInner::new(),
        }
    }

    pub fn create_edge_type(&mut self, si: SnapshotId, label: LabelId, type_def: &TypeDef) -> GraphResult<()> {
        self.inner.create_edge_type(si, label, type_def)
    }

    pub fn drop_edge_type(&mut self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        self.inner.drop_edge_type(si, label)
    }

    pub fn add_edge_kind(&mut self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        self.inner.add_edge_kind(si, kind)
    }

    pub fn remove_edge_kind(&mut self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        self.inner.remove_edge_kind(si, kind)
    }

    pub fn add_edge_table(&mut self, si: SnapshotId, kind: &EdgeKind, table: Table) -> GraphResult<()> {
        let info = res_unwrap!(self.inner.get_edge_kind(si, kind), add_edge_table, si, kind, table)?;
        let info_mut = unsafe { unsafe_util::to_mut(info) };
        res_unwrap!(info_mut.online_table(table.clone()), add_edge_table, si, kind, table)
    }

    pub fn build(self) -> EdgeTypeManager {
        EdgeTypeManager {
            inner: Atomic::new(self.inner)
        }
    }
}

#[derive(Clone)]
struct EdgeManagerInner {
    info_map: EdgeInfoMap,
    type_map: EdgeKindMap,
}

impl EdgeManagerInner {
    fn new() -> Self {
        EdgeManagerInner {
            info_map: EdgeInfoMap::new(),
            type_map: EdgeKindMap::new(),
        }
    }

    fn get_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<&EdgeKindInfo> {
        if let Some(list) = self.type_map.get(kind) {
            for info in list {
                if info.lifetime.is_alive_at(si) {
                    return Ok(info.as_ref());
                }
            }
            let msg = format!("no {:?} is alive at {}", kind, si);
            let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge_kind, si, kind);
            return Err(err);
        }
        let msg = format!("edge {:?} not found", kind);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge_kind, si, kind);
        Err(err)
    }

    fn get_edge(&self, si: SnapshotId, label: LabelId) -> GraphResult<&EdgeInfo> {
        if let Some(info) = self.info_map.get(&label) {
            if info.lifetime.is_alive_at(si) {
                return Ok(info.as_ref());
            }
            let msg = format!("edge#{} is not alive at {}", label, si);
            let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge, si, label);
            return Err(err);
        }
        let msg = format!("edge#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge, si, label);
        Err(err)
    }

    fn get_edge_info(&self, si: SnapshotId, label: LabelId) -> GraphResult<Arc<EdgeInfo>> {
        if let Some(info) = self.info_map.get(&label) {
            if info.lifetime.is_alive_at(si) {
                return Ok(info.clone());
            }
            let msg = format!("edge#{} is not alive at {}", label, si);
            let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge, si, label);
            return Err(err);
        }
        let msg = format!("edge#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge, si, label);
        Err(err)
    }

    fn contains_edge(&self, label: LabelId) -> bool {
        self.info_map.contains_key(&label)
    }

    fn contains_edge_kind(&self, si: SnapshotId, kind: &EdgeKind) -> bool {
        if let Some(list) = self.type_map.get(kind) {
            for info in list {
                if info.is_alive_at(si) {
                    return true;
                }
            }
        }
        false
    }

    fn get_all_edges(&self) -> Values<LabelId, Arc<EdgeInfo>> {
        self.info_map.values()
    }

    fn create_edge_type(&mut self, si: SnapshotId, label: LabelId, type_def: &TypeDef) -> GraphResult<()> {
        if self.info_map.contains_key(&label) {
            let msg = format!("edge#{} already exists", label);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_edge_type, si, label, type_def);
            return Err(err);
        }
        let info = EdgeInfo::new(si, label);
        let codec = Codec::from(type_def);
        let res = info.add_codec(si, codec);
        res_unwrap!(res, create_edge, si, label, type_def)?;
        self.info_map.insert(label, Arc::new(info));
        Ok(())
    }

    fn drop_edge_type(&mut self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        if let Some(info) = self.info_map.get(&label) {
            info.lifetime.set_end(si);
            for t in &info.kinds {
                t.lifetime.set_end(si);
            }
            return Ok(());
        }
        let msg = format!("edge#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, drop_edge_type, si, label);
        Err(err)
    }

    fn add_edge_kind(&mut self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        if let Some(edge_info) = self.info_map.get(&kind.edge_label_id) {
            let type_info = Arc::new(EdgeKindInfo::new(si, kind.clone(), edge_info.codec_manager.clone()));
            let info_mut = unsafe { &mut *(edge_info.as_ref() as *const EdgeInfo as *mut EdgeInfo) };
            info_mut.add_edge_kind(type_info.clone());
            if let Some(list) = self.type_map.get_mut(kind) {
                list.insert(0, type_info);
            } else {
                let mut list = Vec::new();
                list.push(type_info);
                self.type_map.insert(kind.clone(), list);
            }
            return Ok(());
        }
        let msg = format!("edge#{} not found", kind.edge_label_id);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_edge_kind, si, kind);
        Err(err)
    }

    fn remove_edge_kind(&mut self, si: SnapshotId, kind: &EdgeKind) -> GraphResult<()> {
        if let Some(list) = self.type_map.get_mut(kind) {
            list[0].lifetime.set_end(si);
            return Ok(());
        }
        let msg = format!("edge#{:?} not found", kind);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, remove_edge_kind, si, kind);
        Err(err)
    }

    /// if gc nothing return false
    #[allow(dead_code)]
    fn gc(&mut self, si: SnapshotId) {
        let mut dropped_labels = Vec::new();
        let mut dropped_types = Vec::new();
        for (label, info) in &self.info_map {
            if !info.lifetime.is_alive_at(si) {
                dropped_labels.push(*label);
                for t in &info.kinds {
                    dropped_types.push(t.edge_kind.clone());
                }
            } else {
                for t in &info.kinds {
                    if !t.lifetime.is_alive_at(si) {
                        dropped_types.push(t.edge_kind.clone());
                    } else {
                        t.gc(si);
                    }
                }
            }
        }
        for label in dropped_labels {
            self.info_map.remove(&label);
        }
        for t in dropped_types {
            self.type_map.remove(&t);
        }
    }
}
