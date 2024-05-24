#![allow(dead_code)]
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ::crossbeam_epoch as epoch;
use ::crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use super::super::codec::*;
use super::super::table_manager::*;
use super::common::*;
use crate::db::api::*;

pub struct VertexTypeInfo {
    label: LabelId,
    lifetime: LifeTime,
    info: TypeCommon,
}

impl VertexTypeInfo {
    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_table(&self, si: SnapshotId) -> Option<Table> {
        self.info.get_table(si)
    }

    pub fn online_table(&self, table: Table) -> GraphResult<()> {
        res_unwrap!(self.info.online_table(table), online_table)
    }

    pub fn update_codec(&self, si: SnapshotId, codec: Codec) -> GraphResult<()> {
        res_unwrap!(self.info.update_codec(si, codec), update_codec)
    }

    #[allow(dead_code)]
    pub fn gc(&self, si: SnapshotId) -> GraphResult<Vec<TableId>> {
        self.info.gc(si)
    }

    pub fn get_decoder(&self, si: SnapshotId, version: CodecVersion) -> GraphResult<Decoder> {
        res_unwrap!(self.info.get_decoder(si, version), get_decoder, si, version)
    }

    pub fn get_encoder(&self, si: SnapshotId) -> GraphResult<Encoder> {
        res_unwrap!(self.info.get_encoder(si), get_encoder, si)
    }

    fn is_alive_at(&self, si: SnapshotId) -> bool {
        self.lifetime.is_alive_at(si)
    }

    fn is_obsolete_at(&self, si: SnapshotId) -> bool {
        self.lifetime.is_obsolete_at(si)
    }

    fn new(si: SnapshotId, label: LabelId) -> Self {
        VertexTypeInfo { label, lifetime: LifeTime::new(si), info: TypeCommon::new() }
    }
}

pub fn next_vertex_type_info<'a>(
    si: SnapshotId, iter: &mut Values<'a, LabelId, Arc<VertexTypeInfo>>,
) -> Option<Arc<VertexTypeInfo>> {
    loop {
        let info = iter.next()?;
        if info.lifetime.is_alive_at(si) {
            return Some(info.clone());
        }
    }
}

type VertexMap = HashMap<LabelId, Arc<VertexTypeInfo>>;

pub struct VertexTypeManager {
    map: Atomic<VertexMap>,
}

impl Drop for VertexTypeManager {
    fn drop(&mut self) {
        unsafe {
            drop(std::mem::replace(&mut self.map, Atomic::null()).into_owned());
        }
    }
}

impl VertexTypeManager {
    pub fn new() -> Self {
        VertexTypeManager { map: Atomic::new(VertexMap::new()) }
    }

    pub fn contains_type(&self, _si: SnapshotId, label: LabelId) -> bool {
        let guard = &epoch::pin();
        let map = self.get_map(guard);

        if let Some(map_ref) = unsafe { map.as_ref() } {
            map_ref.contains_key(&label)
        } else {
            // TODO(longbin): any better solution
            false
        }
    }

    pub fn create_type(
        &self, si: SnapshotId, label: LabelId, codec: Codec, table0: Table,
    ) -> GraphResult<()> {
        assert_eq!(si, table0.start_si, "type start si must be equal to table0.start_si");

        let guard = &epoch::pin();
        let map = self.get_map(guard);
        let mut map_clone = unsafe { map.as_ref() }
            .ok_or_else(|| {
                let msg = "get map reference return `None`".to_string();
                gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si, label)
            })?
            .clone();
        if map_clone.contains_key(&label) {
            let msg = format!("vertex#{} already exists", label);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_type);
            return Err(err);
        }
        let info = VertexTypeInfo::new(si, label);
        res_unwrap!(info.update_codec(si, codec), create_type)?;
        res_unwrap!(info.online_table(table0), create_type)?;
        map_clone.insert(label, Arc::new(info));
        self.map
            .store(Owned::new(map_clone).into_shared(guard), Ordering::Release);
        unsafe { guard.defer_destroy(map) };
        Ok(())
    }

    pub fn update_type(
        &self, si: SnapshotId, label: LabelId, codec: Codec, table0: Table,
    ) -> GraphResult<()> {
        assert_eq!(si, table0.start_si, "type start si must be equal to table0.start_si");

        let guard = &epoch::pin();
        let map = self.get_map(guard);
        let mut map_clone = unsafe { map.as_ref() }
            .ok_or_else(|| {
                let msg = "get map reference return `None`".to_string();
                gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si, label)
            })?
            .clone();
        if !map_clone.contains_key(&label) {
            let msg = format!("vertex#{} not found.", label);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_type);
            return Err(err);
        }
        let info = VertexTypeInfo::new(si, label);
        res_unwrap!(info.update_codec(si, codec), create_type)?;
        res_unwrap!(info.online_table(table0), create_type)?;
        map_clone.insert(label, Arc::new(info));
        self.map
            .store(Owned::new(map_clone).into_shared(guard), Ordering::Release);
        unsafe { guard.defer_destroy(map) };
        Ok(())
    }

    pub fn get_type(&self, si: SnapshotId, label: LabelId) -> GraphResult<Arc<VertexTypeInfo>> {
        let guard = epoch::pin();
        let map = self.get_map(&guard);
        let map_ref = unsafe { map.as_ref() }.ok_or_else(|| {
            let msg = "get map reference return `None`".to_string();
            gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si, label)
        })?;
        if let Some(info) = map_ref.get(&label) {
            if info.is_alive_at(si) {
                return Ok(info.clone());
            }
            let msg = format!("vertex#{} is not visible at {}", label, si);
            let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get, si, label);
            return Err(err);
        }
        let msg = format!("vertex#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get, si, label);
        Err(err)
    }

    pub fn get_type_info(&self, si: SnapshotId, label: LabelId) -> GraphResult<Arc<VertexTypeInfo>> {
        let guard = epoch::pin();
        let map = self.get_map(&guard);
        let map_ref = unsafe { map.as_ref() }.ok_or_else(|| {
            let msg = "get map reference return `None`".to_string();
            gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si, label)
        })?;
        if let Some(info) = map_ref.get(&label) {
            if info.is_alive_at(si) {
                let ret = info.clone();
                return Ok(ret);
            }
            let msg = format!("vertex#{} is not visible at {}", label, si);
            let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get, si, label);
            return Err(err);
        }
        let msg = format!("vertex#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get, si, label);
        Err(err)
    }

    pub fn drop_type(&self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        let guard = epoch::pin();
        let map = self.get_map(&guard);
        let map_ref = unsafe { map.as_ref() }.ok_or_else(|| {
            let msg = "get map reference return `None`".to_string();
            gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si, label)
        })?;
        if let Some(info) = map_ref.get(&label) {
            info.lifetime.set_end(si);
        }
        Ok(())
    }

    pub fn gc(&self, si: SnapshotId) -> GraphResult<Vec<TableId>> {
        let guard = &epoch::pin();
        let map = self.get_map(guard);
        let map_ref: &VertexMap = unsafe { map.as_ref() }.ok_or_else(|| {
            let msg = "get map reference return `None`".to_string();
            gen_graph_err!(GraphErrorCode::InvalidData, msg, get_map, si)
        })?;
        let mut b = Vec::new();
        let mut table_ids = Vec::new();
        for (label, info) in map_ref {
            table_ids.append(&mut info.gc(si)?);
            if info.is_obsolete_at(si) {
                b.push(*label);
            }
        }
        if !b.is_empty() {
            let mut map_clone = map_ref.clone();
            for label in b {
                map_clone.remove(&label);
            }
            self.map
                .store(Owned::new(map_clone).into_shared(guard), Ordering::Release);
            unsafe { guard.defer_destroy(map) };
        }
        Ok(table_ids)
    }

    pub fn get_map<'g>(&self, guard: &'g Guard) -> Shared<'g, VertexMap> {
        self.map.load(Ordering::Acquire, guard)
    }
}

pub struct VertexTypeManagerBuilder {
    map: VertexMap,
}

impl VertexTypeManagerBuilder {
    pub fn new() -> Self {
        VertexTypeManagerBuilder { map: VertexMap::new() }
    }

    pub fn create(&mut self, si: SnapshotId, label: LabelId, type_def: &TypeDef) -> GraphResult<()> {
        if self.map.contains_key(&label) {
            let msg = format!("vertex#{} already exists", label);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create, si, label, type_def);
            return Err(err);
        }
        let info = VertexTypeInfo::new(si, label);
        let codec = Codec::from(type_def);
        let res = info.update_codec(si, codec);
        res_unwrap!(res, create, si, label, type_def)?;
        self.map.insert(label, Arc::new(info));
        Ok(())
    }

    pub fn drop(&mut self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        if let Some(info) = self.map.get(&label) {
            info.lifetime.set_end(si);
            return Ok(());
        }
        let msg = format!("vertex#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, drop, si, label);
        Err(err)
    }

    pub fn get_info(&self, si: SnapshotId, label: LabelId) -> GraphResult<&VertexTypeInfo> {
        if let Some(info) = self.map.get(&label) {
            if info.lifetime.is_alive_at(si) {
                return Ok(info.as_ref());
            }
        }
        let msg = format!("vertex#{} not found", label);
        let err = gen_graph_err!(GraphErrorCode::TypeNotFound, msg, get_info_mut, label);
        Err(err)
    }

    pub fn build(self) -> VertexTypeManager {
        VertexTypeManager { map: Atomic::new(self.map) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_online_table() {
        let info = VertexTypeInfo::new(1, 1);

        for si in 10..=100 {
            if si % 10 == 0 {
                let id = si as TableId / 10;
                let table = Table::new(si, id);
                info.online_table(table).unwrap();
            }
        }
        for si in 1..10 {
            assert!(info.get_table(si).is_none());
        }

        for si in 10..110 {
            let table = Table::new(si / 10 * 10, si / 10);
            assert_eq!(info.get_table(si).unwrap(), table);
        }
    }

    #[test]
    fn test_online_table2() {
        let info = VertexTypeInfo::new(1, 1);
        let table = Table::new(10, -9223372036854775807);
        info.online_table(table.clone()).unwrap();
        let table2 = info.get_table(20).unwrap();
        assert_eq!(table, table2);
    }
}
