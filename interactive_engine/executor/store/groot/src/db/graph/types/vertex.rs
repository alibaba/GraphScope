#![allow(dead_code)]
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::Duration;

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

pub struct VertexTypeInfoIter {
    si: SnapshotId,
    inner: Values<'static, LabelId, Arc<VertexTypeInfo>>,
}

impl VertexTypeInfoIter {
    pub fn next(&mut self) -> Option<Arc<VertexTypeInfo>> {
        loop {
            let info = self.inner.next()?;
            if info.lifetime.is_alive_at(self.si) {
                return Some(info.clone());
            }
        }
    }

    pub fn next_info(&mut self) -> Option<Arc<VertexTypeInfo>> {
        loop {
            let info = self.inner.next()?;
            if info.lifetime.is_alive_at(self.si) {
                return Some(info.clone());
            }
        }
    }

    fn new(si: SnapshotId, values: Values<'static, LabelId, Arc<VertexTypeInfo>>) -> Self {
        VertexTypeInfoIter { si, inner: values }
    }
}

type VertexMap = HashMap<LabelId, Arc<VertexTypeInfo>>;

pub struct VertexTypeManager {
    map: RwLock<VertexMap>,
}

/*
impl Drop for VertexTypeManager {
    fn drop(&mut self) {
        unsafe {
            drop(std::mem::replace(&mut self.map, Atomic::null()).into_owned());
        }
    }
}
*/

impl VertexTypeManager {
    pub fn new() -> Self {
        VertexTypeManager { map: RwLock::new(VertexMap::new()) }
    }

    pub fn contains_type(&self, _si: SnapshotId, label: LabelId) -> bool {
        if let Ok(map) = self.get_map() {
            map.contains_key(&label)
        } else {
            false
        }
    }

    pub fn create_type(
        &self, si: SnapshotId, label: LabelId, codec: Codec, table0: Table,
    ) -> GraphResult<()> {
        assert_eq!(si, table0.start_si, "type start si must be equal to table0.start_si");
        let mut map = self.get_map_write()?;
        if map.contains_key(&label) {
            let msg = format!("vertex#{} already exists", label);
            let err =
                gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create, si, label, codec, table0);
            Err(err)
        } else {
            let info = VertexTypeInfo::new(si, label);
            res_unwrap!(info.update_codec(si, codec), create, si, label, codec, table0)?;
            res_unwrap!(info.online_table(table0), create, si, label, codec, table0)?;
            map.insert(label, Arc::new(info));

            Ok(())
        }
    }

    pub fn get_type(&self, si: SnapshotId, label: LabelId) -> GraphResult<Arc<VertexTypeInfo>> {
        let map = self.get_map()?;
        if let Some(info) = map.get(&label) {
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
        let map = self.get_map()?;
        if let Some(info) = map.get(&label) {
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

    /*
    pub fn get_all(&self, si: SnapshotId) -> GraphResult<VertexTypeInfoIter> {
        let map = self.get_map()?;

        Ok(VertexTypeInfoIter::new(si, map.values()))
    }
    */

    pub fn drop_type(&self, si: SnapshotId, label: LabelId) -> GraphResult<()> {
        let map = self.get_map()?;
        if let Some(info) = map.get(&label) {
            info.lifetime.set_end(si);
        }
        Ok(())
    }

    pub fn gc(&self, si: SnapshotId) -> GraphResult<Vec<TableId>> {
        let mut b = Vec::new();
        let mut table_ids = Vec::new();
        {
            let map_ref = self.get_map()?;
            for (label, info) in map_ref.iter() {
                table_ids.append(&mut info.gc(si)?);
                if info.is_obsolete_at(si) {
                    b.push(*label);
                }
            }
        }
        if !b.is_empty() {
            let mut map = self.get_map_write()?;
            for label in b {
                map.remove(&label);
            }
            /*
            let mut map_clone = map_ref.clone();
            for label in b {
                map_clone.remove(&label);
            }
            self.map
                .store(Owned::new(map_clone).into_shared(guard), Ordering::Relaxed);
            unsafe { guard.defer_destroy(map) };
            */
        }
        Ok(table_ids)
    }

    fn get_map(&self) -> GraphResult<RwLockReadGuard<VertexMap>> {
        let mut counter = 0;
        loop {
            if let Ok(map) = self.map.read() {
                return Ok(map);
            } else {
                thread::sleep(Duration::from_millis(10));
                counter += 1;
                if counter > 10 {
                    break;
                }
            }
        }

        let msg = format!("fail to get the read lock");
        let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, get_map);

        Err(err)
    }

    fn get_map_write(&self) -> GraphResult<RwLockWriteGuard<VertexMap>> {
        let mut counter = 0;
        loop {
            if let Ok(map) = self.map.write() {
                return Ok(map);
            } else {
                thread::sleep(Duration::from_millis(10));
                counter += 1;
                if counter > 10 {
                    break;
                }
            }
        }

        let msg = format!("fail to get the read lock");
        let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, get_map);

        Err(err)
    }

    /*
    fn get_shared_map<'g>(&self, guard: &'g Guard) -> Shared<'g, VertexMap> {
        self.map.load(Ordering::Relaxed, guard)
    }
    */
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
        VertexTypeManager { map: RwLock::new(self.map) }
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
