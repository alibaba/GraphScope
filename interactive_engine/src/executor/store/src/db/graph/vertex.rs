#![allow(dead_code)]
use std::sync::Arc;

use crate::db::api::*;
use crate::db::storage::*;
use crate::db::common::unsafe_util;
use super::codec::*;
use super::property::*;
use super::types::*;
use super::bin::*;

pub struct VertexImpl {
    id: VertexId,
    label: LabelId,
    data: PropData<'static>,
    decoder: Decoder,
}

impl Vertex for VertexImpl {
    type PI = PropertiesIter<'static>;

    fn get_id(&self) -> VertexId {
        self.id
    }

    fn get_label(&self) -> LabelId {
        self.label
    }

    fn get_property(&self, prop_id: i32) -> Option<ValueRef> {
        self.decoder.decode_property(self.data.as_bytes(), prop_id)
    }

    fn get_properties_iter(&self) -> PropertiesRef<Self::PI> {
        let data = unsafe { std::mem::transmute(self.data.as_bytes()) };
        let iter = self.decoder.decode_properties(data);
        let ret = PropertiesIter::new(iter);
        PropertiesRef::new(ret)
    }
}

impl std::fmt::Debug for VertexImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<id: {}, label: {}, properties: ", self.id, self.label)?;
        let mut iter = self.get_properties_iter();
        while let Some((prop_id, v)) = iter.next() {
            write!(f, "{{{}: {:?}}}", prop_id, v)?;
        }
        write!(f, ">")
    }
}

impl VertexImpl {
    pub fn new(id: VertexId, label: LabelId, data: PropData<'static>, decoder: Decoder) -> Self {
        VertexImpl {
            id,
            label,
            data,
            decoder,
        }
    }
}

pub struct SingleLabelVertexIter<'a> {
    si: SnapshotId,
    ts: SnapshotId, // real si - table.start_si
    last_id: Option<VertexId>,
    type_info: VertexTypeInfoRef,
    condition: Option<Arc<Condition>>,
    iter: StorageIter<'a>,
    err: Option<GraphError>,
}

impl<'a> SingleLabelVertexIter<'a> {
    pub fn create(si: SnapshotId,
                  type_info: VertexTypeInfoRef,
                  storage: &'a dyn ExternalStorage,
                  condition: Option<Arc<Condition>>)
        -> GraphResult<Option<SingleLabelVertexIter>> {
        if let Some(table) = type_info.get_table(si) {
            let prefix = vertex_table_prefix_key(table.id);
            let iter = res_unwrap!(storage.scan_prefix(&prefix), create)?;
            let ret = Self::new(si, si - table.start_si, type_info, iter, condition);
            return Ok(Some(ret));
        }
        Ok(None)
    }

    fn new(si: SnapshotId, ts: SnapshotId, type_info: VertexTypeInfoRef, iter: StorageIter<'a>, condition: Option<Arc<Condition>>) -> Self {
        SingleLabelVertexIter {
            si,
            ts,
            last_id: None,
            type_info,
            condition,
            iter,
            err: None,
        }
    }

    fn check(&self, id: VertexId, ts: SnapshotId) -> bool {
        self.ts >= ts && match self.last_id {
            Some(last_id) => id != last_id,
            None => true,
        }
    }

    fn set_err(&self, e: GraphError) {
        unsafe { unsafe_util::to_mut(self) }.err = Some(e);
    }

    fn set_last_id(&self, id: VertexId) {
        unsafe { unsafe_util::to_mut(self) }.last_id = Some(id);
    }

    fn next_item(&self) -> Option<(&[u8], &[u8])> {
        unsafe { unsafe_util::to_mut(self) }.iter.next()
    }

    fn create_vertex(&self, id: VertexId, val: &[u8]) -> Option<VertexWrapper<VertexImpl>> {
        let version = get_codec_version(val);
        match self.type_info.get_decoder(self.si, version) {
            Ok(decoder) => {
                let data = unsafe { std::mem::transmute(PropData::from(val)) };
                let label = self.type_info.get_label();
                let ret = VertexImpl::new(id, label, data, decoder);
                Some(VertexWrapper::new(ret))
            }
            Err(e) => {
                self.set_err(e);
                return None;
            }
        }
    }
}

impl<'a> VertexResultIter for SingleLabelVertexIter<'a> {
    type V = VertexImpl;

    fn next(&mut self) -> Option<VertexWrapper<Self::V>> {
        if self.err.is_some() {
            return None;
        }
        loop {
            let (key, val) = self.next_item()?;
            match parse_vertex_key(key) {
                Ok((id, ts)) => {
                    if self.check(id, ts) {
                        self.set_last_id(id);
                        if val.len() >= 4 { // val.len() == 0 means this item is a tombstone of this vertex id
                            return self.create_vertex(id, val);
                        }
                    }
                }
                Err(e) => {
                    self.set_err(e);
                    return None;
                }
            }
        }
    }

    fn ok(&self) -> GraphResult<()> {
        if let Some(ref err) = self.err {
            return Err(err.clone());
        }
        Ok(())
    }
}

pub struct MultiLabelsVertexIter<'a> {
    iters: Vec<SingleLabelVertexIter<'a>>,
    cur: usize,
    err: Option<GraphError>,
}

impl<'a> MultiLabelsVertexIter<'a> {
    pub fn new(iters: Vec<SingleLabelVertexIter<'a>>) -> Self {
        MultiLabelsVertexIter {
            iters,
            cur: 0,
            err: None,
        }
    }
}

impl<'a> VertexResultIter for MultiLabelsVertexIter<'a> {
    type V = VertexImpl;

    fn next(&mut self) -> Option<VertexWrapper<Self::V>> {
        if self.err.is_some() {
            return None;
        }
        loop {
            let iter = self.iters.get(self.cur)?;
            let iter_mut = unsafe { unsafe_util::to_mut(iter) };
            if let Some(v) = iter_mut.next() {
                return Some(v);
            }
            if let Some(ref e) = iter.err {
                self.err = Some(e.clone());
                return None;
            }
            self.cur += 1;
        }
    }

    fn ok(&self) -> GraphResult<()> {
        if let Some(ref err) = self.err {
            return Err(err.clone());
        }
        Ok(())
    }
}
