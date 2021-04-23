use std::sync::Arc;

use crate::db::api::*;
use crate::db::storage::{StorageIter, ExternalStorage};
use crate::db::common::unsafe_util;
use super::property::*;
use super::codec::*;
use super::types::*;
use super::bin::*;
use super::query;

pub struct EdgeImpl {
    id: EdgeId,
    edge_kind: EdgeKind,
    data: PropData<'static>,
    decoder: Decoder,
}

impl EdgeImpl {
    pub fn new(id: EdgeId, edge_kind: EdgeKind, data: PropData<'static>, decoder: Decoder) -> Self {
        EdgeImpl {
            id,
            edge_kind,
            data,
            decoder,
        }
    }
}

impl Edge for EdgeImpl {
    type PI = PropertiesIter<'static>;

    fn get_id(&self) -> &EdgeId {
        &self.id
    }

    fn get_src_id(&self) -> i64 {
        self.id.src_id
    }

    fn get_dst_id(&self) -> i64 {
        self.id.dst_id
    }

    fn get_kind(&self) -> &EdgeKind {
        &self.edge_kind
    }

    fn get_property(&self, prop_id: i32) -> Option<ValueRef> {
        self.decoder.decode_property(self.data.as_bytes(), prop_id)
    }

    fn get_properties_iter(&self) -> PropertiesRef<Self::PI> {
        let data = unsafe { std::mem::transmute(self.data.as_bytes()) };
        let iter = self.decoder.decode_properties(data);
        PropertiesRef::new(PropertiesIter::new(iter))
    }
}

impl std::fmt::Debug for EdgeImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{:?}, {:?}, properties: ", self.get_id(), self.get_kind())?;
        let mut iter = self.get_properties_iter();
        while let Some((prop_id, v)) = iter.next() {
            write!(f, "{{{}:{:?}}}", prop_id, v)?;
        }
        write!(f, ">")
    }
}

pub struct SingleLabelEdgeIter<'a> {
    inner: EdgeResultIterList<SingleTypeEdgeIter<'a>>,
}

impl<'a> SingleLabelEdgeIter<'a> {
    pub fn create(si: SnapshotId,
                  id: VertexId,
                  direction: EdgeDirection,
                  info: EdgeInfoRef,
                  storage: &'a dyn ExternalStorage,
                  condition: Option<Arc<Condition>>)
        -> GraphResult<Self> {
        let mut info_iter = info.into_iter();
        let mut iters = Vec::new();
        while let Some(type_info) = info_iter.next() {
            let res = SingleTypeEdgeIter::create(si, id, direction, type_info, storage, condition.clone());
            match res_unwrap!(res, create)? {
                Some(iter) => iters.push(iter),
                None => {},
            }
        }
        let inner = EdgeResultIterList::new(iters);
        let ret = SingleLabelEdgeIter {
            inner,
        };
        Ok(ret)
    }
}

impl<'a> EdgeResultIter for SingleLabelEdgeIter<'a> {
    type E = EdgeImpl;

    fn next(&mut self) -> Option<EdgeWrapper<Self::E>> {
        self.inner.next()
    }

    fn ok(&self) -> GraphResult<()> {
        self.inner.ok()
    }
}

pub struct MultiLabelsEdgeIter<'a> {
    inner: EdgeResultIterList<SingleLabelEdgeIter<'a>>,
}

impl<'a> MultiLabelsEdgeIter<'a> {
    pub fn create(si: SnapshotId,
                  id: VertexId,
                  direction: EdgeDirection,
                  mut info_iter: EdgeInfoIter,
                  storage: &'a dyn ExternalStorage,
                  condition: Option<Arc<Condition>>) -> GraphResult<Self> {
        let mut iters = Vec::new();
        while let Some(info) = info_iter.next() {
            let res = SingleLabelEdgeIter::create(si, id, direction, info, storage, condition.clone());
            let iter = res_unwrap!(res, create)?;
            iters.push(iter);
        }
        let inner = EdgeResultIterList::new(iters);
        let ret = MultiLabelsEdgeIter {
            inner,
        };
        Ok(ret)
    }
}

impl<'a> EdgeResultIter for MultiLabelsEdgeIter<'a> {
    type E = EdgeImpl;

    fn next(&mut self) -> Option<EdgeWrapper<Self::E>> {
        self.inner.next()
    }

    fn ok(&self) -> GraphResult<()> {
        self.inner.ok()
    }
}

struct SingleTypeEdgeIter<'a> {
    si: SnapshotId,
    ts: SnapshotId,
    info: EdgeKindInfoRef,
    condition: Option<Arc<Condition>>,
    iter: StorageIter<'a>,
    last_id: Option<EdgeId>,
    err: Option<GraphError>,
}

impl<'a> SingleTypeEdgeIter<'a> {
    fn create(si: SnapshotId,
              id: VertexId,
              direction: EdgeDirection,
              info: EdgeKindInfoRef,
              storage: &'a dyn ExternalStorage,
              condition: Option<Arc<Condition>>)
              -> GraphResult<Option<Self>> {
        if let Some(table) = info.get_table(si) {
            let ts = si - table.start_si;
            let iter = match direction {
                EdgeDirection::In | EdgeDirection::Out => {
                    let prefix = edge_prefix(table.id, id, direction);
                    storage.scan_prefix(&prefix)
                },
                EdgeDirection::Both => {
                    let prefix = edge_table_prefix_key(table.id, EdgeDirection::Out);
                    storage.scan_prefix(&prefix)
                },
            };
            let iter = res_unwrap!(iter, create)?;
            let ret = SingleTypeEdgeIter::new(si, ts, info, iter, condition);
            return Ok(Some(ret));
        }
        Ok(None)
    }

    fn new(si: SnapshotId, ts: SnapshotId, info: EdgeKindInfoRef, iter: StorageIter<'a>, condition: Option<Arc<Condition>>) -> Self {
        SingleTypeEdgeIter {
            si,
            ts,
            info,
            condition,
            iter,
            last_id: None,
            err: None,
        }
    }

    unsafe fn next_item<'b>(&self) -> Option<(&'b [u8], &'b [u8])> {
        std::mem::transmute(unsafe_util::to_mut(self).iter.next())
    }

    fn check(&self, id: &EdgeId, ts: SnapshotId) -> bool {
        self.ts >= ts && match self.last_id {
            Some(last_id) => *id != last_id,
            None => true,
        }
    }

    fn set_err(&self, e: GraphError) {
        unsafe { unsafe_util::to_mut(self) }.err = Some(e);
    }
}

impl<'a> EdgeResultIter for SingleTypeEdgeIter<'a> {
    type E = EdgeImpl;

    fn next(&mut self) -> Option<EdgeWrapper<Self::E>> {
        if self.err.is_some() {
            return None;
        }
        loop {
            let (key, val) = unsafe { self.next_item() }?;
            let (edge_id, ts) = parse_edge_key(key);

            if !self.check(&edge_id, ts) {
                continue;
            }
            self.last_id = Some(edge_id);
            if val.len() < 4 {
                // it's a tombstone
                continue;
            }
            let version = get_codec_version(val);
            return match self.info.get_decoder(self.si, version) {
                Ok(decoder) => {
                    if let Some(ref condition) = self.condition {
                        if !query::check_condition(&decoder, val, condition.as_ref()) {
                            continue;
                        }
                    }
                    let data = unsafe { std::mem::transmute(PropData::from(val)) };
                    let ret = EdgeImpl::new(edge_id, self.info.get_type().clone(), data, decoder);
                    Some(EdgeWrapper::new(ret))
                }
                Err(e) => {
                    error!("decode edge error, {:?}", e);
                    self.set_err(e);
                    None
                }
            };
        }
    }

    fn ok(&self) -> GraphResult<()> {
        if let Some(ref e) = self.err {
            return Err(e.clone());
        }
        Ok(())
    }
}

struct EdgeResultIterList<I> {
    iters: Vec<I>,
    cur: usize,
    err: Option<GraphError>,
}

impl<I: EdgeResultIter> EdgeResultIterList<I> {
    fn new(iters: Vec<I>) -> Self {
        EdgeResultIterList {
            iters,
            cur: 0,
            err: None,
        }
    }
}

impl<E: Edge, I: EdgeResultIter<E=E>> EdgeResultIter for EdgeResultIterList<I> {
    type E = E;

    fn next(&mut self) -> Option<EdgeWrapper<Self::E>> {
        if self.err.is_some() {
            return None;
        }
        loop {
            let iter = self.iters.get(self.cur)?;
            let iter_mut = unsafe { unsafe_util::to_mut(iter) };
            if let Some(e) = iter_mut.next() {
                return Some(e);
            }
            if let Err(e) = iter.ok() {
                self.err = Some(e);
                return None;
            }
            self.cur += 1;
        }
    }

    fn ok(&self) -> GraphResult<()> {
        if let Some(ref e) = self.err {
            return Err(e.clone());
        }
        Ok(())
    }
}