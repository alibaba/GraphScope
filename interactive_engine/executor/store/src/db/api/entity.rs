use std::marker::PhantomData;
use super::{VertexId, LabelId, PropId, EdgeId, EdgeKind, ValueRef};

pub trait Vertex: std::fmt::Debug {
    type PI: PropIter;
    fn get_id(&self) -> VertexId;
    fn get_label(&self) -> LabelId;
    fn get_property(&self, prop_id: PropId) -> Option<ValueRef>;
    fn get_properties_iter(&self) -> PropertiesRef<Self::PI>;
}

pub trait Edge: std::fmt::Debug {
    type PI: PropIter;
    fn get_id(&self) -> &EdgeId;
    fn get_src_id(&self) -> VertexId;
    fn get_dst_id(&self) -> VertexId;
    fn get_kind(&self) -> &EdgeKind;
    fn get_property(&self, prop_id: PropId) -> Option<ValueRef>;
    fn get_properties_iter(&self) -> PropertiesRef<Self::PI>;
}

pub trait PropIter: Sized {
    fn next(&mut self) -> Option<(PropId, ValueRef)>;
}

pub struct PropertiesRef<'a, T> {
    iter: T,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T: PropIter> PropertiesRef<'a, T> {
    pub fn new(iter: T) -> Self {
        PropertiesRef {
            iter,
            _phantom: Default::default(),
        }
    }
}

impl<'a, T: PropIter> PropIter for PropertiesRef<'a, T> {
    fn next(&mut self) -> Option<(PropId, ValueRef)> {
        self.iter.next()
    }
}
