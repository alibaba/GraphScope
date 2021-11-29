use crate::db::graph::codec::{Decoder, IterDecoder};
use crate::db::storage::RawBytes;
use crate::db::api::{GraphResult, VertexId, LabelId, EdgeId, EdgeKind, PropertyId};
use crate::db::api::types::{PropertyReader, PropertyValue, Property, RocksVertex, RocksEdge};
use std::fmt::{Debug, Formatter};
use crate::api::{Vertex, Edge};
use crate::schema::PropId;

pub struct PropertyImpl {
    property_id: PropertyId,
    property_value: PropertyValue,
}

impl Property for PropertyImpl {
    fn get_property_id(&self) -> PropertyId {
        self.property_id
    }

    fn get_property_value(&self) -> &PropertyValue {
        &self.property_value
    }
}

impl PropertyImpl {
    fn parse_to_property(self) -> (PropId, crate::api::prelude::Property) {
        let p = match self.property_value {
            PropertyValue::Null => crate::api::prelude::Property::Null,
            PropertyValue::Boolean(b) => crate::api::prelude::Property::Bool(b),
            PropertyValue::Char(c) => crate::api::prelude::Property::Char(c as u8),
            PropertyValue::Short(s) => crate::api::prelude::Property::Short(s),
            PropertyValue::Int(i) => crate::api::prelude::Property::Int(i),
            PropertyValue::Long(l) => crate::api::prelude::Property::Long(l),
            PropertyValue::Float(f) => crate::api::prelude::Property::Float(f),
            PropertyValue::Double(d) => crate::api::prelude::Property::Double(d),
            PropertyValue::String(s) => crate::api::prelude::Property::String(s),
            PropertyValue::Bytes(b) => crate::api::prelude::Property::Bytes(b),
            PropertyValue::IntList(il) => crate::api::prelude::Property::ListInt(il),
            PropertyValue::LongList(ll) => crate::api::prelude::Property::ListLong(ll),
            PropertyValue::FloatList(fl) => crate::api::prelude::Property::ListFloat(fl),
            PropertyValue::DoubleList(dl) => crate::api::prelude::Property::ListDouble(dl),
            PropertyValue::StringList(sl) => crate::api::prelude::Property::ListString(sl),
        };
        (self.property_id as PropId, p)
    }
}

pub struct PropertiesIter<'a> {
    decode_iter: IterDecoder<'a>,
}

impl<'a> Iterator for PropertiesIter<'a> {
    type Item = GraphResult<PropertyImpl>;

    fn next(&mut self) -> Option<Self::Item> {
        self.decode_iter.next().map(|(prop_id, v)| {
            Ok(PropertyImpl {
                property_id: prop_id as PropertyId,
                property_value: v.into(),
            })
        })
    }
}

pub struct RocksVertexImpl {
    vertex_id: VertexId,
    label_id: LabelId,
    decoder: Option<Decoder>,
    raw_bytes: RawBytes,
}

impl RocksVertexImpl {
    pub fn new(vertex_id: VertexId, label_id: LabelId, decoder: Option<Decoder>, raw_bytes: RawBytes) -> Self {
        RocksVertexImpl { vertex_id, label_id, decoder, raw_bytes }
    }
}

impl PropertyReader for RocksVertexImpl {
    type P = PropertyImpl;
    type PropertyIterator = Box<dyn Iterator<Item=GraphResult<Self::P>>>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        match &self.decoder {
            None => None,
            Some(decoder) => {
                let bytes = unsafe { self.raw_bytes.to_slice() };
                let value_ref = decoder.decode_property(bytes, property_id as i32);
                value_ref.map(|v| {
                    PropertyImpl {
                        property_id,
                        property_value: v.into(),
                    }
                })
            }
        }
    }

    fn get_property_iterator(&self) -> Box<dyn Iterator<Item=GraphResult<Self::P>>> {
        match &self.decoder {
            None => Box::new(::std::iter::empty()),
            Some(decoder) => {
                let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
                let decode_iter = decoder.decode_properties(bytes);
                Box::new(PropertiesIter {
                    decode_iter
                })
            }
        }
    }
}

impl Debug for RocksVertexImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<id: {}, label: {}, properties: ", self.get_vertex_id(), self.label_id)?;
        let mut iter = self.get_property_iterator();
        while let Some(p) = iter.next() {
            let p = p.unwrap();
            let prop_id = p.get_property_id();
            let v = p.get_property_value();
            write!(f, "{{{}: {:?}}}", prop_id, v)?;
        }
        write!(f, ">")
    }
}

impl RocksVertex for RocksVertexImpl {
    fn get_vertex_id(&self) -> VertexId {
        self.vertex_id
    }

    fn get_label_id(&self) -> LabelId {
        self.label_id
    }
}

unsafe impl Send for RocksVertexImpl {}
unsafe impl Sync for RocksVertexImpl {}

impl Vertex for RocksVertexImpl {
    type PI = Box<dyn Iterator<Item=(PropId, crate::api::prelude::Property)>>;

    fn get_id(&self) -> crate::api::VertexId {
        self.vertex_id
    }

    fn get_label_id(&self) -> crate::api::LabelId {
        self.label_id as u32
    }

    fn get_property(&self, prop_id: PropId) -> Option<crate::api::prelude::Property> {
        let property_id = prop_id as i32;
        match &self.decoder {
            None => None,
            Some(decoder) => {
                let bytes = unsafe { self.raw_bytes.to_slice() };
                let value_ref = decoder.decode_property(bytes, property_id);
                value_ref.map(|v| {
                    PropertyImpl {
                        property_id,
                        property_value: v.into(),
                    }.parse_to_property().1
                })
            }
        }
    }

    fn get_properties(&self) -> Self::PI {
        match &self.decoder {
            None => Box::new(::std::iter::empty()),
            Some(decoder) => {
                let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
                let decode_iter = decoder.decode_properties(bytes);
                Box::new(PropertiesIter {
                    decode_iter
                }.map(|p| p.unwrap().parse_to_property()))
            }
        }
    }
}

pub struct RocksEdgeImpl {
    edge_id: EdgeId,
    edge_relation: EdgeKind,
    decoder: Option<Decoder>,
    raw_bytes: RawBytes,
}

impl RocksEdgeImpl {
    pub fn new(edge_id: EdgeId, edge_relation: EdgeKind, decoder: Option<Decoder>, raw_bytes: RawBytes) -> Self {
        RocksEdgeImpl { edge_id, edge_relation, decoder, raw_bytes }
    }
}

impl PropertyReader for RocksEdgeImpl {
    type P = PropertyImpl;
    type PropertyIterator = Box<dyn Iterator<Item=GraphResult<Self::P>>>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        match &self.decoder {
            None => None,
            Some(decoder) => {
                let bytes = unsafe { self.raw_bytes.to_slice() };
                let value_ref = decoder.decode_property(bytes, property_id as i32);
                value_ref.map(|v| {
                    PropertyImpl {
                        property_id,
                        property_value: v.into(),
                    }
                })
            }
        }
    }

    fn get_property_iterator(&self) -> Box<dyn Iterator<Item=GraphResult<Self::P>>> {
        match &self.decoder {
            None => Box::new(::std::iter::empty()),
            Some(decoder) => {
                let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
                let decode_iter = decoder.decode_properties(bytes);
                Box::new(PropertiesIter {
                    decode_iter
                })
            }
        }
    }
}

impl Debug for RocksEdgeImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{:?}, {:?}, properties: ", self.edge_id, self.get_edge_relation())?;
        let mut iter = self.get_property_iterator();
        while let Some(p) = iter.next() {
            let p = p.unwrap();
            let prop_id = p.get_property_id();
            let v = p.get_property_value();
            write!(f, "{{{}: {:?}}}", prop_id, v)?;
        }
        write!(f, ">")
    }
}

impl RocksEdge for RocksEdgeImpl {
    fn get_edge_id(&self) -> &EdgeId {
        &self.edge_id
    }

    fn get_edge_relation(&self) -> &EdgeKind {
        &self.edge_relation
    }
}

unsafe impl Send for RocksEdgeImpl {}
unsafe impl Sync for RocksEdgeImpl {}

impl Edge for RocksEdgeImpl {
    type PI = Box<dyn Iterator<Item=(PropId, crate::api::prelude::Property)>>;

    fn get_label_id(&self) -> crate::api::LabelId {
        self.edge_relation.edge_label_id as crate::api::LabelId
    }

    fn get_src_label_id(&self) -> crate::api::LabelId {
        self.edge_relation.src_vertex_label_id as crate::api::LabelId
    }

    fn get_dst_label_id(&self) -> crate::api::LabelId {
        self.edge_relation.dst_vertex_label_id as crate::api::LabelId
    }

    fn get_src_id(&self) -> crate::api::VertexId {
        self.edge_id.src_id
    }

    fn get_dst_id(&self) -> crate::api::VertexId {
        self.edge_id.dst_id
    }

    fn get_edge_id(&self) -> crate::api::EdgeId {
        self.edge_id.inner_id
    }

    fn get_property(&self, prop_id: PropId) -> Option<crate::api::prelude::Property> {
        let property_id = prop_id as i32;
        match &self.decoder {
            None => None,
            Some(decoder) => {
                let bytes = unsafe { self.raw_bytes.to_slice() };
                let value_ref = decoder.decode_property(bytes, property_id);
                value_ref.map(|v| {
                    PropertyImpl {
                        property_id,
                        property_value: v.into(),
                    }.parse_to_property().1
                })
            }
        }
    }

    fn get_properties(&self) -> Self::PI {
        match &self.decoder {
            None => Box::new(::std::iter::empty()),
            Some(decoder) => {
                let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
                let decode_iter = decoder.decode_properties(bytes);
                Box::new(PropertiesIter {
                    decode_iter
                }.map(|p| p.unwrap().parse_to_property()))
            }
        }
    }
}
