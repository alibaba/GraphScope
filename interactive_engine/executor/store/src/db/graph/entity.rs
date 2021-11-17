use crate::db::graph::codec::{Decoder, IterDecoder};
use crate::db::storage::RawBytes;
use crate::db::api::{GraphResult, VertexId, LabelId, EdgeId, EdgeKind, PropertyId};
use crate::db::api::types::{PropertyReader, PropertyValue, Property, RocksVertex, RocksEdge};

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
    decoder: Decoder,
    raw_bytes: RawBytes,
}

impl RocksVertexImpl {
    pub fn new(vertex_id: VertexId, label_id: LabelId, decoder: Decoder, raw_bytes: RawBytes) -> Self {
        RocksVertexImpl { vertex_id, label_id, decoder, raw_bytes }
    }
}

impl PropertyReader for RocksVertexImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertiesIter<'static>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        let bytes = unsafe { self.raw_bytes.to_slice() };
        let value_ref = self.decoder.decode_property(bytes, property_id as i32);
        value_ref.map(|v| {
            PropertyImpl {
                property_id,
                property_value: v.into(),
            }
        })
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
        let decode_iter = self.decoder.decode_properties(bytes);
        PropertiesIter {
            decode_iter
        }
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

pub struct RocksEdgeImpl {
    edge_id: EdgeId,
    edge_relation: EdgeKind,
    decoder: Decoder,
    raw_bytes: RawBytes,
}

impl RocksEdgeImpl {
    pub fn new(edge_id: EdgeId, edge_relation: EdgeKind, decoder: Decoder, raw_bytes: RawBytes) -> Self {
        RocksEdgeImpl { edge_id, edge_relation, decoder, raw_bytes }
    }
}

impl PropertyReader for RocksEdgeImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertiesIter<'static>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        let bytes = unsafe { self.raw_bytes.to_slice() };
        let value_ref = self.decoder.decode_property(bytes, property_id as i32);
        value_ref.map(|v| {
            PropertyImpl {
                property_id,
                property_value: v.into(),
            }
        })
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        let bytes = unsafe { std::mem::transmute(self.raw_bytes.to_slice()) };
        let decode_iter = self.decoder.decode_properties(bytes);
        PropertiesIter {
            decode_iter
        }
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
