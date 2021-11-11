use crate::v2::api::{Vertex, VertexId, LabelId, PropertyReader, PropertyId, Property, PropertyValue, Edge, EdgeId, EdgeRelation};
use crate::db::graph::codec::{Decoder, IterDecoder};
use crate::db::storage::RawBytes;
use crate::v2::{parse_property_value, GraphResult};

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
            let property_value = parse_property_value(v);
            Ok(PropertyImpl {
                property_id: prop_id as PropertyId,
                property_value,
            })
        })
    }
}

pub struct VertexImpl {
    vertex_id: VertexId,
    label_id: LabelId,
    decoder: Decoder,
    raw_bytes: RawBytes,
}

impl VertexImpl {
    pub fn new(vertex_id: VertexId, label_id: LabelId, decoder: Decoder, raw_bytes: RawBytes) -> Self {
        VertexImpl { vertex_id, label_id, decoder, raw_bytes }
    }
}

impl PropertyReader for VertexImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertiesIter<'static>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        let bytes = unsafe { self.raw_bytes.to_slice() };
        let value_ref = self.decoder.decode_property(bytes, property_id as i32);
        value_ref.map(|v| {
            let property_value = parse_property_value(v);
            PropertyImpl {
                property_id,
                property_value,
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

impl Vertex for VertexImpl {
    fn get_vertex_id(&self) -> VertexId {
        self.vertex_id
    }

    fn get_label_id(&self) -> LabelId {
        self.label_id
    }
}

pub struct EdgeImpl {
    edge_id: EdgeId,
    edge_relation: EdgeRelation,
    decoder: Decoder,
    raw_bytes: RawBytes,
}

impl EdgeImpl {
    pub fn new(edge_id: EdgeId, edge_relation: EdgeRelation, decoder: Decoder, raw_bytes: RawBytes) -> Self {
        EdgeImpl { edge_id, edge_relation, decoder, raw_bytes }
    }
}

impl PropertyReader for EdgeImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertiesIter<'static>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P> {
        let bytes = unsafe { self.raw_bytes.to_slice() };
        let value_ref = self.decoder.decode_property(bytes, property_id as i32);
        value_ref.map(|v| {
            let property_value = parse_property_value(v);
            PropertyImpl {
                property_id,
                property_value,
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

impl Edge for EdgeImpl {
    fn get_edge_id(&self) -> &EdgeId {
        &self.edge_id
    }

    fn get_edge_relation(&self) -> &EdgeRelation {
        &self.edge_relation
    }
}
