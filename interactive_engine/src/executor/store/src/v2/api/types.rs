//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::v2::api::{PropertyId, VertexId, LabelId, EdgeId};

pub enum DataType {
    Boolean,
    Char,
    Short,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    IntList,
    LongList,
    FloatList,
    DoubleList,
    StringList,
}

pub enum PropertyValue {
    Null,
    Boolean(bool),
    Char(char),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    IntList(Vec<i32>),
    LongList(Vec<i64>),
    FloatList(Vec<f32>),
    DoubleList(Vec<f64>),
    StringList(Vec<String>),
}

pub trait Property {
    fn get_property_id(&self) -> PropertyId;
    fn get_property_value(&self) -> &PropertyValue;
}

pub trait PropertyReader {
    type P: Property;
    type PropertyIterator: Iterator<Item=Self::P>;

    fn get_property_value(&self, property_id: PropertyId) -> Option<&PropertyValue>;
    fn get_property_iterator(&self) -> Self::PropertyIterator;
}

pub trait Vertex: PropertyReader {
    fn get_vertex_id(&self) -> VertexId;
    fn get_label_id(&self) -> LabelId;
}

pub struct EdgeRelation {
    edge_label_id: LabelId,
    src_vertex_label_id: LabelId,
    dst_vertex_label_id: LabelId,
}

impl EdgeRelation {
    pub fn new(edge_label_id: LabelId, src_vertex_label_id: LabelId, dst_vertex_label_id: LabelId) -> Self {
        EdgeRelation {
            edge_label_id,
            src_vertex_label_id,
            dst_vertex_label_id,
        }
    }

    pub fn get_edge_label_id(&self) -> LabelId {
        self.edge_label_id
    }
    pub fn get_src_vertex_label_id(&self) -> LabelId {
        self.src_vertex_label_id
    }
    pub fn get_dst_vertex_label_id(&self) -> LabelId {
        self.dst_vertex_label_id
    }
}

pub trait Edge: PropertyReader {
    fn get_edge_id(&self) -> &EdgeId;
    fn get_edge_relation(&self) -> &EdgeRelation;
}
