//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::fmt::Debug;

use crate::db::api::{EdgeId, EdgeKind, GraphResult, LabelId, PropertyId, ValueRef, ValueType, VertexId};

#[repr(C)]
#[allow(dead_code)]
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

#[derive(Clone, Debug, PartialEq)]
pub enum PropertyValue {
    Null,
    Boolean(bool),
    Char(char),
    Short(i16),
    Int(i32),
    UInt(u32),
    Long(i64),
    ULong(u64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    IntList(Vec<i32>),
    LongList(Vec<i64>),
    FloatList(Vec<f32>),
    DoubleList(Vec<f64>),
    StringList(Vec<String>),
    Date32(i32),
    Time32(i32),
    Timestamp(i64),
    FixedChar(FixedCharValue),
    VarChar(VarCharValue),
}

#[derive(Clone, Debug, PartialEq)]
pub struct FixedCharValue {
    value: Vec<char>,
    fixed_length: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VarCharValue {
    value: Vec<char>,
    max_length: usize,
}

impl From<ValueRef<'_>> for PropertyValue {
    fn from(value_ref: ValueRef) -> Self {
        match value_ref.get_type() {
            ValueType::Bool => PropertyValue::Boolean(value_ref.get_bool().unwrap()),
            ValueType::Char => PropertyValue::Char(char::from(value_ref.get_char().unwrap())),
            ValueType::Short => PropertyValue::Short(value_ref.get_short().unwrap()),
            ValueType::Int => PropertyValue::Int(value_ref.get_int().unwrap()),
            ValueType::UInt => PropertyValue::UInt(value_ref.get_uint().unwrap()),
            ValueType::Long => PropertyValue::Long(value_ref.get_long().unwrap()),
            ValueType::ULong => PropertyValue::ULong(value_ref.get_ulong().unwrap()),
            ValueType::Float => PropertyValue::Float(value_ref.get_float().unwrap()),
            ValueType::Double => PropertyValue::Double(value_ref.get_double().unwrap()),
            ValueType::String => PropertyValue::String(String::from(value_ref.get_str().unwrap())),
            ValueType::Bytes => PropertyValue::Bytes(Vec::from(value_ref.get_bytes().unwrap())),
            ValueType::IntList => PropertyValue::IntList(
                value_ref
                    .get_int_list()
                    .unwrap()
                    .iter()
                    .collect(),
            ),
            ValueType::LongList => PropertyValue::LongList(
                value_ref
                    .get_long_list()
                    .unwrap()
                    .iter()
                    .collect(),
            ),
            ValueType::FloatList => PropertyValue::FloatList(
                value_ref
                    .get_float_list()
                    .unwrap()
                    .iter()
                    .collect(),
            ),
            ValueType::DoubleList => PropertyValue::DoubleList(
                value_ref
                    .get_double_list()
                    .unwrap()
                    .iter()
                    .collect(),
            ),
            ValueType::StringList => PropertyValue::StringList(
                value_ref
                    .get_str_list()
                    .unwrap()
                    .iter()
                    .map(String::from)
                    .collect(),
            ),
            ValueType::Date32 => PropertyValue::Date32(value_ref.get_date32().unwrap()),
            ValueType::Time32 => PropertyValue::Time32(value_ref.get_time32().unwrap()),
            ValueType::Timestamp => PropertyValue::Timestamp(value_ref.get_timestamp().unwrap()),
            ValueType::FixedChar(fixed) => PropertyValue::FixedChar(FixedCharValue {
                value: value_ref
                    .get_fixed_char()
                    .unwrap()
                    .chars()
                    .collect(),
                fixed_length: *fixed,
            }),
            ValueType::VarChar(max) => PropertyValue::VarChar(VarCharValue {
                value: value_ref
                    .get_var_char()
                    .unwrap()
                    .chars()
                    .collect(),
                max_length: *max,
            }),
        }
    }
}

pub trait Property {
    fn get_property_id(&self) -> PropertyId;
    fn get_property_value(&self) -> &PropertyValue;
}

pub trait PropertyReader {
    type P: Property;
    type PropertyIterator: Iterator<Item = GraphResult<Self::P>>;

    fn get_property(&self, property_id: PropertyId) -> Option<Self::P>;
    fn get_property_iterator(&self) -> Self::PropertyIterator;
}

pub trait RocksVertex: PropertyReader + Debug {
    fn get_vertex_id(&self) -> VertexId;
    fn get_label_id(&self) -> LabelId;
}

pub trait RocksEdge: PropertyReader + Debug {
    fn get_edge_id(&self) -> &EdgeId;
    fn get_edge_relation(&self) -> &EdgeKind;
}
