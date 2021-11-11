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

use crate::v2::errors::GraphError;
use crate::db::api::{ValueRef, ValueType};
use crate::v2::api::PropertyValue;

pub mod api;
pub mod errors;
pub mod wrapper;
mod multi_version_graph;
mod graph;

pub type GraphResult<T> = std::result::Result<T, GraphError>;

fn parse_property_value(value_ref: ValueRef) -> PropertyValue {
    match value_ref.get_type() {
        ValueType::Bool => PropertyValue::Boolean(value_ref.get_bool().unwrap()),
        ValueType::Char => PropertyValue::Char(char::from(value_ref.get_char().unwrap())),
        ValueType::Short => PropertyValue::Short(value_ref.get_short().unwrap()),
        ValueType::Int => PropertyValue::Int(value_ref.get_int().unwrap()),
        ValueType::Long => PropertyValue::Long(value_ref.get_long().unwrap()),
        ValueType::Float => PropertyValue::Float(value_ref.get_float().unwrap()),
        ValueType::Double => PropertyValue::Double(value_ref.get_double().unwrap()),
        ValueType::String => PropertyValue::String(String::from(value_ref.get_str().unwrap())),
        ValueType::Bytes => PropertyValue::Bytes(Vec::from(value_ref.get_bytes().unwrap())),
        ValueType::IntList => PropertyValue::IntList(value_ref.get_int_list().unwrap().iter().collect()),
        ValueType::LongList => PropertyValue::LongList(value_ref.get_long_list().unwrap().iter().collect()),
        ValueType::FloatList => PropertyValue::FloatList(value_ref.get_float_list().unwrap().iter().collect()),
        ValueType::DoubleList => PropertyValue::DoubleList(value_ref.get_double_list().unwrap().iter().collect()),
        ValueType::StringList => PropertyValue::StringList(value_ref.get_str_list().unwrap().iter()
            .map(String::from).collect()),
    }
}
