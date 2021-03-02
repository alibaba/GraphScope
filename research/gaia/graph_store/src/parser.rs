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

use crate::common::{Label, LabelId};
use crate::error::{GDBError, GDBResult};
use crate::schema::*;
use crate::table::Row;
use std::fmt::Debug;
use std::str::FromStr;

/// The supported data type of this graph database
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum DataType {
    NULL,
    String,
    Integer,
    Long,
    Float,
    Date,
    ID,
    LABEL,
}

impl<'a> From<&'a str> for DataType {
    fn from(_token: &'a str) -> Self {
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "STRING" || token == "STRING[]" {
            DataType::String
        } else if token == "LONG" {
            DataType::Long
        } else if token == "INT" {
            DataType::Integer
        } else if token == "DATE" {
            DataType::Date
        } else if token == "ID" {
            DataType::ID
        } else if token == "LABEL" {
            DataType::LABEL
        } else {
            error!("Unsupported type {:?}", token);
            DataType::NULL
        }
    }
}

/// Parse one item from `&str` in the iterator to a given type `T`.
/// After the parsing, the iterator will be moved to the next item.
///
/// Return
///     * succeed: the parsed data of type `T` succeed
///     * parsing error: `GDBError::ParseError`
///     * empty iterator: `GDBError::OutOfBoundError`
fn _parse_one_item_to<'a, T: FromStr, Iter: Iterator<Item = &'a str>>(
    iter: &mut Iter,
) -> GDBResult<T> {
    if let Some(item) = iter.next() {
        item.parse::<T>().map_err(|_| GDBError::ParseError)
    } else {
        Err(GDBError::OutOfBoundError)
    }
}

/// The vertex's meta data including global_id and label_id
#[derive(Abomonation, PartialEq, Clone, Debug)]
pub struct VertexMeta<G> {
    pub global_id: G,
    pub label: Label,
}

/// The edge's meta data after parsing from the csv file.
#[derive(Abomonation, PartialEq, Clone, Debug)]
pub struct EdgeMeta<G> {
    pub src_global_id: G,
    pub src_label_id: LabelId,
    pub dst_global_id: G,
    pub dst_label_id: LabelId,
    pub label_id: LabelId,
}

/// Typical symbols that split a string-format of a time data.
fn is_time_splitter(c: char) -> bool {
    c == '-' || c == ':' || c == ' ' || c == 'T' || c == 'Z'
}

pub fn parse_properties<'a, Iter: Iterator<Item = &'a str>>(
    mut record_iter: Iter, _header: Option<&[(String, DataType)]>,
) -> GDBResult<Row> {
    let mut properties = Row::default();
    if _header.is_none() {
        return Ok(properties);
    }

    let header = _header.unwrap();
    let mut header_iter = header.iter();

    while let Some(val) = record_iter.next() {
        // unwrap the property and type
        if let Some((field, ty)) = header_iter.next() {
            if ty == &DataType::String {
                properties.push(json!(val.to_string()));
            } else if ty == &DataType::Integer {
                properties.push(json!(val.parse::<i32>()?));
            } else if ty == &DataType::Long {
                properties.push(json!(val.parse::<i64>()?));
            } else if ty == &DataType::Float {
                properties.push(json!(val.parse::<f32>()?));
            } else if ty == &DataType::Date {
                let mut _date = String::with_capacity(val.len());
                for c in val.chars() {
                    if c == '.' {
                        // "2012-07-21T07:59:14.322+000", skip the content after "."
                        break;
                    } else if is_time_splitter(c) {
                        continue; // replace any time splitter with void
                    } else {
                        _date.push(c);
                    }
                }
                properties.push(json!(_date.parse::<u64>()?));
            } else if ty == &DataType::ID {
                // do not record the starting (ldbc) id and end id of an edge
                if field != START_ID_FIELD && field != END_ID_FIELD {
                    properties.push(json!(val.parse::<usize>()?));
                }
            } else if ty == &DataType::LABEL {
                // do not further record the label of a vertex
                continue;
            } else {
                return GDBResult::Err(GDBError::ParseError);
            }
        }
    }

    debug!("Parse properties successfully: {:?}", properties);

    Ok(properties)
}

/// Define a trait for parsing a record into a vertex or edge. Specifically,
/// * a vertex has `VertexMeta` and its properties
/// * an edge has `EdgeMeta` and its properties
///
/// This trait regulates the parsing of `VertexMeta`, `EdgeMeta` and the properties, respectively.
pub trait ParserTrait<G: FromStr + PartialEq> {
    /// Parsing a line of record (as an iterator) into a `VertexMeta`.
    /// After the parsing, the record iterator will move forward skipping the `VertexMeta` fields.
    fn parse_vertex_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<VertexMeta<G>>;

    /// Parsing a line of record (as an iterator) into an `EdgeMeta`.
    /// After the parsing, the record iterator will move forward skipping the `EdgeMeta` fields.
    fn parse_edge_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<EdgeMeta<G>>;
}
