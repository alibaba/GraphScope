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

use maxgraph_store::schema::Schema;
use maxgraph_store::schema::prelude::DataType;
use maxgraph_store::api::prelude::Property;

use dataflow::message::{PropertyEntity, ValuePayload, RawMessage};
use dataflow::message::primitive::Write;
use dataflow::test::MockSchema;

use std::sync::Arc;
use std::io::Cursor;
use std::collections::HashMap;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use dataflow::message::RawMessageType::PROP;

pub fn parse_property_entity_list(labelid: u32,
                                  props: Vec<u8>,
                                  schema: &Arc<Schema>) -> Option<Vec<(i32, Property)>> {
    let mut prop_val_list = vec![];
    let mut rdr = Cursor::new(props);
    if let Ok(prop_count) = rdr.read_i32::<BigEndian>() {
        for i in 0..prop_count {
            if let Ok(propid) = rdr.read_i32::<BigEndian>() {
                if let Some(data_type) = schema.as_ref().get_prop_type(labelid, propid as u32) {
                    if let Some(val_payload) = parse_payload_from_prop_bytes(data_type, &mut rdr) {
                        prop_val_list.push((propid, val_payload));
                    } else {
                        error!("parse invalid property");
                        return None;
                    }
                }
            }
        }
    }
    return Some(prop_val_list);
}

fn parse_payload_from_prop_bytes(data_type: &DataType,
                                 props: &mut Cursor<Vec<u8>>) -> Option<Property> {
    match data_type {
        DataType::Bool => {
            if let Ok(val) = props.read_u8() {
                let bool_val = {
                    if val == 0 {
                        false
                    } else {
                        true
                    }
                };
                return Some(Property::Bool(bool_val));
            }
        }
        DataType::Char => {
            if let Ok(val) = props.read_u8() {
                return Some(Property::Char(val));
            }
        }
        DataType::Short => {
            if let Ok(val) = props.read_i16::<BigEndian>() {
                return Some(Property::Short(val));
            }
        }
        DataType::Int => {
            if let Ok(val) = props.read_i32::<BigEndian>() {
                return Some(Property::Int(val));
            }
        }
        DataType::Long => {
            if let Ok(val) = props.read_i64::<BigEndian>() {
                return Some(Property::Long(val));
            }
        }
        DataType::Float => {
            if let Ok(val) = props.read_f32::<BigEndian>() {
                return Some(Property::Float(val));
            }
        }
        DataType::Double => {
            if let Ok(val) = props.read_f64::<BigEndian>() {
                return Some(Property::Double(val));
            }
        }
        DataType::Bytes => {
            if let Ok(len) = props.read_i32::<BigEndian>() {
                let mut bytes = Vec::with_capacity(len as usize);
                for i in 0..len {
                    if let Ok(val) = props.read_u8() {
                        bytes.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(Property::Bytes(bytes));
            }
        }
        DataType::String => {
            if let Ok(len) = props.read_i32::<BigEndian>() {
                let mut bytes = Vec::with_capacity(len as usize);
                for i in 0..len {
                    if let Ok(val) = props.read_u8() {
                        bytes.push(val);
                    } else {
                        return None;
                    }
                }
                if let Ok(str_val) = String::from_utf8(bytes) {
                    return Some(Property::String(str_val));
                } else {
                    return None;
                }
            }
        }
        DataType::Date => {
            if let Ok(len) = props.read_i32::<BigEndian>() {
                let mut bytes = Vec::with_capacity(len as usize);
                for i in 0..len {
                    if let Ok(val) = props.read_u8() {
                        bytes.push(val);
                    } else {
                        return None;
                    }
                }
                if let Ok(str_val) = String::from_utf8(bytes) {
                    return Some(Property::Date(str_val));
                } else {
                    return None;
                }
            }
        }
        DataType::ListInt => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                for i in 0..val_count {
                    if let Ok(val) = props.read_i32::<BigEndian>() {
                        list_val.push(val);
                    }
                }
                return Some(Property::ListInt(list_val));
            }
        }
        DataType::ListLong => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                for i in 0..val_count {
                    if let Ok(val) = props.read_i64::<BigEndian>() {
                        list_val.push(val);
                    }
                }
                return Some(Property::ListLong(list_val));
            }
        }
        DataType::ListString => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                for i in 0..val_count {
                    if let Ok(len) = props.read_i32::<BigEndian>() {
                        let mut bytes = Vec::with_capacity(len as usize);
                        for i in 0..len {
                            if let Ok(val) = props.read_u8() {
                                bytes.push(val);
                            } else {
                                return None;
                            }
                        }
                        if let Ok(str_val) = String::from_utf8(bytes) {
                            list_val.push(str_val);
                        } else {
                            return None;
                        }
                    }
                }
                return Some(Property::ListString(list_val));
            }
        }
        DataType::ListFloat => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                for i in 0..val_count {
                    if let Ok(val) = props.read_f32::<BigEndian>() {
                        list_val.push(val);
                    }
                }
                return Some(Property::ListFloat(list_val));
            }
        }
        DataType::ListDouble => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                for i in 0..val_count {
                    if let Ok(val) = props.read_f64::<BigEndian>() {
                        list_val.push(val);
                    }
                }
                return Some(Property::ListDouble(list_val));
            }
        }
        DataType::ListBytes => {
            if let Ok(val_count) = props.read_i32::<BigEndian>() {
                let mut list_val = Vec::with_capacity(val_count as usize);
                if let Ok(len) = props.read_i32::<BigEndian>() {
                    let mut bytes = Vec::with_capacity(len as usize);
                    for i in 0..len {
                        if let Ok(val) = props.read_u8() {
                            bytes.push(val);
                        } else {
                            return None;
                        }
                    }
                    list_val.push(bytes);
                }
                return Some(Property::ListBytes(list_val));
            }
        }
        _ => {
            error!("Not support data type {:?}", data_type);
        }
    }

    return None;
}
