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

use dataflow::message::{RawMessage, RawMessageType};
use utils::{PROP_ID, PROP_ID_LABEL, PROP_KEY, PROP_VALUE};
use maxgraph_store::api::graph_schema::Schema;

pub fn get_message_int_prop_value(prop_id: i32,
                                  message: &RawMessage) -> Option<i32> {
    if prop_id == 0 {
        if let Some(value) = message.get_value() {
            if let Ok(vl) = value.get_int() {
                return Some(vl);
            }
        }
    } else if prop_id == PROP_ID {
        return Some(message.get_id() as i32);
    } else if prop_id == PROP_ID_LABEL {
        return Some(message.get_label_id() as i32);
    } else if prop_id == PROP_KEY {
        if let Some(entry) = message.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_int() {
                    return Some(val);
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if message.get_message_type() == RawMessageType::PROP &&
            message.get_label_id() == 0 {
            if let Some(payload) = message.get_value() {
                if let Ok(val) = payload.get_int() {
                    return Some(val);
                }
            }
        } else if let Some(entry) = message.get_entry_value() {
            if let Some(key) = entry.get_value().get_value() {
                if let Ok(val) = key.get_int() {
                    return Some(val);
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = message.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_int() {
                    return Some(val);
                }
            }
        }
    } else if prop_id > 0 {
        if let Some(prop) = message.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_int() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_message_long_prop_value(prop_id: i32,
                                   message: &RawMessage) -> Option<i64> {
    if prop_id == 0 {
        if let Some(value) = message.get_value() {
            if let Ok(vl) = value.get_long() {
                return Some(vl);
            }
        }
    } else if prop_id == PROP_ID {
        return Some(message.get_id());
    } else if prop_id == PROP_ID_LABEL {
        return Some(message.get_label_id() as i64);
    } else if prop_id == PROP_KEY {
        if let Some(entry) = message.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_long() {
                    return Some(val);
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if message.get_message_type() == RawMessageType::PROP &&
            message.get_label_id() == 0 {
            if let Some(payload) = message.get_value() {
                if let Ok(val) = payload.get_long() {
                    return Some(val);
                }
            }
        } else if let Some(entry) = message.get_entry_value() {
            if let Some(key) = entry.get_value().get_value() {
                if let Ok(val) = key.get_long() {
                    return Some(val);
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = message.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_long() {
                    return Some(val);
                }
            }
        }
    } else if prop_id > 0 {
        if let Some(prop) = message.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_long() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_message_string_prop_value(prop_id: i32,
                                     v: &RawMessage,
                                     schema: &Schema) -> Option<String> {
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(vl) = value.get_string() {
                return Some(vl.to_owned());
            }
        }
    } else if prop_id == PROP_ID {
        return Some(v.get_id().to_string());
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id().to_string());
    } else if prop_id == PROP_KEY {
        if v.get_message_type() == RawMessageType::PROP {
            if v.get_label_id() == 0 {
                if let Some(prop_name) = schema.get_prop_name(v.get_id() as u32) {
                    return Some(prop_name.to_owned());
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_string() {
                    return Some(val.to_owned());
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP {
            if v.get_label_id() == 0 {
                if let Some(val) = v.get_value() {
                    if let Ok(vv) = val.get_string() {
                        return Some(vv.clone());
                    }
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_value().get_value() {
                if let Ok(val) = key.get_string() {
                    return Some(val.to_owned());
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_string() {
                    return Some(val.to_owned());
                }
            }
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_string() {
                return Some(val.to_owned());
            }
        }
    }
    return None;
}

pub fn get_message_bool_prop_value(prop_id: i32,
                                   v: &RawMessage) -> Option<bool> {
    if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_bool() {
                return Some(val);
            }
        }
    } else if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(val) = value.get_bool() {
                return Some(val);
            }
        }
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_bool() {
                    return Some(val);
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(val) = payload.get_bool() {
                    return Some(val);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(value) = entry.get_value().get_value() {
                if let Ok(val) = value.get_bool() {
                    return Some(val);
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_bool() {
                    return Some(val);
                }
            }
        }
    }
    return None;
}

pub fn get_message_float_prop_value(prop_id: i32,
                                    v: &RawMessage) -> Option<f32> {
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(vl) = value.get_float() {
                return Some(vl);
            }
        }
    } else if prop_id == PROP_ID {
        return Some(v.get_id() as f32);
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id() as f32);
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_float() {
                    return Some(val);
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(val) = payload.get_float() {
                    return Some(val);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_value().get_value() {
                if let Ok(val) = key.get_float() {
                    return Some(val);
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_float() {
                    return Some(val);
                }
            }
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_float() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_message_double_prop_value(prop_id: i32,
                                     v: &RawMessage) -> Option<f64> {
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(vl) = value.get_double() {
                return Some(vl);
            }
        }
    } else if prop_id == PROP_ID {
        return Some(v.get_id() as f64);
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id() as f64);
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_key().get_value() {
                if let Ok(val) = key.get_double() {
                    return Some(val);
                }
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(val) = payload.get_double() {
                    return Some(val);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(key) = entry.get_value().get_value() {
                if let Ok(val) = key.get_double() {
                    return Some(val);
                }
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            if let Some(value) = label.get_message().get_value() {
                if let Ok(val) = value.get_double() {
                    return Some(val);
                }
            }
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_double() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_message_int_list_prop_value(prop_id: i32,
                                       v: &RawMessage) -> Option<Vec<i32>> {
    let mut vallist = vec![];
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(list) = value.get_list() {
                for m in list.iter() {
                    if let Some(v) = m.get_value() {
                        if let Ok(vi) = v.get_int() {
                            vallist.push(vi);
                        }
                    }
                }
            }
        }
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_key().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_int_prop_value(0, m) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(list) = payload.get_list() {
                    let mut int_list = Vec::with_capacity(list.len());
                    for m in list.iter() {
                        if let Some(val) = get_message_int_prop_value(0, m) {
                            int_list.push(val);
                        } else {
                            return None;
                        }
                    }
                    return Some(int_list);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_value().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_int_prop_value(0, m) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            return get_message_int_list_prop_value(0, label.get_message());
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(val) = prop.get_value().get_list() {
                for intval in val.iter() {
                    if let Some(real_val) = get_message_int_prop_value(0, intval) {
                        vallist.push(real_val);
                    }
                }
            }
        }
    }
    return Some(vallist);
}

pub fn get_message_long_list_prop_value(prop_id: i32,
                                        v: &RawMessage) -> Option<Vec<i64>> {
    let mut vallist = vec![];
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(list) = value.get_list() {
                for m in list.iter() {
                    if let Some(v) = m.get_value() {
                        if let Ok(vi) = v.get_long() {
                            vallist.push(vi);
                        }
                    }
                }
            }
        }
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_key().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_long_prop_value(0, m) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(list) = payload.get_list() {
                    let mut int_list = Vec::with_capacity(list.len());
                    for m in list.iter() {
                        if let Some(val) = get_message_long_prop_value(0, m) {
                            int_list.push(val);
                        } else {
                            return None;
                        }
                    }
                    return Some(int_list);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_value().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_long_prop_value(0, m) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            return get_message_long_list_prop_value(0, label.get_message());
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(list) = prop.get_value().get_list() {
                for val in list.iter() {
                    if let Some(longval) = get_message_long_prop_value(0, val) {
                        vallist.push(longval);
                    }
                }
            }
        }
    }
    return Some(vallist);
}

pub fn get_message_string_list_prop_value(prop_id: i32,
                                          v: &RawMessage,
                                          schema: &Schema) -> Option<Vec<String>> {
    let mut vallist = vec![];
    if prop_id == 0 {
        if let Some(value) = v.get_value() {
            if let Ok(list) = value.get_list() {
                for m in list.iter() {
                    if let Some(v) = m.get_value() {
                        if let Ok(vi) = v.get_string() {
                            vallist.push(vi.to_owned());
                        }
                    }
                }
            }
        }
    } else if prop_id == PROP_KEY {
        if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_key().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_string_prop_value(0, m, schema) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id == PROP_VALUE {
        if v.get_message_type() == RawMessageType::PROP &&
            v.get_label_id() == 0 {
            if let Some(payload) = v.get_value() {
                if let Ok(list) = payload.get_list() {
                    let mut int_list = Vec::with_capacity(list.len());
                    for m in list.iter() {
                        if let Some(val) = get_message_string_prop_value(0, m, schema) {
                            int_list.push(val);
                        } else {
                            return None;
                        }
                    }
                    return Some(int_list);
                }
            }
        } else if let Some(entry) = v.get_entry_value() {
            if let Some(list) = entry.get_value().get_list_value() {
                let mut int_list = Vec::with_capacity(list.len());
                for m in list.iter() {
                    if let Some(val) = get_message_string_prop_value(0, m, schema) {
                        int_list.push(val);
                    } else {
                        return None;
                    }
                }
                return Some(int_list);
            }
        }
    } else if prop_id < 0 {
        if let Some(label) = v.get_label_entity_by_id(prop_id) {
            return get_message_string_list_prop_value(0, label.get_message(), schema);
        }
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id) {
            if let Ok(list) = prop.get_value().get_list() {
                for val in list.iter() {
                    if let Some(strval) = get_message_string_prop_value(0, val, schema) {
                        vallist.push(strval);
                    }
                }
            }
        }
    }
    return Some(vallist);
}
