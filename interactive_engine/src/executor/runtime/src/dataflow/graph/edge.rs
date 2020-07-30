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

use maxgraph_store::api::prelude::Edge;
use maxgraph_store::schema::PropId;
use utils::{PROP_ID, PROP_ID_LABEL};

pub fn get_edge_int_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<i32> {
    if prop_id == PROP_ID {
        return Some(v.get_edge_id() as i32);
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id() as i32);
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_int() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_edge_long_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<i64> {
    if prop_id == PROP_ID {
        return Some(v.get_edge_id());
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id() as i64);
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_long() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_edge_string_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<String> {
    if prop_id == PROP_ID {
        return Some(v.get_edge_id().to_string());
    } else if prop_id == PROP_ID_LABEL {
        return Some(v.get_label_id().to_string());
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_string() {
                return Some(val.to_owned());
            }
        }
    }
    return None;
}

pub fn get_edge_bool_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<bool> {
    if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_bool() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_edge_float_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<f32> {
    if prop_id == PROP_ID {
        return Some(v.get_edge_id() as f32);
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_float() {
                return Some(val);
            }
        }
    }
    return None;
}

pub fn get_edge_double_prop_value<E: Edge>(prop_id: i32, v: &E) -> Option<f64> {
    if prop_id == PROP_ID {
        return Some(v.get_edge_id() as f64);
    } else if prop_id > 0 {
        if let Some(prop) = v.get_property(prop_id as PropId) {
            if let Ok(val) = prop.get_double() {
                return Some(val);
            }
        }
    }
    return None;
}
