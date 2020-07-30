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

use maxgraph_common::proto::message::LogicalCompare;
use maxgraph_store::api::prelude::{Vertex, Edge};

use dataflow::graph::vertex::get_vertex_string_prop_value;
use dataflow::graph::edge::get_edge_string_prop_value;
use dataflow::message::RawMessage;
use dataflow::graph::message::get_message_string_prop_value;

use regex::Regex;
use maxgraph_store::api::graph_schema::Schema;

#[derive(Debug, Clone)]
pub struct RegexFilter {
    pub regex: Regex,
    pub logical_compare: LogicalCompare,
}

impl RegexFilter {
    pub fn filter_native_vertex<V: Vertex>(&self, v: &V) -> bool {
        let prop_id = self.logical_compare.get_prop_id();
        if prop_id > 0 {
            if let Some(val) = get_vertex_string_prop_value(prop_id, v) {
                return self.regex.is_match(&val);
            }
        }
        return false;
    }

    pub fn filter_native_edge<E: Edge>(&self, v: &E) -> bool {
        let prop_id = self.logical_compare.get_prop_id();
        if prop_id > 0 {
            if let Some(val) = get_edge_string_prop_value(prop_id, v) {
                return self.regex.is_match(&val);
            }
        }
        return false;
    }

    pub fn filter_message(&self, v: &RawMessage, schema: &Schema) -> bool {
        let prop_id = self.logical_compare.get_prop_id();
        if prop_id > 0 {
            if let Some(val) = get_message_string_prop_value(prop_id, v, schema) {
                return self.regex.is_match(&val);
            }
        }
        return false;
    }
}
