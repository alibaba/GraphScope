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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use maxgraph_store::api::{LabelId, PropId};
use maxgraph_store::schema::prelude::{TypeDef, PropDef, DataType};
use maxgraph_store::schema::Schema as LocalSchema;
use maxgraph_store::api::graph_schema::Schema;
use maxgraph_common::proto::schema::SchemaProto;

pub struct LocalGraphSchema {
    local_schema: Arc<dyn LocalSchema>,
}

impl LocalGraphSchema {
    pub fn new(local_schema: Arc<dyn LocalSchema>) -> Self {
        LocalGraphSchema {
            local_schema,
        }
    }
}

impl Schema for LocalGraphSchema {
    fn get_prop_id(&self, name: &'_ str) -> Option<u32> {
        self.local_schema.get_prop_id(name)
    }

    fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<DataType> {
        self.local_schema.get_prop_type(label, prop_id).cloned()
    }

    fn get_prop_name(&self, prop_id: u32) -> Option<String> {
        if let Some(prop_name) = self.local_schema.get_prop_name(prop_id) {
            Some(prop_name.to_string())
        } else {
            None
        }
    }

    fn get_label_id(&self, name: &'_ str) -> Option<u32> {
        self.local_schema.get_label_id(name)
    }

    fn get_label_name(&self, label: u32) -> Option<String> {
        if let Some(label_name) = self.local_schema.get_label_name(label) {
            Some(label_name.to_string())
        } else {
            None
        }
    }

    fn to_proto(&self) -> Vec<u8> {
        self.local_schema.to_proto()
    }
}

// TODO: impl RemoteGraphSchema
