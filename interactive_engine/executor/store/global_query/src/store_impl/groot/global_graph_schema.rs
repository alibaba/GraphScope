//
//! Copyright 2020-2022 Alibaba Group Holding Limited.
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
//!

use std::collections::HashMap;

use maxgraph_store::api::DataType;
use maxgraph_store::db::api::{GraphDef, ValueType};

use crate::apis::graph_schema::Schema;

pub struct GlobalGraphSchema {
    graph_def: GraphDef,
    id_to_prop_name: HashMap<u32, String>,
}

impl GlobalGraphSchema {
    pub fn new(graph_def: GraphDef) -> Self {
        let mut id_to_prop_name = HashMap::new();
        for (name, id) in &graph_def.property_name_to_id {
            id_to_prop_name.insert(*id as u32, name.clone());
        }
        GlobalGraphSchema { graph_def, id_to_prop_name }
    }
}

impl Schema for GlobalGraphSchema {
    fn get_prop_id(&self, name: &str) -> Option<u32> {
        Some(*self.graph_def.property_name_to_id.get(name)? as u32)
    }

    fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<DataType> {
        let type_def = self
            .graph_def
            .label_to_types
            .get(&(label as i32))?;
        let prop_def = type_def.get_prop_def(prop_id as i32)?;
        match prop_def.r#type {
            ValueType::Bool => Some(DataType::Bool),
            ValueType::Char => Some(DataType::Char),
            ValueType::Short => Some(DataType::Short),
            ValueType::Int => Some(DataType::Int),
            ValueType::Long => Some(DataType::Long),
            ValueType::Float => Some(DataType::Float),
            ValueType::Double => Some(DataType::Double),
            ValueType::String => Some(DataType::String),
            ValueType::Bytes => Some(DataType::Bytes),
            ValueType::IntList => Some(DataType::ListInt),
            ValueType::LongList => Some(DataType::ListLong),
            ValueType::FloatList => Some(DataType::ListFloat),
            ValueType::DoubleList => Some(DataType::ListDouble),
            ValueType::StringList => Some(DataType::ListString),
        }
    }

    fn get_prop_name(&self, prop_id: u32) -> Option<String> {
        Some(self.id_to_prop_name.get(&prop_id)?.to_string())
    }

    fn get_label_id(&self, _name: &str) -> Option<u32> {
        unimplemented!()
    }

    fn get_label_name(&self, label: u32) -> Option<String> {
        let type_def = self
            .graph_def
            .label_to_types
            .get(&(label as i32))?;
        Some(type_def.get_label())
    }

    fn to_proto(&self) -> Vec<u8> {
        unimplemented!()
    }
}
