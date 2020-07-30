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

use std::sync::Arc;
use dataflow::builder::{SourceOperator, Operator};
use dataflow::message::{RawMessage, ValuePayload};
use store::graph_builder_ffi::VineyardGraphBuilder;
use maxgraph_common::proto::query_flow::RuntimeGraphSchemaProto;

pub struct VineyardBuilderOperator {
    id: i32,
    graph_name: String,
    schema: RuntimeGraphSchemaProto,
    worker_index: i32,
}

impl VineyardBuilderOperator {
    pub fn new(id: i32,
               graph_name: String,
               schema: RuntimeGraphSchemaProto,
               worker_index: i32) -> Self {
        VineyardBuilderOperator {
            id,
            graph_name,
            schema,
            worker_index,
        }
    }
}

impl Operator for VineyardBuilderOperator {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl SourceOperator for VineyardBuilderOperator {
    fn execute(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        let vineyard_builder = VineyardGraphBuilder::new(self.graph_name.clone(),
                                                         &self.schema,
                                                         self.worker_index);
        let (graph_id, instance_id) = vineyard_builder.get_graph_instance_id();
        info!("vineyard graph id {:?} instance id {:?} in builder operator", graph_id, instance_id);
        let message = RawMessage::from_value(ValuePayload::ListLong(vec![graph_id, instance_id as i64]));
        return Box::new(Some(message).into_iter());
    }
}
