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

use dataflow::builder::{Operator, SourceOperator};
use dataflow::message::{RawMessage, ValuePayload, RawMessageType};
use maxgraph_common::proto::message::DfsCommand;
use protobuf::Message;

// dfs source  operator
pub struct DfsSourceOperator {
    id: i32,
    batch_size: i64,
}

impl DfsSourceOperator {
    pub fn new(id: i32, batch_size: i64) -> Self {
        DfsSourceOperator {
            id,
            batch_size,
        }
    }
}

impl Operator for DfsSourceOperator {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl SourceOperator for DfsSourceOperator {
    fn execute(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        let mut dfs_command = DfsCommand::new();
        dfs_command.set_batch_size(self.batch_size);
        dfs_command.set_send_count(0);
        let dfs_message = RawMessage::from_value_type(ValuePayload::Bytes(dfs_command.write_to_bytes().unwrap()),
                                                      RawMessageType::DFSCMD);

        return Box::new(Some(dfs_message).into_iter());
    }
}
