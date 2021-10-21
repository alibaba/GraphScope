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

use dataflow::builder::{ProgramOperator, Operator};
use maxgraph_common::proto::query_flow::OperatorType;

pub struct ProgramOperatorDelegate {
    id: i32,
    input_id: i32,
    stream_index: i32,
    operator_type: OperatorType,
    argument: Vec<u8>,
}

impl ProgramOperatorDelegate {
    pub fn new(id: i32,
               input_id: i32,
               stream_index: i32,
               operator_type: OperatorType,
               argument: Vec<u8>) -> Self {
        ProgramOperatorDelegate {
            id,
            input_id,
            stream_index,
            operator_type,
            argument,
        }
    }
}

impl Operator for ProgramOperatorDelegate {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl ProgramOperator for ProgramOperatorDelegate {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn get_program_type(&self) -> OperatorType {
        self.operator_type
    }

    fn get_program_argument(&self) -> &Vec<u8> {
        &self.argument
    }
}
