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

use dataflow::message::RawMessage;
use dataflow::builder::{Operator, SourceOperator};

pub struct SourceChainOperator {
    id: i32,
    iter: Option<Box<dyn Iterator<Item=RawMessage> + Send>>,
}

impl SourceChainOperator {
    pub fn new(id: i32, iter: Option<Box<dyn Iterator<Item=RawMessage> + Send>>) -> Self {
        SourceChainOperator {
            id,
            iter,
        }
    }
}

unsafe impl Send for SourceChainOperator {}

impl Operator for SourceChainOperator {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl SourceOperator for SourceChainOperator {
    fn execute(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        self.iter.take().unwrap()
    }
}
