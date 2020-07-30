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
use dataflow::message::{RawMessage, ValuePayload};

pub struct SourceCountOperator {
    id: i32,
    count: i64,
}

impl SourceCountOperator {
    pub fn new(id: i32,
               count: i64) -> Self {
        SourceCountOperator {
            id,
            count,
        }
    }
}

impl Operator for SourceCountOperator {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl SourceOperator for SourceCountOperator {
    fn execute(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        return Box::new(Some(RawMessage::from_value(ValuePayload::Long(self.count))).into_iter());
    }
}
