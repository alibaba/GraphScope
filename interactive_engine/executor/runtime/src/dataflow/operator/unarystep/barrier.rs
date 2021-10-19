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

use maxgraph_common::proto::query_flow::{OperatorBase, OperatorType, InputShuffleType};

use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::message::{RawMessage,RawBaseDataEntity,BulkExtraEntity};
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::manager::requirement::RequirementManager;
use std::collections::HashMap;

pub struct BarrierOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    barrier_size: i32,
    message_list: HashMap<RawBaseDataEntity, BulkExtraEntity>,
}

impl<F> BarrierOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               barrier_size: i32) -> Self {
        BarrierOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            barrier_size,
            message_list: HashMap::new(),
        }
    }
}

impl<F> Operator for BarrierOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for BarrierOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, mut data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        for mut message in data.drain(..) {
            let base_message = RawBaseDataEntity::from_message(&mut message);
            let val_list = self.message_list.entry(base_message).or_insert(BulkExtraEntity::new());
            val_list.add_path_entity(&mut message);
            val_list.increase_bulk_value(message.get_bulk());
        }
        if self.message_list.len() >= (self.barrier_size as usize) {
            let mut result_list = Vec::with_capacity(self.message_list.len());
            for (mut key, mut val) in self.message_list.drain() {
                let mut result = RawMessage::from_raw_base_data_entity(&mut key);
                if let Some(path_bulk_list) = val.take_path_bulk_list() {
                    result.set_extend_path_list(path_bulk_list);
                }
                result.set_bulk_value(val.get_bulk_value());
                result_list.push(result);
            }
            collector.collect_iterator(Box::new(result_list.into_iter()));
            self.message_list.clear();
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.message_list.is_empty() {
            let mut result_list = Vec::with_capacity(self.message_list.len());
            for (mut key, mut val) in self.message_list.drain() {
                let mut result = RawMessage::from_raw_base_data_entity(&mut key);
                if let Some(path_bulk_list) = val.take_path_bulk_list() {
                    result.set_extend_path_list(path_bulk_list);
                }
                result.set_bulk_value(val.get_bulk_value());
                result_list.push(result);
            }
            return Box::new(result_list.into_iter());
        }
        return Box::new(None.into_iter());
    }
}

