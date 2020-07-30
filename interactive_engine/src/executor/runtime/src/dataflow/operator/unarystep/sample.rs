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

extern crate rand;

use maxgraph_common::proto::query_flow::OperatorBase;

use dataflow::operator::shuffle::StreamShuffleKeyType;
use dataflow::message::RawMessage;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::manager::requirement::RequirementManager;

use rand::Rng;
use rand::distributions::WeightedIndex;
use rand::distributions::Distribution;
use std::collections::HashMap;

pub struct MetapathSampleOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    amount_to_sample: i32,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    message_vec: Vec<RawMessage>,
    keyed_message_vec: HashMap<Vec<u8>, Vec<RawMessage>>,
}

impl<F> MetapathSampleOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               _amount_to_sample: i32) -> Self {
        MetapathSampleOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            amount_to_sample: 1,
            message_vec: Vec::new(),
            keyed_message_vec: HashMap::new(),
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for MetapathSampleOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for MetapathSampleOperator<F>
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

    fn execute<'a>(&mut self, mut data: Vec<RawMessage>, _collector: &mut Box<'a + MessageCollector>) {
        for message in data.drain(..) {
            let mut message = self.before_requirement.process_requirement(message);
            if let Some(key) = message.take_extend_key_payload() {
                let val_list = self.keyed_message_vec.entry(key).or_insert(vec![]);
                val_list.push(message);
            } else {
                self.message_vec.push(message);
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.keyed_message_vec.is_empty() {
            let mut result_list = Vec::with_capacity(self.keyed_message_vec.len());
            for (key, mut val) in self.keyed_message_vec.drain() {
                if !val.is_empty() {
                    let mut rng = rand::thread_rng();
                    let weight = WeightedIndex::new(val.iter().map(|item|item.get_bulk())).unwrap();
                    let secret_number = weight.sample(&mut rng);
                    let mut result_message = val.remove(secret_number);
                    result_message.set_extend_key_payload(key);
                    result_message.update_with_bulk(1);
                    result_list.push(self.after_requirement.process_requirement(result_message));
                }
            }
            return Box::new(result_list.into_iter());
        } else if !self.message_vec.is_empty() {
            let mut rng = rand::thread_rng();
            let weight = WeightedIndex::new(self.message_vec.iter().map(|item|item.get_bulk())).unwrap();
            let secret_number = weight.sample(&mut rng);
            let mut result_message = self.message_vec.remove(secret_number);
            result_message.update_with_bulk(1);
            return Box::new(Some(self.after_requirement.process_requirement(result_message)).into_iter());
        }

        return Box::new(None.into_iter());
    }
}
