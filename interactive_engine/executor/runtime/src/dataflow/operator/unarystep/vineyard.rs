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

use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::message::{RawMessage, ValuePayload};
use dataflow::operator::shuffle::StreamShuffleType;
use store::graph_builder_ffi::VineyardGraphStream;

pub struct VineyardStreamOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    shuffle_type: StreamShuffleType<F>,
    stream_index: i32,
    graph_name: String,
    graph_instance_list: Vec<(i64, u64)>,
}

impl<F> VineyardStreamOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(id: i32,
               input_id: i32,
               shuffle_type: StreamShuffleType<F>,
               stream_index: i32,
               graph_name: String) -> Self {
        VineyardStreamOperator {
            id,
            input_id,
            shuffle_type,
            stream_index,
            graph_name,
            graph_instance_list: Vec::new(),
        }
    }
}

impl<F> Operator for VineyardStreamOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for VineyardStreamOperator<F>
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

    fn execute<'a>(&mut self, message: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        for mut m in message.into_iter() {
            if let Some(value) = m.take_value() {
                if let Ok(long_list) = value.take_list_long() {
                    if long_list.len() == 2 {
                        let graph_id = *long_list.get(0).unwrap();
                        let instance_id = *long_list.get(1).unwrap() as u64;
                        self.graph_instance_list.push((graph_id, instance_id));
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        if !self.graph_instance_list.is_empty() {
            let vineyard_stream = VineyardGraphStream::new();
            info!("Build vineyard graph {:?} stream with {:?}",
                  &self.graph_name,
                  &self.graph_instance_list);
            let global_id = vineyard_stream.build_global_stream(self.graph_name.clone(),
                                                                &self.graph_instance_list);
            let result = RawMessage::from_value(ValuePayload::Long(global_id));
            return Box::new(Some(result).into_iter());
        } else {
            return Box::new(None.into_iter());
        }
    }
}
