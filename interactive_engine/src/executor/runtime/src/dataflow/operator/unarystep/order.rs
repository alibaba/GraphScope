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

use dataflow::operator::shuffle::{StreamShuffleType, StreamShuffleKeyType};
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::{RawMessage, RawMessageType, ValuePayload};
use dataflow::manager::order::OrderManager;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::common::iterator::IteratorList;

use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::proto::message::OrderComparatorList;
use maxgraph_common::util::hash::murmur_hash64;

use protobuf::parse_from_bytes;
use std::collections::HashMap;
use itertools::Itertools;

pub struct OrderOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    range_flag: bool,
    range_start: usize,
    range_end: usize,
    order_manager: OrderManager,
    comparators: OrderComparatorList,
    keyed_order_manager: HashMap<i64, OrderManager>,
    order_label_id: i32,
}

impl<F> OrderOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase) -> Self {
        let range_flag = base.has_range_limit();
        let (range_start, range_end) = {
            if range_flag {
                let range_limit = base.get_range_limit();
                let range_start = {
                    if range_limit.range_start < 0 {
                        0
                    } else {
                        range_limit.range_start as usize
                    }
                };
                let range_end = {
                    if range_limit.range_end < 0 {
                        1000000000
                    } else {
                        range_limit.range_end as usize
                    }
                };
                (range_start, range_end)
            } else {
                (0, 0)
            }
        };
        let payload = base.get_argument().get_payload();
        let comparators = parse_from_bytes::<OrderComparatorList>(payload).expect("parse comparator list");
        let order_manager = OrderManager::new(comparators.clone(), range_end, range_flag);
        let order_label_id = base.get_argument().get_int_value();

        OrderOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            range_flag,
            range_start,
            range_end,
            order_manager,
            comparators,
            keyed_order_manager: HashMap::new(),
            order_label_id,
        }
    }
}

impl<F> Operator for OrderOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for OrderOperator<F>
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

    fn execute<'a>(&mut self, data: Vec<RawMessage>, _collector: &mut Box<'a + MessageCollector>) {
        for message in data.into_iter() {
            let message = self.before_requirement.process_requirement(message);
            if let Some(key) = message.get_extend_key_payload() {
                let order_manager = self.keyed_order_manager.entry(murmur_hash64(key))
                    .or_insert(OrderManager::new(self.comparators.clone(), self.range_end, self.range_flag));
                let mut prop_flag = true;
                for prop_id in order_manager.get_order_property_id().iter() {
                    if message.get_property(*prop_id).is_none() {
                        prop_flag = false;
                        break;
                    }
                }
                if prop_flag {
                    order_manager.add_message(message);
                }
            } else {
                let mut prop_flag = true;
                for prop_id in self.order_manager.get_order_property_id().iter() {
                    if message.get_property(*prop_id).is_none() {
                        prop_flag = false;
                        break;
                    }
                }
                if prop_flag {
                    self.order_manager.add_message(message);
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.keyed_order_manager.is_empty() {
            let mut value_list = vec![];
            for (_, mut result_order) in self.keyed_order_manager.drain() {
                let result_list = {
                    if self.range_flag {
                        result_order.get_range_result_list(self.range_start)
                    } else {
                        result_order.get_result_list()
                    }
                };

                if self.order_label_id < 0 {
                    let mut order_result_list = Vec::with_capacity(result_list.len());
                    let mut order_index = 0;
                    for mut curr_result in result_list.into_iter() {
                        curr_result.add_label_entity(RawMessage::from_value(ValuePayload::Int(order_index)), self.order_label_id);
                        order_result_list.push(curr_result);
                        order_index = order_index + 1;
                    }
                    value_list.push(order_result_list.into_iter());
                } else {
                    value_list.push(result_list.into_iter());
                }
            }
            let after_req = self.after_requirement.clone();
            return Box::new(IteratorList::new(value_list).map(move |v| after_req.process_requirement(v)));
        } else {
            let result_list = {
                if self.range_flag {
                    self.order_manager.get_range_result_list(self.range_start)
                } else {
                    self.order_manager.get_result_list()
                }
            };
            if self.order_label_id < 0 {
                let mut order_index = 0;
                let mut order_result_list = Vec::with_capacity(result_list.len());
                for mut m in result_list.into_iter() {
                    m.add_label_entity(RawMessage::from_value(ValuePayload::Int(order_index)), self.order_label_id);
                    order_index += 1;
                    order_result_list.push(self.after_requirement.process_requirement(m));
                }
                return Box::new(order_result_list.into_iter());
            } else {
                let after_req = self.after_requirement.clone();
                return Box::new(result_list.into_iter().map(move |v| {
                    after_req.process_requirement(v)
                }));
            }
        }
    }
}

pub struct OrderLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    order_manager: OrderManager,
}

impl<F> OrderLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let payload = base.get_argument().get_payload();
        let comparators = parse_from_bytes::<OrderComparatorList>(payload).expect("parse comparator list");
        let order_manager = OrderManager::new(comparators, 0, false);

        OrderLocalOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            order_manager,
        }
    }
}

impl<F> Operator for OrderLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for OrderLocalOperator<F>
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

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        let mut result_list = Vec::with_capacity(data.len());
        for message in data.into_iter() {
            let mut message = self.before_requirement.process_requirement(message);
            match message.get_message_type() {
                RawMessageType::LIST => {
                    if let Some(value) = message.take_value() {
                        if let Ok(list) = value.take_list() {
                            for val in list.into_iter() {
                                self.order_manager.add_message(val);
                            }
                            let order_list = self.order_manager.get_result_list();
                            let mut result = RawMessage::from_value_type(ValuePayload::List(order_list), RawMessageType::LIST);
                            self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                            result_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                }
                RawMessageType::MAP => {
                    if let Some(value) = message.take_value() {
                        if let Ok(list) = value.take_map() {
                            for val in list.into_iter() {
                                self.order_manager.add_message(RawMessage::from_entry_entity(val));
                            }
                            let order_list = self.order_manager
                                .get_result_list()
                                .into_iter()
                                .map(move |mut v| {
                                    v.take_value().unwrap().take_entry().unwrap()
                                }).collect_vec();
                            let mut result = RawMessage::from_value_type(ValuePayload::Map(order_list), RawMessageType::MAP);
                            self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                            result_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                }
                _ => {
                    result_list.push(self.after_requirement.process_requirement(message));
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}
