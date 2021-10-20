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

use dataflow::operator::shuffle::*;
use dataflow::operator::unarystep::limitstop::GlobalStopFlagOperator;
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::{RawMessage, RawMessageType, ValuePayload};
use dataflow::manager::range::RangeManager;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};

use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::util::hash::murmur_hash64;

use std::collections::{HashSet, HashMap};
use utils::{PROP_ID, PROP_ID_LABEL, PROP_KEY, PROP_VALUE};
use protobuf::Message;
use std::cmp::min;
use std::sync::Arc;
use execution::build_empty_router;

pub struct DedupOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync  {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    prop_id: i32,
    dedup_key_list: HashMap<i64, HashSet<i64>>,
    dedup_id_list: HashSet<i64>,
}

impl<F> DedupOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase,
               prop_id: i32) -> Self {
        DedupOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            prop_id,
            dedup_key_list: HashMap::new(),
            dedup_id_list: HashSet::new(),
        }
    }
}

impl<F> Operator for DedupOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

#[inline]
fn get_prop_dedup_id(message: &RawMessage, prop_id: i32) -> i64 {
    if prop_id == 0 || prop_id == PROP_ID {
        return message.get_shuffle_id();
    } else if prop_id == PROP_ID_LABEL {
        return message.get_label_id() as i64;
    } else if prop_id == PROP_KEY {
        if let Some(entry) = message.get_entry_value() {
            return entry.get_key().get_id();
        }
    } else if prop_id == PROP_VALUE {
        if let Some(entry) = message.get_entry_value() {
            return entry.get_value().get_id();
        }
    } else if prop_id > 0 {
        if let Some(prop) = message.get_property(prop_id) {
            let empty_fn = Arc::new(build_empty_router());
            return murmur_hash64(&prop.get_value().to_proto(Some(empty_fn.as_ref())).write_to_bytes().unwrap());
        }
    } else {
        if let Some(label_list) = message.get_label_entity_by_id(prop_id) {
            return label_list.get_message().get_shuffle_id();
        }
    }
    return 0;
}

impl<F> UnaryOperator for DedupOperator<F>
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
            if let Some(key) = message.get_extend_key_payload() {
                let keyid = murmur_hash64(key);
                let dedup_id_list = self.dedup_key_list.entry(keyid).or_insert(HashSet::new());
                let dedup_id = get_prop_dedup_id(&message, self.prop_id);
                if !dedup_id_list.contains(&dedup_id) {
                    dedup_id_list.insert(dedup_id);
                    message.update_with_bulk(1);
                    result_list.push(self.after_requirement.process_requirement(message));
                }
            } else {
                let dedup_id = get_prop_dedup_id(&message, self.prop_id);
                if !self.dedup_id_list.contains(&dedup_id) {
                    self.dedup_id_list.insert(dedup_id);
                    message.update_with_bulk(1);
                    result_list.push(self.after_requirement.process_requirement(message));
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct RangeOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleCompositeType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    start: usize,
    end: usize,
    range_manage: RangeManager,
    keyed_range_list: HashMap<i64, RangeManager>,
    global_filter_flag: bool,
}

impl<F> RangeOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleCompositeType<F>,
               base: &OperatorBase) -> Self {
        let range_args = base.get_argument().get_long_value_list().to_vec();
        let low = range_args[0] as usize;
        let high = range_args[1] as usize;
        RangeOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            start: low,
            end: high,
            range_manage: RangeManager::new(low, high, false),
            keyed_range_list: HashMap::new(),
            global_filter_flag: false,
        }
    }
}

impl<F> Operator for RangeOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for RangeOperator<F>
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
            let bulk = message.get_bulk() as usize;
            if let Some(key) = message.get_extend_key_payload() {
                let keyid = murmur_hash64(key);
                let range_manager = self.keyed_range_list.entry(keyid)
                    .or_insert(RangeManager::new(self.start, self.end, false));
                if range_manager.range_finish() || range_manager.range_filter_with_bulk(bulk) {
                    continue;
                }
                let val = range_manager.check_range_with_bulk(bulk);
                message.update_with_bulk(val as i64);
            } else {
                self.global_filter_flag = self.range_manage.range_finish();
                if self.global_filter_flag || self.range_manage.range_filter_with_bulk(bulk) {
                    continue;
                }
                let val = self.range_manage.check_range_with_bulk(bulk);
                message.update_with_bulk(val as i64);
            }
            if message.get_bulk() > 0 {
                result_list.push(self.after_requirement.process_requirement(message));
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

impl<F> GlobalStopFlagOperator for RangeOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync{
    fn generate_stop_flag(&mut self) -> bool {
        if self.global_filter_flag {
            self.global_filter_flag = false;
            return true;
        }

        return false;
    }
}

pub struct RangeLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    start: usize,
    end: usize,
}

impl<F> RangeLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let range_args = base.get_argument().get_long_value_list().to_vec();
        let low = range_args[0] as usize;
        let high = range_args[1] as usize;
        RangeLocalOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            start: low,
            end: high,
        }
    }
}

impl<F> Operator for RangeLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for RangeLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync  {
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
                        if let Ok(mut list) = value.take_list() {
                            let low = min(self.start, list.len());
                            let high = min(self.end, list.len());
                            message.set_value(ValuePayload::List(list.drain(low..high).collect()));
                            result_list.push(self.after_requirement.process_requirement(message));
                        }
                    }
                }
                RawMessageType::MAP => {
                    if let Some(value) = message.take_value() {
                        if let Ok(mut list) = value.take_map() {
                            let low = min(self.start, list.len());
                            let high = min(self.end, list.len());
                            message.set_value(ValuePayload::Map(list.drain(low..high).collect()));
                            result_list.push(self.after_requirement.process_requirement(message));
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
        Box::new(None.into_iter())
    }
}
