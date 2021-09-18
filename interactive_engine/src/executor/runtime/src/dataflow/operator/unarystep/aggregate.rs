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
use dataflow::operator::shuffle::*;
use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraEntryEntity, ExtraExtendEntity};
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::primitive::Write;

use maxgraph_common::proto::query_flow::{OperatorBase, CountArgumentProto};
use maxgraph_common::proto::message::VariantType;

use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use protobuf::parse_from_bytes;

pub struct CountGlobalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    total_count: Option<i64>,
    after_requirement: RequirementManager,
    count_argument: CountArgumentProto,
}

impl<F> CountGlobalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(id: i32,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let count_argument = parse_from_bytes::<CountArgumentProto>(base.get_argument().get_payload()).expect("parse count argument");

        CountGlobalOperator {
            id,
            input_id,
            stream_index,
            shuffle_type,
            total_count: None,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            count_argument,
        }
    }
}

impl<F> Operator for CountGlobalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for CountGlobalOperator<F>
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
        if self.count_argument.get_limit_flag() && self.total_count >= Some(self.count_argument.get_limit_count()) {
            return;
        }

        let bulk_sum: i64 = data.iter().map(|message|message.get_bulk()).sum();
        let current_value = self.total_count.map_or(bulk_sum, |value| value + bulk_sum);
        if self.count_argument.get_limit_flag() &&
            current_value > self.count_argument.get_limit_count() {
            self.total_count.replace(self.count_argument.get_limit_count());
        } else {
            self.total_count.replace(current_value);
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if let Some(total_count) = self.total_count {
            let result = RawMessage::from_value(ValuePayload::Long(total_count));
            Box::new(Some(self.after_requirement.process_requirement(result)).into_iter())
        } else {
            debug!("There is None input is proceed....");
            Box::new(None.into_iter())
        }
    }
}

pub struct GroupCountOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    after_requirement: RequirementManager,
    count_list: HashMap<RawMessage, i64>,
}

impl<F> GroupCountOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        GroupCountOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            count_list: HashMap::new(),
        }
    }
}

impl<F> Operator for GroupCountOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for GroupCountOperator<F>
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
            let count = self.count_list.entry(message.build_message_value()).or_insert(0 as i64);
            *count += message.get_bulk();
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        let mut result_list = Vec::with_capacity(self.count_list.len());
        for (key, val) in self.count_list.drain() {
            result_list.push(self.after_requirement.process_requirement(RawMessage::from_entry(key, RawMessage::from_value(ValuePayload::Long(val)))));
        }
        return Box::new(result_list.into_iter());
    }
}

pub struct CountByKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    after_requirement: RequirementManager,
    count_list: HashMap<Vec<u8>, i64>,
    count_extend_list: HashMap<Vec<u8>, ExtraExtendEntity>,
}

impl<F> CountByKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase) -> Self {
        CountByKeyOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            count_list: HashMap::new(),
            count_extend_list: HashMap::new(),
        }
    }
}

impl<F> Operator for CountByKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for CountByKeyOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(key) = message.take_extend_key_payload() {
                if !self.count_extend_list.contains_key(&key) {
                    if let Some(extend_val) = message.take_extend_entity() {
                        self.count_extend_list.insert(key.clone(), extend_val);
                    }
                }
                let count = self.count_list.entry(key).or_insert(0 as i64);
                *count += message.get_bulk();
            } else {
                error!("cant get key from message {:?}", &message);
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        let mut result_list = Vec::with_capacity(self.count_list.len());
        for (key, val) in self.count_list.drain() {
            let mut result = RawMessage::from_value(ValuePayload::Long(val));
            if let Some(extend_val) = self.count_extend_list.remove(&key) {
                result.set_extend_entity(extend_val);
            }
            result.set_extend_key_payload(key);
            result_list.push(self.after_requirement.process_requirement(result));
        }
        return Box::new(result_list.into_iter());
    }
}

pub struct FoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    after_requirement: RequirementManager,
    keyed_fold_list: HashMap<Vec<u8>, Vec<RawMessage>>,
    fold_list: Option<Vec<RawMessage>>,
}

impl<F> FoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase) -> Self {
        FoldOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            keyed_fold_list: HashMap::new(),
            fold_list: Some(Vec::new()),
        }
    }
}

impl<F> Operator for FoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for FoldOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(key) = message.take_extend_key_payload() {
                message.clean_extend();
                let fold_list = self.keyed_fold_list.entry(key).or_insert(Vec::new());
                let bulk = message.get_bulk();
                message.set_bulk_value(1);
                for _i in 0..bulk {
                    fold_list.push(message.clone());
                }
            } else {
                message.clean_extend();
                let bulk = message.get_bulk();
                message.set_bulk_value(1);
                let fold_list = self.fold_list.as_mut().unwrap();
                for _i in 0..bulk {
                    fold_list.push(message.clone());
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.fold_list.as_ref().unwrap().is_empty() {
            let result = RawMessage::from_value_type(ValuePayload::List(self.fold_list.take().unwrap()),
                                                     RawMessageType::LIST);
            return Box::new(Some(self.after_requirement.process_requirement(result)).into_iter());
        } else if !self.keyed_fold_list.is_empty() {
            let mut result_list = Vec::with_capacity(self.keyed_fold_list.len());
            for (key, value) in self.keyed_fold_list.drain() {
                let mut result = RawMessage::from_value_type(ValuePayload::List(value),
                                                             RawMessageType::LIST);
                result.set_extend_key_payload(key);
                result_list.push(self.after_requirement.process_requirement(result));
            }
            return Box::new(result_list.into_iter());
        }

        return Box::new(None.into_iter());
    }
}

pub struct FoldMapOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    after_requirement: RequirementManager,
    keyed_fold_list: HashMap<Vec<u8>, Vec<ExtraEntryEntity>>,
    fold_list: Option<Vec<ExtraEntryEntity>>,
}

impl<F> FoldMapOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase) -> Self {
        FoldMapOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            keyed_fold_list: HashMap::new(),
            fold_list: Some(Vec::new()),
        }
    }
}

impl<F> Operator for FoldMapOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for FoldMapOperator<F>
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
        for mut message in data.into_iter() {
            match message.get_message_type() {
                RawMessageType::ENTRY => {
                    if let Some(key) = message.take_extend_key_payload() {
                        message.clean_extend();
                        if let Some(value) = message.take_value() {
                            if let Ok(entry) = value.take_entry() {
                                let fold_list = self.keyed_fold_list.entry(key).or_insert(Vec::new());
                                fold_list.push(entry);
                            }
                        }
                    } else {
                        message.clean_extend();
                        if let Some(value) = message.take_value() {
                            if let Ok(entry) = value.take_entry() {
                                self.fold_list.as_mut().unwrap().push(entry);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.fold_list.as_ref().unwrap().is_empty() {
            let result = RawMessage::from_value_type(ValuePayload::Map(self.fold_list.take().unwrap()),
                                                     RawMessageType::MAP);
            return Box::new(Some(self.after_requirement.process_requirement(result)).into_iter());
        } else if !self.keyed_fold_list.is_empty() {
            let mut result_list = Vec::with_capacity(self.keyed_fold_list.len());
            for (key, value) in self.keyed_fold_list.drain() {
                let mut result = RawMessage::from_value_type(ValuePayload::Map(value),
                                                             RawMessageType::MAP);
                result.set_extend_key_payload(key);
                result_list.push(self.after_requirement.process_requirement(result));
            }
            return Box::new(result_list.into_iter());
        }

        return Box::new(None.into_iter());
    }
}

pub struct FoldStoreOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    fold_int_list: HashSet<i32>,
    fold_long_list: HashSet<i64>,
    fold_string_list: HashSet<String>,
}

impl<F> FoldStoreOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>,
               base: &OperatorBase) -> Self {
        FoldStoreOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            fold_int_list: HashSet::new(),
            fold_long_list: HashSet::new(),
            fold_string_list: HashSet::new(),
        }
    }
}

impl<F> Operator for FoldStoreOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for FoldStoreOperator<F>
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
        for mut message in data.into_iter() {
            match message.get_message_type() {
                RawMessageType::VERTEX | RawMessageType::EDGE => {
                    self.fold_long_list.insert(message.get_id());
                }
                RawMessageType::VALUE => {
                    if let Some(value) = message.take_value() {
                        if let Ok(intval) = value.get_int() {
                            self.fold_int_list.insert(intval);
                        } else if let Ok(longval) = value.get_long() {
                            self.fold_long_list.insert(longval);
                        } else if let Ok(strval) = value.get_string() {
                            self.fold_string_list.insert(strval.to_owned());
                        } else {
                            error!("Not support value for fold {:?}", &value);
                        }
                    }
                }
                _ => {
                    error!("not support value {:?} in fold store", &message);
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if !self.fold_int_list.is_empty() {
            let mut value_list = Vec::with_capacity(self.fold_int_list.len());
            for value in self.fold_int_list.drain() {
                value_list.push(value);
            }
            let result = RawMessage::from_value(ValuePayload::ListInt(value_list));
            return Box::new(Some(result).into_iter());
        } else if !self.fold_long_list.is_empty() {
            let mut value_list = Vec::with_capacity(self.fold_long_list.len());
            for value in self.fold_long_list.drain() {
                value_list.push(value);
            }
            let result = RawMessage::from_value(ValuePayload::ListLong(value_list));
            return Box::new(Some(result).into_iter());
        } else if !self.fold_string_list.is_empty() {
            let mut value_list = Vec::with_capacity(self.fold_string_list.len());
            for value in self.fold_string_list.drain() {
                value_list.push(value);
            }
            let result = RawMessage::from_value(ValuePayload::ListString(value_list));
            return Box::new(Some(result).into_iter());
        }

        return Box::new(None.into_iter());
    }
}

struct AggregateValue {
    val_int: i32,
    val_long: i64,
    val_float: f32,
    val_double: f64,
    val_str: String,
}

pub struct SumOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleCompositeType<F>,
    value_type: VariantType,
    agg_value: Option<AggregateValue>,
    keyed_agg_value: HashMap<Vec<u8>, AggregateValue>,
    after_requirement: RequirementManager,
}

impl<F> SumOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleCompositeType<F>) -> Self {
        let value_type = base.get_argument().get_value_type().clone();
        SumOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            value_type,
            agg_value: None,
            keyed_agg_value: HashMap::new(),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for SumOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for SumOperator<F>
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
        for mut message in data.into_iter() {
            let curr_agg_value = {
                if let Some(extra_key) = message.take_extend_key_payload() {
                    self.keyed_agg_value.entry(extra_key).or_insert(AggregateValue {
                        val_int: 0,
                        val_long: 0,
                        val_float: 0.0,
                        val_double: 0.0,
                        val_str: "".to_string(),
                    })
                } else {
                    if let Some(curr_agg_value) = self.agg_value.as_mut() {
                        curr_agg_value
                    } else {
                        let new_agg_value = AggregateValue {
                            val_int: 0,
                            val_long: 0,
                            val_float: 0.0,
                            val_double: 0.0,
                            val_str: "".to_string(),
                        };
                        self.agg_value = Some(new_agg_value);
                        self.agg_value.as_mut().unwrap()
                    }
                }
            };
            if let Some(value) = message.get_value() {
                let bulk = message.get_bulk();
                match self.value_type {
                    VariantType::VT_INT | VariantType::VT_LONG => {
                        if let Ok(val) = value.get_long() {
                            curr_agg_value.val_long += val * bulk;
                        }
                    }
                    VariantType::VT_FLOAT | VariantType::VT_DOUBLE => {
                        if let Ok(val) = value.get_double() {
                            curr_agg_value.val_double += val * (bulk as f64);
                        }
                    }
                    _ => {
                        error!("Not support sum for type {:?}", &self.value_type);
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        let output_value_type = {
            match self.value_type {
                VariantType::VT_INT | VariantType::VT_LONG => VariantType::VT_LONG,
                _ => VariantType::VT_DOUBLE,
            }
        };
        if self.keyed_agg_value.is_empty() {
            if let Some(curr_agg_value) = self.agg_value.as_mut() {
                if let Some(result) = build_aggregate_value(output_value_type, curr_agg_value) {
                    return Box::new(Some(self.after_requirement.process_requirement(result)).into_iter());
                }
            }
        } else {
            let mut result_list = Vec::with_capacity(self.keyed_agg_value.len());
            for (key, mut curr_agg_value) in self.keyed_agg_value.drain() {
                if let Some(mut result) = build_aggregate_value(output_value_type, &mut curr_agg_value) {
                    result.set_extend_key_payload(key);
                    result_list.push(self.after_requirement.process_requirement(result));
                }
            }
            return Box::new(result_list.into_iter());
        }

        return Box::new(None.into_iter());
    }
}

pub struct MaxOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    value_type: VariantType,
    agg_value: Option<AggregateValue>,
    keyed_agg_value: HashMap<Vec<u8>, AggregateValue>,
    after_requirement: RequirementManager,
}

impl<F> MaxOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>) -> Self {
        let value_type = base.get_argument().get_value_type().clone();
        MaxOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            value_type,
            agg_value: None,
            keyed_agg_value: HashMap::new(),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for MaxOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for MaxOperator<F>
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
        for mut message in data.into_iter() {
            let curr_agg_value = {
                if let Some(extra_key) = message.take_extend_key_payload() {
                    self.keyed_agg_value.entry(extra_key).or_insert(build_aggregate_value_from_message(&message, self.value_type))
                } else {
                    if let Some(ref mut agg_value_ref) = self.agg_value {
                        agg_value_ref
                    } else {
                        self.agg_value = Some(build_aggregate_value_from_message(&message, self.value_type));
                        self.agg_value.as_mut().unwrap()
                    }
                }
            };
            if let Some(value) = message.get_value() {
                match self.value_type {
                    VariantType::VT_INT => {
                        if let Ok(val) = value.get_int() {
                            if val > curr_agg_value.val_int {
                                curr_agg_value.val_int = val;
                            }
                        }
                    }
                    VariantType::VT_LONG => {
                        if let Ok(val) = value.get_long() {
                            if val > curr_agg_value.val_long {
                                curr_agg_value.val_long = val;
                            }
                        }
                    }
                    VariantType::VT_FLOAT => {
                        if let Ok(val) = value.get_float() {
                            if val > curr_agg_value.val_float {
                                curr_agg_value.val_float = val;
                            }
                        }
                    }
                    VariantType::VT_DOUBLE => {
                        if let Ok(val) = value.get_double() {
                            if val > curr_agg_value.val_double {
                                curr_agg_value.val_double = val;
                            }
                        }
                    }
                    VariantType::VT_STRING => {
                        if let Ok(val) = value.get_string() {
                            if val.cmp(&curr_agg_value.val_str) == Ordering::Greater {
                                curr_agg_value.val_str = val.to_owned();
                            }
                        }
                    }
                    _ => {
                        error!("Not support sum for type {:?}", &self.value_type);
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if self.keyed_agg_value.is_empty() {
            if let Some(ref mut curr_agg_value) = self.agg_value {
                if let Some(result) = build_aggregate_value(self.value_type, curr_agg_value) {
                    return Box::new(Some(self.after_requirement.process_requirement(result)).into_iter());
                }
            }
        } else {
            let mut result_list = Vec::with_capacity(self.keyed_agg_value.len());
            for (key, mut curr_agg_value) in self.keyed_agg_value.drain() {
                if let Some(mut result) = build_aggregate_value(self.value_type, &mut curr_agg_value) {
                    result.set_extend_key_payload(key);
                    result_list.push(self.after_requirement.process_requirement(result));
                }
            }
            return Box::new(result_list.into_iter());
        }

        return Box::new(None.into_iter());
    }
}

pub struct MinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleKeyType<F>,
    value_type: VariantType,
    agg_value: Option<AggregateValue>,
    keyed_agg_value: HashMap<Vec<u8>, AggregateValue>,
    after_requirement: RequirementManager,
}

impl<F> MinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleKeyType<F>) -> Self {
        let value_type = base.get_argument().get_value_type().clone();
        MinOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            value_type,
            agg_value: None,
            keyed_agg_value: HashMap::new(),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for MinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for MinOperator<F>
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
        for mut message in data.into_iter() {
            let curr_agg_value = {
                if let Some(extra_key) = message.take_extend_key_payload() {
                    self.keyed_agg_value.entry(extra_key).or_insert(build_aggregate_value_from_message(&message, self.value_type))
                } else {
                    if let Some(ref mut agg_value_ref) = self.agg_value {
                        agg_value_ref
                    } else {
                        self.agg_value = Some(build_aggregate_value_from_message(&message, self.value_type));
                        self.agg_value.as_mut().unwrap()
                    }
                }
            };
            if let Some(value) = message.get_value() {
                match self.value_type {
                    VariantType::VT_INT => {
                        if let Ok(val) = value.get_int() {
                            if val < curr_agg_value.val_int {
                                curr_agg_value.val_int = val;
                            }
                        }
                    }
                    VariantType::VT_LONG => {
                        if let Ok(val) = value.get_long() {
                            if val < curr_agg_value.val_long {
                                curr_agg_value.val_long = val;
                            }
                        }
                    }
                    VariantType::VT_FLOAT => {
                        if let Ok(val) = value.get_float() {
                            if val < curr_agg_value.val_float {
                                curr_agg_value.val_float = val;
                            }
                        }
                    }
                    VariantType::VT_DOUBLE => {
                        if let Ok(val) = value.get_double() {
                            if val < curr_agg_value.val_double {
                                curr_agg_value.val_double = val;
                            }
                        }
                    }
                    VariantType::VT_STRING => {
                        if let Ok(val) = value.get_string() {
                            if val.cmp(&curr_agg_value.val_str) == Ordering::Less {
                                curr_agg_value.val_str = val.to_owned();
                            }
                        }
                    }
                    _ => {
                        error!("Not support sum for type {:?}", &self.value_type);
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if self.keyed_agg_value.is_empty() {
            if let Some(ref mut curr_agg_value) = self.agg_value {
                if let Some(result) = build_aggregate_value(self.value_type, curr_agg_value) {
                    return Box::new(Some(self.after_requirement.process_requirement(result)).into_iter());
                }
            }
        } else {
            let mut result_list = Vec::with_capacity(self.keyed_agg_value.len());
            for (key, mut curr_agg_value) in self.keyed_agg_value.drain() {
                if let Some(mut result) = build_aggregate_value(self.value_type, &mut curr_agg_value) {
                    result.set_extend_key_payload(key);
                    result_list.push(self.after_requirement.process_requirement(result));
                }
            }
            return Box::new(result_list.into_iter());
        }

        return Box::new(None.into_iter());
    }
}

fn build_aggregate_value_from_message(message: &RawMessage, value_type: VariantType) -> AggregateValue {
    if let Some(value) = message.get_value() {
        match value_type {
            VariantType::VT_INT => {
                if let Ok(val) = value.get_int() {
                    return AggregateValue {
                        val_int: val,
                        val_long: 0,
                        val_float: 0.0,
                        val_double: 0.0,
                        val_str: "".to_string(),
                    };
                }
            }
            VariantType::VT_LONG => {
                if let Ok(val) = value.get_long() {
                    return AggregateValue {
                        val_int: 0,
                        val_long: val,
                        val_float: 0.0,
                        val_double: 0.0,
                        val_str: "".to_string(),
                    };
                }
            }
            VariantType::VT_FLOAT => {
                if let Ok(val) = value.get_float() {
                    return AggregateValue {
                        val_int: 0,
                        val_long: 0,
                        val_float: val,
                        val_double: 0.0,
                        val_str: "".to_string(),
                    };
                }
            }
            VariantType::VT_DOUBLE => {
                if let Ok(val) = value.get_double() {
                    return AggregateValue {
                        val_int: 0,
                        val_long: 0,
                        val_float: 0.0,
                        val_double: val,
                        val_str: "".to_string(),
                    };
                }
            }
            VariantType::VT_STRING => {
                if let Ok(val) = value.get_string() {
                    return AggregateValue {
                        val_int: 0,
                        val_long: 0,
                        val_float: 0.0,
                        val_double: 0.0,
                        val_str: val.to_owned(),
                    };
                }
            }
            _ => {
                error!("Not support sum for type {:?}", &value_type);
            }
        }
    }

    return AggregateValue {
        val_int: 0,
        val_long: 0,
        val_float: 0.0,
        val_double: 0.0,
        val_str: "".to_owned(),
    };
}

fn build_aggregate_value(value_type: VariantType, agg_value: &mut AggregateValue) -> Option<RawMessage> {
    match value_type {
        VariantType::VT_INT => Some(RawMessage::from_value(ValuePayload::Int(agg_value.val_int))),
        VariantType::VT_LONG => Some(RawMessage::from_value(ValuePayload::Long(agg_value.val_long))),
        VariantType::VT_FLOAT => Some(RawMessage::from_value(ValuePayload::Float(agg_value.val_float.into_bytes()))),
        VariantType::VT_DOUBLE => Some(RawMessage::from_value(ValuePayload::Double(agg_value.val_double.into_bytes()))),
        VariantType::VT_STRING => Some(RawMessage::from_value(ValuePayload::String(agg_value.val_str.clone()))),
        _ => None
    }
}
