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
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::message::{RawMessage, ExtraEntryEntity, ValuePayload, RawMessageType};
use dataflow::manager::requirement::RequirementManager;

use maxgraph_common::proto::message::PopType;
use maxgraph_common::proto::query_flow::{OperatorBase, ColumnType};

use protobuf::ProtobufEnum;
use std::collections::HashMap;
use dataflow::operator::collector::MessageLocalCollector;

pub struct SelectOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    label_exist: bool,
    label_ids: Vec<i32>,
    label_name_list: Vec<String>,
    pop_type: PopType,
}

impl<F> SelectOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let label_exist = base.get_argument().get_bool_value();
        let label_ids = base.get_argument().get_int_value_list().to_vec();
        let label_name_list = base.get_argument().get_str_value_list().to_vec();
        let pop_type = PopType::from_i32(base.get_argument().get_int_value()).unwrap();
        SelectOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            label_exist,
            label_ids,
            label_name_list,
            pop_type,
        }
    }
}

impl<F> Operator for SelectOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for SelectOperator<F>
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
        if !self.label_exist {
            return;
        }

        let mut result_list = Vec::with_capacity(data.len());
        for message in data.into_iter() {
            let mut message = self.before_requirement.process_requirement(message);
            let mut map_value = Vec::with_capacity(self.label_ids.len());
            for i in 0..self.label_ids.len() {
                let label_id = self.label_ids[i];
                let mut map_value_flag = false;
                if let Some(curr_map_value) = message.get_map_value() {
                    for map_entry in curr_map_value.iter() {
                        if let Some(key_payload) = map_entry.get_key().get_value() {
                            if let Ok(key_str) = key_payload.get_string() {
                                if key_str.eq(self.label_name_list.get(i).unwrap()) {
                                    map_value_flag = true;
                                    map_value.push(map_entry.clone());
                                }
                            }
                        }
                    }
                }
                if !map_value_flag {
                    if let Some(label_list) = message.get_label_entity_list_by_id(label_id) {
                        let label_name = self.label_name_list.get(i).unwrap().to_owned();
                        if !label_list.is_empty() {
                            let entry = {
                                match self.pop_type {
                                    PopType::FIRST => {
                                        ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(label_name)),
                                            label_list.get(0).unwrap().get_message().clone())
                                    }
                                    PopType::LAST => {
                                        ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(label_name)),
                                            label_list.get(label_list.len() - 1).unwrap().get_message().clone())
                                    }
                                    PopType::ALL => {
                                        let mut val_list = Vec::new();
                                        for label_entity in label_list {
                                            val_list.push(label_entity.get_message().clone());
                                        }
                                        ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(label_name)),
                                            RawMessage::from_value_type(ValuePayload::List(val_list),
                                                                        RawMessageType::LIST))
                                    }
                                    PopType::POP_EMPTY => {
                                        if label_list.len() == 1 {
                                            ExtraEntryEntity::new(
                                                RawMessage::from_value(ValuePayload::String(label_name)),
                                                label_list.get(0).unwrap().get_message().clone())
                                        } else {
                                            let mut val_list = Vec::new();
                                            for label_entity in label_list {
                                                val_list.push(label_entity.get_message().clone());
                                            }
                                            ExtraEntryEntity::new(
                                                RawMessage::from_value(ValuePayload::String(label_name)),
                                                RawMessage::from_value_type(ValuePayload::List(val_list),
                                                                            RawMessageType::LIST))
                                        }
                                    }
                                }
                            };
                            map_value.push(entry);
                        }
                    }
                }
            }
            let mut result = RawMessage::from_value_type(ValuePayload::Map(map_value),
                                                         RawMessageType::MAP);
            self.after_requirement.process_take_extend_entity(&mut message, &mut result);
            result_list.push(self.after_requirement.process_requirement(result));
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct SelectOneOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    label_exist: bool,
    label_id: i32,
    label_name: String,
    pop_type: PopType,
}

impl<F> SelectOneOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let label_exist = base.get_argument().get_bool_value();
        let pop_type = PopType::from_i32(base.get_argument().get_int_value()).unwrap();
        let label_id = base.get_argument().get_int_value_list()[0];
        let label_name = base.get_argument().get_str_value().to_string();
        SelectOneOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            label_exist,
            label_id,
            label_name,
            pop_type,
        }
    }
}

impl<F> Operator for SelectOneOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for SelectOneOperator<F>
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
            if !self.label_exist {
                if message.get_message_type() == RawMessageType::MAP {
                    let mut message = self.before_requirement.process_requirement(message);
                    if let Some(entry_list) = message.take_map_value() {
                        for entry in entry_list.into_iter() {
                            if let Some(key_payload) = entry.get_key().get_value() {
                                if let Ok(key_val) = key_payload.get_string() {
                                    if key_val.eq(&self.label_name) {
                                        let mut result = entry.take_value();
                                        self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                                        result_list.push(self.after_requirement.process_requirement(result));
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }
            let message = self.before_requirement.process_requirement(message);
            let mut found_flag = false;
            if let Some(map_value) = message.get_map_value() {
                for entry in map_value {
                    if let Some(value) = entry.get_key().get_value() {
                        if let Ok(str_val) = value.get_string() {
                            if str_val.eq(&self.label_name) {
                                let mut result = entry.get_value().as_ref().clone();
                                self.after_requirement.process_extend_entity(&message, &mut result);
                                result_list.push(self.after_requirement.process_requirement(result));
                                found_flag = true;
                            }
                        }
                    }
                }
            }
            if !found_flag {
                if let Some(label_list) = message.get_label_entity_list_by_id(self.label_id) {
                    if !label_list.is_empty() {
                        let mut result = {
                            match self.pop_type {
                                PopType::FIRST => {
                                    label_list.get(0).unwrap().get_message().clone()
                                }
                                PopType::LAST => {
                                    label_list.get(label_list.len() - 1).unwrap().get_message().clone()
                                }
                                PopType::ALL => {
                                    let mut val_list = Vec::new();
                                    for label_entity in label_list {
                                        val_list.push(label_entity.get_message().clone());
                                    }
                                    RawMessage::from_value_type(ValuePayload::List(val_list),
                                                                RawMessageType::LIST)
                                }
                                PopType::POP_EMPTY => {
                                    if label_list.len() == 1 {
                                        label_list.get(0).unwrap().get_message().clone()
                                    } else {
                                        let mut val_list = Vec::new();
                                        for label_entity in label_list {
                                            val_list.push(label_entity.get_message().clone());
                                        }
                                        RawMessage::from_value_type(ValuePayload::List(val_list),
                                                                    RawMessageType::LIST)
                                    }
                                }
                            }
                        };
                        self.after_requirement.process_extend_entity(&message, &mut result);
                        result_list.push(self.after_requirement.process_requirement(result));
                    }
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct ColumnOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    column_type: ColumnType,
    label_id_name_list: HashMap<i32, String>,
}

impl<F> ColumnOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               label_id_name_list: HashMap<i32, String>) -> Self {
        let column_type = ColumnType::from_i32(base.get_argument().get_int_value()).unwrap();
        ColumnOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            column_type,
            label_id_name_list,
        }
    }
}

impl<F> Operator for ColumnOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

#[inline]
fn get_column_value(message: &mut RawMessage,
                    column_type: &ColumnType,
                    label_id_name_list: &HashMap<i32, String>) -> Option<RawMessage> {
    match column_type {
        &ColumnType::COLUMN_KEYS => {
            match message.get_message_type() {
                RawMessageType::ENTRY => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(entry) = value_entity.take_entry() {
                            return Some(entry.take_key());
                        }
                    }
                }
                RawMessageType::MAP => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(mut map) = value_entity.take_map() {
                            let mut value_list = Vec::with_capacity(map.len());
                            for map_entry in map.drain(..) {
                                value_list.push(map_entry.take_key());
                            }
                            return Some(RawMessage::from_value_type(ValuePayload::List(value_list), RawMessageType::LIST));
                        }
                    }
                }
                RawMessageType::LIST => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(mut list) = value_entity.take_list() {
                            let mut value_list = Vec::with_capacity(list.len());
                            for mut list_entry in list.drain(..) {
                                if list_entry.get_message_type() == RawMessageType::PATHENTRY {
                                    if let Some(path_entry_value) = list_entry.take_value() {
                                        if let Ok(mut path_entry) = path_entry_value.take_path_entry() {
                                            if let Some(label_list) = path_entry.take_label_list() {
                                                let mut label_message_list = Vec::with_capacity(label_list.len());
                                                for label_id in label_list {
                                                    label_message_list.push(RawMessage::from_value(
                                                        ValuePayload::String(
                                                            label_id_name_list.get(&label_id).unwrap().clone())));
                                                }
                                                value_list.push(RawMessage::from_value_type(ValuePayload::List(label_message_list),
                                                                                            RawMessageType::LIST));
                                            }
                                        }
                                    }
                                }
                            }
                            return Some(RawMessage::from_value_type(ValuePayload::List(value_list), RawMessageType::LIST));
                        }
                    }
                }
                _ => {}
            }
        }
        &ColumnType::COLUMN_VALUES => {
            match message.get_message_type() {
                RawMessageType::ENTRY => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(entry) = value_entity.take_entry() {
                            return Some(entry.take_value());
                        }
                    }
                }
                RawMessageType::MAP => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(mut map) = value_entity.take_map() {
                            let mut value_list = Vec::with_capacity(map.len());
                            for map_entry in map.drain(..) {
                                value_list.push(map_entry.take_value());
                            }
                            return Some(RawMessage::from_value_type(ValuePayload::List(value_list), RawMessageType::LIST));
                        }
                    }
                }
                RawMessageType::LIST => {
                    if let Some(value_entity) = message.take_value() {
                        if let Ok(mut list) = value_entity.take_list() {
                            let mut value_list = Vec::with_capacity(list.len());
                            for mut list_entry in list.drain(..) {
                                if list_entry.get_message_type() == RawMessageType::PATHENTRY {
                                    if let Some(path_entry_value) = list_entry.take_value() {
                                        if let Ok(path_entry) = path_entry_value.take_path_entry() {
                                            value_list.push(path_entry.take_message());
                                        }
                                    }
                                }
                            }
                            return Some(RawMessage::from_value_type(ValuePayload::List(value_list), RawMessageType::LIST));
                        }
                    }
                }
                _ => {}
            }
        }
    }
    return None;
}

impl<F> UnaryOperator for ColumnOperator<F>
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
            match message.get_message_type() {
                RawMessageType::ERROR => {
                    result_list.push(message);
                }
                _ => {
                    let mut message = self.before_requirement.process_requirement(message);
                    if let Some(mut result) = get_column_value(&mut message,
                                                               &self.column_type,
                                                               &self.label_id_name_list) {
                        self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                        result_list.push(self.after_requirement.process_requirement(result));
                    }
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct LabelValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    label_id: i32,
    label_value_operator: Option<Box<UnaryOperator>>,
    require_label_flag: bool,
}

impl<F> LabelValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               label_value_operator: Option<Box<UnaryOperator>>) -> Self {
        let label_id = base.get_argument().get_int_value();
        let require_label_flag = base.get_argument().get_bool_value();
        LabelValueOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            label_id,
            label_value_operator,
            require_label_flag,
        }
    }
}

impl<F> Operator for LabelValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for LabelValueOperator<F>
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
            let label_input_option = {
                if let Some(curr_label_value) = message.take_label_entity_by_id(self.label_id) {
                    Some(curr_label_value.take_message())
                } else {
                    if self.require_label_flag {
                        None
                    } else {
                        Some(message.clone())
                    }
                }
            };
            if let Some(label_input) = label_input_option {
                if let Some(ref mut label_operator) = self.label_value_operator {
                    let mut data = Vec::with_capacity(10);
                    {
                        let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                        label_operator.execute(vec![label_input], &mut local_collector);
                    }
                    if let Some(label_value) = data.pop() {
                        message.add_label_entity(label_value, self.label_id);
                    } else {
                        error!("cant build label value");
                    }
                } else {
                    let label_message = message.build_message_value();
                    message.add_label_entity(label_message, self.label_id);
                }
                result_list.push(message);
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}
