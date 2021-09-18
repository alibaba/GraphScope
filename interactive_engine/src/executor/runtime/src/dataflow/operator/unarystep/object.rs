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

use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::proto::message::*;
use maxgraph_store::api::*;

use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::message::*;
use dataflow::message::primitive::Write;
use dataflow::manager::requirement::RequirementManager;

use std::sync::Arc;
use utils::{PROP_ID, PROP_ID_LABEL};
use itertools::Itertools;
use maxgraph_store::api::graph_schema::Schema;

// count local operator
pub struct CountLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
}

impl<F> CountLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(id: i32,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        CountLocalOperator {
            id,
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for CountLocalOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for CountLocalOperator<F>
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
            if let Some(mut result) = {
                match message.get_message_type() {
                    RawMessageType::LIST => {
                        if let Some(value) = message.get_value() {
                            if let Ok(list) = value.get_list() {
                                Some(RawMessage::from_value(ValuePayload::Long(list.len() as i64)))
                            } else {
                                error!("cant get list value");
                                None
                            }
                        } else {
                            error!("cant get list value");
                            None
                        }
                    }
                    RawMessageType::MAP => {
                        if let Some(value) = message.get_value() {
                            if let Ok(list) = value.get_map() {
                                Some(RawMessage::from_value(ValuePayload::Long(list.len() as i64)))
                            } else {
                                error!("cant get map value");
                                None
                            }
                        } else {
                            error!("cant get map value");
                            None
                        }
                    }
                    _ => {
                        Some(RawMessage::from_value(ValuePayload::Long(1)))
                    }
                }
            } {
                self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                result_list.push(self.after_requirement.process_requirement(result));
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct UnfoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
}

impl<F> UnfoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        UnfoldOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for UnfoldOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for UnfoldOperator<F>
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
            let message = self.before_requirement.process_requirement(message);
            if let Some(value) = message.get_value() {
                if let Ok(list) = value.get_list() {
                    for m in list.iter() {
                        let mut result = m.clone();
                        self.after_requirement.process_extend_entity(&message, &mut result);
                        result_list.push(self.after_requirement.process_requirement(result));
                    }
                } else if let Ok(map) = value.get_map() {
                    for entry in map.iter() {
                        let mut result = RawMessage::from_value_type(ValuePayload::Entry(entry.clone()),
                                                                     RawMessageType::ENTRY);
                        self.after_requirement.process_extend_entity(&message, &mut result);
                        result_list.push(self.after_requirement.process_requirement(result));
                    }
                } else {
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

pub struct PathOutOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    schema: Arc<dyn Schema>,
    path_out_value_list: Vec<PathOutValue>,
    path_delete_flag: bool,
}

impl<F> PathOutOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               schema: Arc<dyn Schema>) -> Self {
        let path_out_value_list = base.get_argument().get_path_out_value().to_vec();
        let path_delete_flag = base.get_argument().get_bool_value();
        PathOutOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            schema,
            path_out_value_list,
            path_delete_flag,
        }
    }
}

impl<F> Operator for PathOutOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for PathOutOperator<F>
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
            message.set_bulk_value(1);
            if let Some(path_list) = {
                if self.path_delete_flag {
                    message.take_path_list()
                } else {
                    if let Some(path) = message.get_path_list() {
                        Some(path.to_vec())
                    } else {
                        None
                    }
                }
            } {
                let mut path_value_list = {
                    if self.path_out_value_list.is_empty() {
                        path_list.into_iter()
                            .map(
                                move |v| {
                                    let path_result_list = v.into_iter()
                                        .map(move |vv| RawMessage::from_value_type(
                                            ValuePayload::PathEntry(Box::new(vv)),
                                            RawMessageType::PATHENTRY))
                                        .collect_vec();
                                    RawMessage::from_value_type(ValuePayload::List(path_result_list), RawMessageType::LIST)
                                })
                            .collect_vec()
                    } else {
                        path_list.into_iter()
                            .map(|v| {
                                let pathlen = v.len();
                                let mut path_result_list = Vec::with_capacity(pathlen);
                                let mut index = 0;
                                for mut path_value in v.into_iter() {
                                    let path_out_value = self.path_out_value_list.get(index % self.path_out_value_list.len()).unwrap();
                                    match path_out_value.path_out_type {
                                        PathOutType::PATH_VALUE => {
                                            path_result_list.push(RawMessage::from_value_type(ValuePayload::PathEntry(Box::new(path_value)),
                                                                                              RawMessageType::PATHENTRY));
                                        }
                                        PathOutType::PATH_PROP => {
                                            let prop_id = path_out_value.get_path_prop_id();
                                            let label_list = path_value.take_label_list();
                                            if let Some(result) = {
                                                if prop_id == PROP_ID {
                                                    Some(RawMessage::from_value(ValuePayload::Long(path_value.get_message().get_id())))
                                                } else if prop_id == PROP_ID_LABEL {
                                                    let label_id = path_value.get_message().get_label_id();
                                                    if let Some(label_name) = self.schema.get_label_name(label_id as LabelId) {
                                                        Some(RawMessage::from_value(ValuePayload::String(label_name.to_owned())))
                                                    } else {
                                                        error!("cant get label name for {}", label_id);
                                                        None
                                                    }
                                                } else if prop_id > 0 {
                                                    let mut path_message = path_value.take_message();
                                                    if let Some(prop) = path_message.take_prop_entity(prop_id) {
                                                        Some(RawMessage::from_value(prop.take_value()))
                                                    } else {
                                                        error!("cant get property {} from {:?}", prop_id, &path_message);
                                                        None
                                                    }
                                                } else {
                                                    error!("cant get path value from {:?}", &prop_id);
                                                    None
                                                }
                                            } {
                                                let path_result = RawMessage::from_path_entry(label_list, result);
                                                path_result_list.push(path_result);
                                            }
                                        }
                                    }
                                    index += 1;
                                }
                                RawMessage::from_value_type(ValuePayload::List(path_result_list), RawMessageType::LIST)
                            })
                            .collect_vec()
                    }
                };
                let path_count = path_value_list.len();
                for i in 0..path_count - 1 {
                    self.after_requirement.process_extend_entity(&message, path_value_list.get_mut(i).unwrap());
                }
                if path_count > 0 {
                    self.after_requirement.process_take_extend_entity(&mut message, path_value_list.get_mut(path_count - 1).unwrap());
                }
                for path_value in path_value_list.into_iter() {
                    result_list.push(self.after_requirement.process_requirement(path_value));
                }
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct ConstantOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    message: RawMessage,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
}

impl<F> ConstantOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let value = base.get_argument().clone();
        let message = {
            match value.get_value_type() {
                VariantType::VT_INT => {
                    RawMessage::from_value(ValuePayload::Int(value.get_int_value()))
                }
                VariantType::VT_LONG => {
                    RawMessage::from_value(ValuePayload::Long(value.get_long_value()))
                }
                VariantType::VT_STRING => {
                    RawMessage::from_value(ValuePayload::String(value.get_str_value().to_owned()))
                }
                VariantType::VT_FLOAT => {
                    RawMessage::from_value(ValuePayload::Float(value.get_float_value().into_bytes()))
                }
                VariantType::VT_DOUBLE => {
                    RawMessage::from_value(ValuePayload::Double(value.get_double_value().into_bytes()))
                }
                _ => {
                    error!("cant build constant value for {:?}", &value);
                    RawMessage::from_error(ErrorCode::INTERNAL_ERROR, format!("cant build constant value for {:?}", &value))
                }
            }
        };
        ConstantOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            message,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for ConstantOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for ConstantOperator<F>
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
            let mut result = self.message.clone();
            self.after_requirement.process_take_extend_entity(&mut message, &mut result);
            result_list.push(self.after_requirement.process_requirement(result));
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}
