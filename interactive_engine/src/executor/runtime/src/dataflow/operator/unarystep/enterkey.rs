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

use maxgraph_common::proto::query_flow::*;
use maxgraph_store::schema::{LabelId, PropId};
use maxgraph_store::api::prelude::*;

use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::message::*;
use dataflow::manager::requirement::RequirementManager;

use utils::*;
use std::sync::Arc;
use store::store_delegate::StoreDelegate;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use std::collections::HashMap;
use maxgraph_store::api::graph_schema::Schema;

pub struct EnterKeyOperator<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    enter_key_argument: EnterKeyArgumentProto,
    prop_id_list: Vec<u32>,
    schema: Arc<dyn Schema>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>
}

impl<V, VI, E, EI, F> EnterKeyOperator<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               schema: Arc<dyn Schema>,
               context: TaskContext,
               shuffle_type: StreamShuffleType<F>,
               enter_key_argument: EnterKeyArgumentProto,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self {
        let prop_id_list = enter_key_argument.get_prop_id_list().iter().map(|p| *p as u32).collect();
        EnterKeyOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            enter_key_argument,
            prop_id_list,
            schema,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for EnterKeyOperator<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for EnterKeyOperator<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
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
        let uniq_flag = self.enter_key_argument.get_uniq_flag();
        let enter_key_type = self.enter_key_argument.get_enter_key_type();
        let mut result_list = Vec::with_capacity(data.len());

//        let mut vertex_prop_message_list = Vec::with_capacity(data.len());
        match enter_key_type {
            EnterKeyTypeProto::KEY_SELF => {
                for mut message in data.into_iter() {
                    let extra_key = {
                        if uniq_flag {
                            message.build_message_key()
                        } else {
                            message.build_message_value()
                        }
                    };
                    message.set_extend_key_message(extra_key, uniq_flag);
                    result_list.push(message);
                }
            }
            EnterKeyTypeProto::KEY_PROP_LABEL => {
                let prop_label_id = self.enter_key_argument.get_prop_label_id();
                if prop_label_id > 0 {
                    let mut vertex_message_list = HashMap::new();
                    let mut partition_vertex_label_list = Vec::with_capacity(data.len());
                    for mut message in data.into_iter() {
                        match message.get_message_type() {
                            RawMessageType::VERTEX => {
                                self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                                          message.get_id(),
                                                                          &mut partition_vertex_label_list);
                                vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                            }
                            _ => {
                                if let Some(prop) = message.get_property(prop_label_id) {
                                    let prop_message = RawMessage::from_value(prop.get_value().clone());
                                    message.set_extend_key_message(prop_message, uniq_flag);
                                    result_list.push(message);
                                } else {
                                    error!("cant generate key from property {:?}", prop_label_id);
                                }
                            }
                        }
                    }
                    let propidlist = vec![prop_label_id as u32];
                    if !partition_vertex_label_list.is_empty() {
                        let mut vertex_list = self.global_graph.as_ref().get_vertex_properties(self.context.get_si(),
                                                                                                             partition_vertex_label_list,
                                                                                                             Some(&propidlist));
                        while let Some(v) = vertex_list.next() {
                            if let Some(mut message) = vertex_message_list.remove(&v.get_id()) {
                                if let Some(propval) = v.get_property(prop_label_id as PropId) {
                                    for i in 1..message.len() {
                                        message.get_mut(i).unwrap().set_extend_key_message(RawMessage::from_prop(propval.clone()), uniq_flag);
                                    }
                                    message.get_mut(0).unwrap().set_extend_key_message(RawMessage::from_prop(propval), uniq_flag);
                                }
                                result_list.append(&mut message);
                            }
                        }
                    }
                } else if prop_label_id < 0 {
                    for mut message in data.into_iter() {
                        if prop_label_id == PROP_ID {
                            let mid = message.get_id();
                            let key_message = RawMessage::from_value(ValuePayload::Long(mid));
                            message.set_extend_key_message(key_message, uniq_flag);
                        } else if prop_label_id == PROP_ID_LABEL {
                            let label_id = message.get_label_id();
                            if let Some(label_name) = self.schema.as_ref().get_label_name(label_id as LabelId) {
                                let key_message = RawMessage::from_value(ValuePayload::String(label_name.to_owned()));
                                message.set_extend_key_message(key_message, uniq_flag);
                            } else {
                                error!("cant get label name for label id {:?} when generate extra key", label_id);
                            }
                        } else if prop_label_id == PROP_KEY {
                            if let Some(value) = message.get_value() {
                                if let Ok(entry) = value.get_entry() {
                                    let entry_key = entry.get_key().clone();
                                    message.set_extend_key_message(*entry_key, uniq_flag);
                                }
                            }
                        } else if prop_label_id == PROP_VALUE {
                            if let Some(value) = message.get_value() {
                                if let Ok(entry) = value.get_entry() {
                                    let entry_value = entry.get_value().clone();
                                    message.set_extend_key_message(*entry_value, uniq_flag);
                                }
                            }
                        } else {
                            if let Some(label_entity) = message.get_label_entity_by_id(prop_label_id) {
                                let key_message = label_entity.get_message().clone();
                                message.set_extend_key_message(key_message, uniq_flag);
                            } else {
                                error!("cant get label entity {:?} when generate extra key", prop_label_id);
                            }
                        }
                        result_list.push(message);
                    }
                } else {
                    for mut message in data.into_iter() {
                        let key_message = message.build_message_value();
                        message.set_extend_key_message(key_message, uniq_flag);
                        result_list.push(message);
                    }
                }
            }
            EnterKeyTypeProto::KEY_PROP_VAL_MAP => {
                let mut vertex_message_list = HashMap::new();
                let mut partition_vertex_label_list = Vec::with_capacity(data.len());
                for mut message in data.into_iter() {
                    match message.get_message_type() {
                        RawMessageType::VERTEX => {
                            self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                                      message.get_id(),
                                                                      &mut partition_vertex_label_list);
                            vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                        }
                        _ => {
                            let mut prop_value_list = Vec::new();
                            for prop_id in self.prop_id_list.iter() {
                                if let Some(prop_name) = self.schema.as_ref().get_prop_name(*prop_id) {
                                    if let Some(prop) = message.get_property(*prop_id as i32) {
                                        prop_value_list.push(ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value(prop.get_value().clone())));
                                    }
                                }
                            }
                            let key_message = RawMessage::from_value_type(ValuePayload::Map(prop_value_list), RawMessageType::MAP);
                            message.set_extend_key_message(key_message, uniq_flag);
                            result_list.push(message);
                        }
                    }
                }
                if !partition_vertex_label_list.is_empty() {
                    let mut vertex_list = self.global_graph.as_ref().get_vertex_properties(self.context.get_si(),
                                                                                                         partition_vertex_label_list,
                                                                                                         Some(&self.prop_id_list));
                    while let Some(vertex) = vertex_list.next() {
                        if let Some(mut mlist) = vertex_message_list.remove(&vertex.get_id()) {
                            let mut prop_value_list = vec![];
                            for (propid, prop) in vertex.get_properties() {
                                if self.prop_id_list.is_empty() || self.prop_id_list.contains(&propid) {
                                    if let Some(prop_name) = self.schema.as_ref().get_prop_name(propid as u32) {
                                        let prop_message = RawMessage::from_prop(prop);
                                        prop_value_list.push(ExtraEntryEntity::new(RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                                                                   prop_message));
                                    }
                                }
                            }
                            for i in 1..mlist.len() {
                                mlist.get_mut(i).unwrap().set_extend_key_message(
                                    RawMessage::from_value_type(ValuePayload::Map(prop_value_list.clone()), RawMessageType::MAP),
                                    uniq_flag);
                            }
                            mlist.get_mut(0).unwrap().set_extend_key_message(
                                RawMessage::from_value_type(ValuePayload::Map(prop_value_list), RawMessageType::MAP),
                                uniq_flag);
                            result_list.append(&mut mlist);
                        }
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

pub struct ByKeyEntryOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    after_requirement: RequirementManager,
}

impl<F> ByKeyEntryOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>) -> Self {
        ByKeyEntryOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for ByKeyEntryOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for ByKeyEntryOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(key) = message.take_extend_key_payload() {
                let extra_key = ExtraKeyEntity::from_payload(&key);
                let mut bykey_entry = RawMessage::from_entry(extra_key.take_message(), message.build_message_value());
                self.after_requirement.process_take_extend_entity(&mut message, &mut bykey_entry);
                result_list.push(self.after_requirement.process_requirement(bykey_entry));
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct KeyMessageOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    after_requirement: RequirementManager,
}

impl<F> KeyMessageOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>) -> Self {
        KeyMessageOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for KeyMessageOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for KeyMessageOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(key) = message.get_extend_key_payload() {
                let extra_key = ExtraKeyEntity::from_payload(&key);
                let mut key_message = extra_key.take_message();
                self.after_requirement.process_take_extend_entity(&mut message, &mut key_message);
                result_list.push(self.after_requirement.process_requirement(key_message));
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}
