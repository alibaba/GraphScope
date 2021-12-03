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

use maxgraph_store::api::prelude::{Vertex, Edge, MVGraph, SnapshotId};
use maxgraph_store::schema::{LabelId, PropId};
use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::proto::message::{PropKeyValueType, PropertyType};

use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::message::*;
use dataflow::builder::*;

use utils::{PROP_ID, PROP_ID_LABEL, PROP_KEY, PROP_VALUE};
use std::sync::Arc;
use protobuf::ProtobufEnum;
use store::store_client::StoreClientManager;
use store::store_service::StoreServiceManager;
use std::collections::HashMap;
use store::store_delegate::StoreDelegate;
use itertools::Itertools;
use maxgraph_store::api::graph_schema::Schema;
use maxgraph_store::api::GlobalGraphQuery;

pub struct PropValueOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    prop_local: bool,
    prop_ids: Vec<i32>,
    schema: Arc<Schema>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> PropValueOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               schema: Arc<Schema>,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self {
        let prop_ids = base.get_argument().get_int_value_list().to_vec();
        let prop_local = base.get_argument().get_bool_flag();
        PropValueOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            prop_local,
            prop_ids,
            schema,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for PropValueOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for PropValueOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
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
        let mut result_list = Vec::with_capacity(data.len());
        let mut vertex_message_list = HashMap::new();
        let mut partition_vertex_label_list = Vec::with_capacity(data.len());
        for message in data.into_iter() {
            let mut message = self.before_requirement.process_requirement(message);
            if !self.prop_local && message.get_message_type() == RawMessageType::VERTEX {
                self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                          message.get_id(),
                                                          &mut partition_vertex_label_list);
                vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
            } else {
                match message.get_message_type() {
                    RawMessageType::VERTEX | RawMessageType::EDGE => {
                        if self.prop_ids.is_empty() {
                            if let Some(proplist) = message.get_prop_list() {
                                for prop in proplist {
                                    let mut result = RawMessage::from_value(prop.get_value().clone());
                                    self.after_requirement.process_extend_entity(&message, &mut result);
                                    result_list.push(self.after_requirement.process_requirement(result));
                                }
                            }
                        } else {
                            for propid in self.prop_ids.iter() {
                                let propid = *propid;
                                if let Some(mut result) = {
                                    if propid == PROP_ID {
                                        Some(RawMessage::from_value(ValuePayload::Long(message.get_id())))
                                    } else if propid == PROP_ID_LABEL {
                                        if let Some(label) = self.schema.get_label_name(message.get_label_id() as LabelId) {
                                            Some(RawMessage::from_value(ValuePayload::String(label.to_owned())))
                                        } else {
                                            None
                                        }
                                    } else if propid > 0 {
                                        if let Some(prop) = message.get_property(propid) {
                                            Some(RawMessage::from_value(prop.get_value().clone()))
                                        } else {
                                            None
                                        }
                                    } else {
                                        error!("invalid prop id {:?}", propid);
                                        None
                                    }
                                } {
                                    self.after_requirement.process_extend_entity(&message, &mut result);
                                    result_list.push(self.after_requirement.process_requirement(result));
                                }
                            }
                        }
                    }
                    RawMessageType::PROP => {
                        for propid in self.prop_ids.iter() {
                            let propid = *propid;
                            if let Some(mut result) = {
                                if propid == PROP_ID {
                                    Some(RawMessage::from_value(ValuePayload::Int(message.get_id() as i32)))
                                } else if propid == PROP_ID_LABEL || propid == PROP_KEY {
                                    let curr_prop_id = message.get_id() as i32;
                                    if let Some(prop_name) = self.schema.as_ref().get_prop_name(curr_prop_id as u32) {
                                        Some(RawMessage::from_value(ValuePayload::String(prop_name.to_owned())))
                                    } else {
                                        None
                                    }
                                } else if propid == PROP_VALUE {
                                    if let Some(prop_val) = message.take_value() {
                                        Some(RawMessage::from_value(prop_val))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } {
                                self.after_requirement.process_extend_entity(&message, &mut result);
                                result_list.push(self.after_requirement.process_requirement(result));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if !vertex_message_list.is_empty() {
            let mut curr_prop_ids = Vec::with_capacity(self.prop_ids.len());
            for propid in self.prop_ids.iter() {
                if *propid == PROP_ID {
                    for (_, mlist) in vertex_message_list.iter() {
                        for m in mlist {
                            let mut result = RawMessage::from_value(ValuePayload::Long(m.get_id()));
                            self.after_requirement.process_extend_entity(m, &mut result);
                            result_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                } else if *propid == PROP_ID_LABEL {
                    for (_, mlist) in vertex_message_list.iter() {
                        for m in mlist {
                            if let Some(label) = self.schema.as_ref().get_label_name(m.get_label_id() as u32) {
                                let mut result = RawMessage::from_value(ValuePayload::String(label.to_owned()));
                                self.after_requirement.process_extend_entity(m, &mut result);
                                result_list.push(self.after_requirement.process_requirement(result));
                            }
                        }
                    }
                } else if *propid > 0 {
                    curr_prop_ids.push(*propid as u32);
                }
            }
            if self.prop_ids.is_empty() || !curr_prop_ids.is_empty() {
                let vertex_prop_ids = {
                    if self.prop_ids.is_empty() {
                        None
                    } else {
                        Some(&curr_prop_ids)
                    }
                };
                let mut vertex_property_list = self.global_graph.get_vertex_properties(self.context.get_si(),
                                                                                       partition_vertex_label_list,
                                                                                       vertex_prop_ids);
                while let Some(v) = vertex_property_list.next() {
                    if let Some(mv) = vertex_message_list.remove(&v.get_id()) {
                        for m in mv.into_iter() {
                            for (propid, prop) in v.get_properties() {
                                if curr_prop_ids.is_empty() || curr_prop_ids.contains(&propid) {
                                    let mut result = RawMessage::from_prop(prop);
                                    self.after_requirement.process_extend_entity(&m, &mut result);
                                    result_list.push(self.after_requirement.process_requirement(result));
                                }
                            }
                        }
                    }
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct PropKeyValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    key_value_type: PropKeyValueType,
    schema: Arc<dyn Schema>,
}

impl<F> PropKeyValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new<V, VI, E, EI>(input_id: i32,
                             stream_index: i32,
                             shuffle_type: StreamShuffleType<F>,
                             base: &OperatorBase,
                             context: &RuntimeContext<V, VI, E, EI, F>) -> Self
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
        let prop_key_value_type = PropKeyValueType::from_i32(base.get_argument().get_int_value()).expect("property type");
        PropKeyValueOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            key_value_type: prop_key_value_type,
            schema: context.get_schema().clone(),
        }
    }
}

impl<F> Operator for PropKeyValueOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for PropKeyValueOperator<F>
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
                RawMessageType::PROP => {
                    if let Some(mut result) = {
                        match self.key_value_type {
                            PropKeyValueType::PROP_KEY_TYPE => {
                                let propid = message.get_id() as i32;
                                if let Some(prop_name) = self.schema.get_prop_name(propid as PropId) {
                                    Some(RawMessage::from_value(ValuePayload::String(prop_name.to_owned())))
                                } else {
                                    None
                                }
                            }
                            PropKeyValueType::PROP_VALUE_TYPE => {
                                if let Some(value) = message.take_value() {
                                    Some(RawMessage::from_value(value))
                                } else {
                                    None
                                }
                            }
                        }
                    } {
                        self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                        result_list.push(self.after_requirement.process_requirement(result));
                    }
                }
                RawMessageType::ERROR => {
                    result_list.push(message);
                }
                _ => {
                    error!("cant get prop from {:?}", &message);
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct PropMapOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    prop_local: bool,
    schema: Arc<dyn Schema>,
    property_type: PropertyType,
    include_tokens: bool,
    prop_ids: Vec<u32>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> PropMapOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               prop_local: bool,
               base: &OperatorBase,
               schema: Arc<Schema>,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self {
        let property_type = PropertyType::from_i32(base.get_argument().get_int_value()).expect("property type");
        let include_tokens = base.get_argument().get_bool_value();
        let prop_ids = base.get_argument().get_int_value_list().iter().map(|p| *p as u32).collect_vec();
        PropMapOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            prop_local,
            schema,
            property_type,
            include_tokens,
            prop_ids,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for PropMapOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> PropMapOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn process_after_requirement(&mut self,
                                 result_list: &mut Vec<RawMessage>,
                                 message: &mut RawMessage,
                                 mut maplist: Vec<ExtraEntryEntity>) {
        if self.include_tokens {
            match self.property_type {
                PropertyType::VALUE_TYPE => {
                    maplist.push(ExtraEntryEntity::new(
                        RawMessage::from_value(
                            ValuePayload::String("~id".to_owned())),
                        RawMessage::from_value(ValuePayload::Long(message.get_id()))));
                    if let Some(label_name) = self.schema.get_label_name(message.get_label_id() as LabelId) {
                        maplist.push(ExtraEntryEntity::new(
                            RawMessage::from_value(
                                ValuePayload::String("~label".to_owned())),
                            RawMessage::from_value(ValuePayload::String(label_name.to_owned()))));
                    }
                }
                _ => {}
            }
        }
        let mut result = RawMessage::from_value_type(ValuePayload::Map(maplist), RawMessageType::MAP);
        self.after_requirement.process_take_extend_entity(message, &mut result);
        result_list.push(self.after_requirement.process_requirement(result));
    }
}

impl<V, VI, E, EI, F> UnaryOperator for PropMapOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
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
        let mut result_list = Vec::with_capacity(data.len());
        let mut vertex_message_list = HashMap::new();
        let mut partition_vertex_label_list = Vec::with_capacity(data.len());
        for message in data.into_iter() {
            if self.context.get_debug_flag() {
                info!("Start to get prop map for {:?}", &message);
            }
            let mut message = self.before_requirement.process_requirement(message);
            let mut maplist = Vec::new();
            if !self.prop_local && message.get_message_type() == RawMessageType::VERTEX {
                self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                          message.get_id(),
                                                          &mut partition_vertex_label_list);
                vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
            } else {
                if self.prop_ids.is_empty() {
                    if let Some(proplist) = message.get_prop_list() {
                        for prop in proplist {
                            if let Some(prop_name) = self.schema.get_prop_name(prop.get_propid() as PropId) {
                                match self.property_type {
                                    PropertyType::VALUE_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_value(prop.get_value().clone())]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                    PropertyType::PROP_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_prop_entity(prop.clone())]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    for propid in self.prop_ids.iter() {
                        let propid = *propid;
                        if let Some(prop_name) = self.schema.get_prop_name(propid as PropId) {
                            if let Some(prop) = message.get_property(propid as i32) {
                                match self.property_type {
                                    PropertyType::VALUE_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_value(prop.get_value().clone())]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                    PropertyType::PROP_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_prop_entity(prop.clone())]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                }
                            }
                        }
                    }
                }
                self.process_after_requirement(&mut result_list, &mut message, maplist);
            }
        }
        if !vertex_message_list.is_empty() {
            if self.context.get_debug_flag() {
                info!("Start to fetch properties for partition vertex label list {:?} vertex list {:?}",
                      &partition_vertex_label_list,
                      &vertex_message_list);
            }
            let mut vertex_list = self.global_graph.get_vertex_properties(self.context.get_si(),
                                                                          partition_vertex_label_list,
                                                                          {
                                                                              if self.prop_ids.is_empty() {
                                                                                  None
                                                                              } else {
                                                                                  Some(&self.prop_ids)
                                                                              }
                                                                          });
            while let Some(v) = vertex_list.next() {
                if let Some(mut mlist) = vertex_message_list.remove(&v.get_id()) {
                    let mut maplist = Vec::new();
                    let mut vertex_prop_list = v.get_properties();
                    while let Some((propid, prop)) = vertex_prop_list.next() {
                        if self.prop_ids.is_empty() || self.prop_ids.contains(&propid) {
                            if let Some(prop_name) = self.schema.get_prop_name(propid) {
                                match self.property_type {
                                    PropertyType::VALUE_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_prop(prop)]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                    PropertyType::PROP_TYPE => {
                                        let entry = ExtraEntryEntity::new(
                                            RawMessage::from_value(ValuePayload::String(prop_name.to_owned())),
                                            RawMessage::from_value_type(ValuePayload::List(vec![RawMessage::from_prop_type(prop, propid as i32)]),
                                                                        RawMessageType::LIST));
                                        maplist.push(entry);
                                    }
                                }
                            }
                        }
                    }
                    if mlist.len() > 0 {
                        for i in 1..mlist.len() {
                            let vv = mlist.get_mut(i).unwrap();
                            self.process_after_requirement(&mut result_list, vv, maplist.clone());
                        }
                        let vv = mlist.get_mut(0).unwrap();
                        self.process_after_requirement(&mut result_list, vv, maplist.clone());
                    }
                }
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}


pub struct PropFillOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    prop_ids: Vec<u32>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> PropFillOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>, ) -> Self {
        let prop_ids = base.get_argument().get_int_value_list().iter().map(|p| *p as u32).collect();
        PropFillOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            prop_ids,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for PropFillOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for PropFillOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
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
        let mut result_list = Vec::with_capacity(data.len());
        let mut vertex_message_list = HashMap::new();
        let mut partition_vertex_list = vec![];
        for m in data.into_iter() {
            self.context.assign_prop_vertex_partition(Some(m.get_label_id() as u32),
                                                      m.get_id(),
                                                      &mut partition_vertex_list);
            vertex_message_list.entry(m.get_id()).or_insert(vec![]).push(m);
        }

        let mut vertex_list = self.global_graph.get_vertex_properties(self.context.get_si(),
                                                                      partition_vertex_list,
                                                                      Some(&self.prop_ids));
        while let Some(v) = vertex_list.next() {
            if let Some(mut mlist) = vertex_message_list.remove(&v.get_id()) {
                if mlist.len() > 0 {
                    for (propid, prop) in v.get_properties() {
                        if self.prop_ids.is_empty() || self.prop_ids.contains(&propid) {
                            for i in 1..mlist.len() {
                                mlist.get_mut(i).unwrap().add_native_property(propid as i32, prop.clone());
                            }
                            mlist.get_mut(0).unwrap().add_native_property(propid as i32, prop);
                        }
                    }
                    result_list.append(&mut mlist);
                }
            }
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct PropertiesOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    prop_ids: Vec<u32>,
    prop_local_flag: bool,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> PropertiesOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               prop_local_flag: bool,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>, ) -> Self {
        let prop_ids = base.get_argument().get_int_value_list().iter().map(|v| *v as u32).collect();
        PropertiesOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            prop_ids,
            prop_local_flag,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for PropertiesOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for PropertiesOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
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
        let mut prop_list = Vec::with_capacity(data.len());
        let mut vertex_message_list = HashMap::new();
        let mut partition_vertex_list = vec![];
        for message in data.into_iter() {
            let mut message = self.before_requirement.process_requirement(message);
            if !self.prop_local_flag && message.get_message_type() == RawMessageType::VERTEX {
                self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                          message.get_id(),
                                                          &mut partition_vertex_list);
                vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
            } else {
                let vertex_flag = message.get_message_type() == RawMessageType::VERTEX;
                if self.prop_ids.is_empty() {
                    if let Some(proplist) = message.take_prop_entity_list() {
                        for propentity in proplist.into_iter() {
                            let mut result = {
                                if vertex_flag {
                                    RawMessage::from_vertex_prop_entity(propentity)
                                } else {
                                    RawMessage::from_prop_entity(propentity)
                                }
                            };
                            self.after_requirement.process_extend_entity(&message, &mut result);
                            prop_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                } else {
                    for pidref in self.prop_ids.iter() {
                        let propid = *pidref as i32;
                        if let Some(propentity) = message.take_prop_entity(propid) {
                            let mut result = {
                                if vertex_flag {
                                    RawMessage::from_vertex_prop_entity(propentity)
                                } else {
                                    RawMessage::from_prop_entity(propentity)
                                }
                            };
                            self.after_requirement.process_extend_entity(&message, &mut result);
                            prop_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                }
            }
        }
        if !partition_vertex_list.is_empty() {
            let mut vertex_list = self.global_graph.as_ref().get_vertex_properties(self.context.get_si(),
                                                                                   partition_vertex_list,
                                                                                   {
                                                                                       if self.prop_ids.is_empty() {
                                                                                           None
                                                                                       } else {
                                                                                           Some(&self.prop_ids)
                                                                                       }
                                                                                   });
            while let Some(vertex) = vertex_list.next() {
                if let Some(message_list) = vertex_message_list.remove(&vertex.get_id()) {
                    for (propid, prop) in vertex.get_properties() {
                        if self.prop_ids.is_empty() || self.prop_ids.contains(&propid) {
                            let mut result = RawMessage::from_vertex_prop_type(prop, propid as i32);
                            for i in 1..message_list.len() {
                                let mut curr_result = result.clone();
                                self.after_requirement.process_extend_entity(message_list.get(i).unwrap(), &mut curr_result);
                                prop_list.push(self.after_requirement.process_requirement(curr_result));
                            }
                            self.after_requirement.process_extend_entity(message_list.get(0).unwrap(), &mut result);
                            prop_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                }
            }
        }

        collector.collect_iterator(Box::new(prop_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

