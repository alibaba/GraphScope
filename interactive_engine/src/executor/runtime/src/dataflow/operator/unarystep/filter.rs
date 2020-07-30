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

use maxgraph_store::api::*;
use maxgraph_store::api::SnapshotId;
use maxgraph_store::schema::LabelId;
use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::proto::message::CompareType;

use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::filter::*;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::message::{RawMessage, RawMessageType, ValuePayload};
use dataflow::message::primitive::Read;
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::builder::{InputStreamShuffle, UnaryOperator, Operator, MessageCollector};
use itertools::Itertools;
use std::sync::Arc;
use utils::*;
use store::store_delegate::StoreDelegate;
use store::LocalStoreVertex;
use std::collections::{HashSet, HashMap};

pub struct FilterOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    shuffle_flag: bool,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    filter_manager: FilterManager,
    prop_id_list: Vec<u32>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> FilterOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               shuffle_flag: bool,
               context: TaskContext,
               filter_manager: FilterManager,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self {
        let mut prop_id_list = vec![];
        for propid in filter_manager.get_related_propid_list() {
            if *propid > 0 {
                prop_id_list.push(*propid as u32);
            }
        }
        FilterOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            shuffle_flag,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            filter_manager,
            prop_id_list,
            context,
            global_graph,
        }
    }
}

impl<V, VI, E, EI, F> Operator for FilterOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for FilterOperator<V, VI, E, EI, F>
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
        let mut vertex_message_list = HashMap::new();
        let mut partition_vertex_list = vec![];
        let mut result_list = Vec::with_capacity(data.len());
        for message in data.into_iter() {
            if self.shuffle_flag && message.get_message_type() == RawMessageType::VERTEX {
                self.context.assign_prop_vertex_partition(Some(message.get_label_id() as u32),
                                                          message.get_id(),
                                                          &mut partition_vertex_list);
                vertex_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
            } else {
                if self.filter_manager.filter_message(&message) {
                    let message = self.before_requirement.process_requirement(message);
                    result_list.push(self.after_requirement.process_requirement(message));
                }
            }
        }
        if !partition_vertex_list.is_empty() {
            let mut vertex_list = self.global_graph.as_ref().get_vertex_properties(self.context.get_si(),
                                                                                                 partition_vertex_list,
                                                                                                 Some(&self.prop_id_list));
            while let Some(vertex) = vertex_list.next() {
                if self.filter_manager.filter_native_vertex(&vertex) {
                    if let Some(message_list) = vertex_message_list.remove(&vertex.get_id()) {
                        for message in message_list.into_iter() {
                            let message = self.before_requirement.process_requirement(message);
                            result_list.push(self.after_requirement.process_requirement(message));
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

pub struct WhereOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    compare_type: CompareType,
    start_label_id: i32,
    label_id: i32,
}

impl<F> WhereOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               start_label_id: i32,
               label_id: i32,
               compare_type: CompareType) -> Self {
        WhereOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            compare_type,
            start_label_id,
            label_id,
        }
    }
}

impl<F> Operator for WhereOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

fn get_where_label_message(label_id: i32, message: &RawMessage) -> Option<RawMessage> {
    if label_id == 0 {
        return Some(message.clone());
    } else {
        if label_id == PROP_ID {
            return Some(RawMessage::from_value(ValuePayload::Long(message.get_id())));
        } else if label_id == PROP_ID_LABEL {
            return Some(RawMessage::from_value(ValuePayload::Int(message.get_label_id())));
        } else if label_id == PROP_KEY {
            if let Some(value) = message.get_value() {
                if let Ok(entry) = value.get_entry() {
                    return Some(entry.clone().take_key());
                }
            }
        } else if label_id == PROP_VALUE {
            if let Some(value) = message.get_value() {
                if let Ok(entry) = value.get_entry() {
                    return Some(entry.clone().take_value());
                }
            }
        } else if label_id < 0 {
            if let Some(label_entity) = message.get_label_entity_by_id(label_id) {
                return Some(label_entity.get_message().clone());
            }
        } else {
            if let Some(prop_entity) = message.get_property(label_id) {
                return Some(RawMessage::from_value(prop_entity.get_value().clone()));
            }
        }
    }

    error!("cant get where label {:?} value from {:?}", label_id, &message);
    return None;
}

fn filter_label_message_value(mut start_message: RawMessage, mut label_message: RawMessage, compare_type: &CompareType) -> bool {
    match start_message.get_message_type() {
        RawMessageType::VERTEX | RawMessageType::EDGE => {
            if start_message.get_message_type() == label_message.get_message_type() {
                match compare_type {
                    CompareType::EQ => {
                        return start_message.get_id() == label_message.get_id();
                    }
                    CompareType::NEQ => {
                        return start_message.get_id() != label_message.get_id();
                    }
                    _ => {
                        error!("cant compare {:?} to {:?} with compare type {:?}",
                               start_message.get_message_type(),
                               label_message.get_message_type(),
                               compare_type);
                        return false;
                    }
                }
            } else {
                error!("cant compare {:?} to {:?} with compare type {:?}",
                       start_message.get_message_type(),
                       label_message.get_message_type(),
                       compare_type);
                return false;
            }
        }
        RawMessageType::VALUE => {
            if let Some(start_value) = start_message.take_value() {
                if let Some(label_value) = label_message.take_value() {
                    match compare_type {
                        CompareType::WITHIN | CompareType::WITHOUT => {
                            if label_message.get_message_type() == RawMessageType::LIST {
                                if let Ok(list_value) = label_value.take_list() {
                                    if *compare_type == CompareType::WITHIN {
                                        return filter_within_value(&start_value, &list_value);
                                    } else {
                                        return filter_without_value(&start_value, &list_value);
                                    }
                                }
                            } else {
                                error!("invalid label message type {:?} for with/without", label_message.get_message_type());
                            }
                        }
                        _ => {
                            if start_message.get_message_type() == label_message.get_message_type() {
                                match start_value {
                                    ValuePayload::Int(v) => {
                                        if let Ok(tv) = label_value.get_int() {
                                            return filter_value(compare_type, &v, &tv);
                                        }
                                    }
                                    ValuePayload::Long(v) => {
                                        if let Ok(tv) = label_value.get_long() {
                                            return filter_value(compare_type, &v, &tv);
                                        }
                                    }
                                    ValuePayload::String(v) => {
                                        if let Ok(tv) = label_value.get_string() {
                                            return filter_value(compare_type, &v, tv);
                                        }
                                    }
                                    ValuePayload::Float(v) => {
                                        if let Ok(tv) = label_value.get_float() {
                                            return filter_double_value(compare_type, &(f32::parse_bytes(&v) as f64), &(tv as f64));
                                        }
                                    }
                                    ValuePayload::Double(v) => {
                                        if let Ok(tv) = label_value.get_double() {
                                            return filter_double_value(compare_type, &(f64::parse_bytes(&v)), &tv);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        _ => {
            error!("invalid message type in where label operator {:?}", &start_message.get_message_type());
            return false;
        }
    }
    return false;
}

impl<F> UnaryOperator for WhereOperator<F>
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
            if let Some(start_message) = get_where_label_message(self.start_label_id, &message) {
                if let Some(label_message) = get_where_label_message(self.label_id, &message) {
                    if filter_label_message_value(start_message, label_message, &self.compare_type) {
                        result_list.push(self.after_requirement.process_requirement(message));
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

pub struct SimplePathOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    is_simple: bool,
}

impl<F> SimplePathOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        SimplePathOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            is_simple: base.get_argument().get_bool_value(),
        }
    }
}

impl<F> Operator for SimplePathOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for SimplePathOperator<F>
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
            if let Some(path_list) = message.get_path_list() {
                let val_list = path_list.iter().map(move |v|
                    v.iter().map(|vv| vv.get_message().get_id()).collect_vec()).collect_vec();
                let mut filter_index_list = vec![];
                match message.get_message_type() {
                    RawMessageType::VERTEX | RawMessageType::EDGE => {
                        for i in 0..val_list.len() {
                            if let Some(path_id_list) = val_list.get(i) {
                                if self.is_simple {
                                    if path_id_list.contains(&message.get_id()) {
                                        filter_index_list.push(i);
                                    }
                                } else {
                                    if path_id_list.len() > 0 && path_id_list[0] != message.get_id() {
                                        filter_index_list.push(i);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
                if filter_index_list.len() as i64 == message.get_bulk() {
                    continue;
                }
                filter_index_list.reverse();
                message.remove_path_bulk_list(&filter_index_list);
            }
            result_list.push(self.after_requirement.process_requirement(message));
        }

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}
