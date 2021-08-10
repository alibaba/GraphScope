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

// vertex related step

use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::common::iterator::{IteratorList, VertexDirectionResultIterator, EdgeDirectionResultIterator};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::dedup::*;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::message::{RawMessage, ValuePayload};

use maxgraph_common::proto::message::{EdgeDirection, ErrorCode, LogicalCompare};
use maxgraph_store::api::*;

use maxgraph_common::proto::query_flow::OperatorBase;
use itertools::Itertools;
use std::sync::Arc;
use crossbeam_queue::ArrayQueue;
use store::store_service::StoreServiceManager;
use std::rc::Rc;
use store::StoreOperatorType;
use core::borrow::BorrowMut;
use std::collections::HashMap;
use maxgraph_store::api::graph_schema::Schema;

pub struct VertexDirectionOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    input_shuffle: StreamShuffleType<F>,
    stream_index: i32,
    direction: EdgeDirection,
    label_list: Vec<u32>,
    filter_manager: Arc<FilterManager>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    before_requirement: RequirementManager,
    after_requirement: Arc<RequirementManager>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> VertexDirectionOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               input_shuffle: StreamShuffleType<F>,
               direction: EdgeDirection,
               base: &OperatorBase,
               schema: Arc<Schema>,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>, ) -> Self {
        let label_list = base.get_argument().get_int_value_list().iter().map(move |v| *v as u32).collect_vec();
        let filter_manager = Arc::new(FilterManager::new(base.get_logical_compare(),
                                                         schema));
        let range_limit = {
            if base.has_range_limit() {
                base.get_range_limit().range_end as usize
            } else {
                0
            }
        };
        let dedup_manager = {
            if let Some(manager) = parse_dedup_manager(base, context.get_debug_flag()) {
                Some(Arc::new(manager))
            } else {
                None
            }
        };

        let before_requirement = RequirementManager::new(base.get_before_requirement().to_vec());
        let after_requirement = RequirementManager::new(base.get_after_requirement().to_vec());

        VertexDirectionOperator {
            id: base.get_id(),
            input_id,
            input_shuffle,
            stream_index,
            direction,
            label_list,
            filter_manager,
            range_limit,
            dedup_manager,
            before_requirement,
            after_requirement: Arc::new(after_requirement),
            context,
            global_graph,
        }
    }

    fn collect_result_list<'a>(&self,
                               keyed_message_list: HashMap<i64, Vec<RawMessage>>,
                               result_list: Box<Iterator<Item=(i64, VI)>>,
                               collector: &mut Box<'a + MessageCollector>) {
        let mut result_iter_vec = Vec::with_capacity(keyed_message_list.len());
        for (src_id, vlist) in result_list {
            if let Some(message_list) = keyed_message_list.get(&src_id) {
                let result_iter = VertexDirectionResultIterator::new(self.filter_manager.clone(),
                                                                     self.range_limit,
                                                                     self.dedup_manager.clone(),
                                                                     self.after_requirement.clone(),
                                                                     vlist,
                                                                     message_list.to_vec());
                result_iter_vec.push(result_iter);
            }
        }
        let result_iter = IteratorList::new(result_iter_vec);
        collector.collect_iterator(Box::new(result_iter));
    }
}

impl<V, VI, E, EI, F> Operator for VertexDirectionOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for VertexDirectionOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.input_shuffle.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        match self.direction {
            EdgeDirection::DIR_OUT => {
                let mut keyed_message_list = HashMap::new();
                let mut partition_vertex_list = Vec::with_capacity(data.len());
                for m in data.into_iter() {
                    self.context.assign_out_vertex_partition(m.get_id(), &mut partition_vertex_list);
                    let message = self.before_requirement.process_requirement(m);
                    keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                }
                if self.context.get_debug_flag() {
                    info!("get out vertices for {:?} with label list {:?} in task {:?}", &partition_vertex_list, &self.label_list, self.context.get_worker_index());
                }
                let out_result_list = self.global_graph.as_ref().get_out_vertex_ids(self.context.get_si(),
                                                                                    partition_vertex_list,
                                                                                    &self.label_list,
                                                                                    None,
                                                                                    None,
                                                                                    self.range_limit);
                self.collect_result_list(keyed_message_list,
                                         out_result_list,
                                         collector);
            }
            EdgeDirection::DIR_IN => {
                let mut keyed_message_list = HashMap::new();
                let mut partition_vertex_list = Vec::with_capacity(data.len());
                self.context.assign_empty_vertex_partition(&mut partition_vertex_list);

                for m in data.into_iter() {
                    self.context.assign_in_vertex_partition(m.get_id(), &mut partition_vertex_list);
                    let message = self.before_requirement.process_requirement(m);
                    keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                }
                if self.context.get_debug_flag() {
                    info!("get in vertices for {:?} with label list {:?} in task {:?}", &partition_vertex_list, &self.label_list, self.context.get_worker_index());
                }
                let in_result_list = self.global_graph.as_ref().get_in_vertex_ids(self.context.get_si(),
                                                                                  partition_vertex_list,
                                                                                  &self.label_list,
                                                                                  None,
                                                                                  None,
                                                                                  self.range_limit);

                self.collect_result_list(keyed_message_list,
                                         in_result_list,
                                         collector);
            }
            EdgeDirection::DIR_NONE => {
                let mut keyed_message_list = HashMap::new();

                let mut partition_out_vertex_list = Vec::with_capacity(data.len());
                let mut partition_in_vertex_list = Vec::with_capacity(data.len());
                self.context.assign_empty_vertex_partition(&mut partition_in_vertex_list);
                for m in data.into_iter() {
                    if let Some(partition_id) = self.context.get_partition_id(m.get_id()) {
                        self.context.assign_out_vertex_partition(m.get_id(), &mut partition_out_vertex_list);
                        self.context.assign_in_vertex_partition(m.get_id(), &mut partition_in_vertex_list);
                        let message = self.before_requirement.process_requirement(m);
                        keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                    }
                }

                if self.context.get_debug_flag() {
                    info!("get out vertices of both for {:?} with label list {:?} in task {:?}", &partition_out_vertex_list, &self.label_list, self.context.get_worker_index());
                }
                let out_result_list = self.global_graph.as_ref().get_out_vertex_ids(self.context.get_si(),
                                                                                    partition_out_vertex_list,
                                                                                    &self.label_list,
                                                                                    None,
                                                                                    None,
                                                                                    self.range_limit);
                self.collect_result_list(keyed_message_list.clone(),
                                         out_result_list,
                                         collector);

                if self.context.get_debug_flag() {
                    info!("get in vertices of both for {:?} with label list {:?} in task {:?}", &partition_in_vertex_list, &self.label_list, self.context.get_worker_index());
                }
                let in_result_list = self.global_graph.as_ref().get_in_vertex_ids(self.context.get_si(),
                                                                                  partition_in_vertex_list,
                                                                                  &self.label_list,
                                                                                  None,
                                                                                  None,
                                                                                  self.range_limit);

                self.collect_result_list(keyed_message_list,
                                         in_result_list,
                                         collector);
            }
        }
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct VertexDirectionEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    input_shuffle: StreamShuffleType<F>,
    stream_index: i32,
    direction: EdgeDirection,
    label_list: Vec<u32>,
    filter_manager: Arc<FilterManager>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    before_requirement: RequirementManager,
    after_requirement: Arc<RequirementManager>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}


impl<V, VI, E, EI, F> VertexDirectionEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               input_shuffle: StreamShuffleType<F>,
               direction: EdgeDirection,
               base: &OperatorBase,
               schema: Arc<Schema>,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self {
        let label_list = base.get_argument().get_int_value_list().iter().map(move |v| *v as u32).collect_vec();
        let filter_manager = Arc::new(FilterManager::new(base.get_logical_compare(),
                                                         schema));
        let range_limit = {
            if base.has_range_limit() {
                base.get_range_limit().get_range_end() as usize
            } else {
                0
            }
        };

        let dedup_manager = if let Some(manager) = parse_dedup_manager(base, context.get_debug_flag()) {
            Some(Arc::new(manager))
        } else {
            None
        };

        let before_requirement = RequirementManager::new(base.get_before_requirement().to_vec());
        let after_requirement = Arc::new(RequirementManager::new(base.get_after_requirement().to_vec()));

        VertexDirectionEdgeOperator {
            id: base.get_id(),
            input_id,
            input_shuffle,
            stream_index,
            direction,
            label_list,
            filter_manager,
            range_limit,
            dedup_manager,
            before_requirement,
            after_requirement,
            context,
            global_graph,
        }
    }

    fn collect_result_list<'a>(&self,
                               keyed_message_list: HashMap<i64, Vec<RawMessage>>,
                               result_list: Box<Iterator<Item=(i64, EI)>>,
                               collector: &mut Box<'a + MessageCollector>,
                               is_out: bool) {
        let mut result_iter_vec = Vec::with_capacity(keyed_message_list.len());
        for (src_id, vlist) in result_list {
            if let Some(message_list) = keyed_message_list.get(&src_id) {
                let result_iter = EdgeDirectionResultIterator::new(self.filter_manager.clone(),
                                                                   self.range_limit,
                                                                   self.dedup_manager.clone(),
                                                                   self.after_requirement.clone(),
                                                                   vlist,
                                                                   message_list.to_vec(),
                                                                   is_out);
                result_iter_vec.push(result_iter);
            }
        }
        let result_iter = IteratorList::new(result_iter_vec);
        collector.collect_iterator(Box::new(result_iter));
    }
}

impl<V, VI, E, EI, F> Operator for VertexDirectionEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for VertexDirectionEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> + Send,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.input_shuffle.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        match self.direction {
            EdgeDirection::DIR_OUT => {
                let mut keyed_message_list = HashMap::new();
                let mut partition_vertex_list = Vec::with_capacity(data.len());
                for m in data.into_iter() {
                    self.context.assign_out_vertex_partition(m.get_id(), &mut partition_vertex_list);
                    let message = self.before_requirement.process_requirement(m);
                    keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                }
                let out_result_list = self.global_graph.as_ref().get_out_edges(self.context.get_si(),
                                                                               partition_vertex_list,
                                                                               &self.label_list,
                                                                               None,
                                                                               None,
                                                                               None,
                                                                               self.range_limit);
                self.collect_result_list(keyed_message_list,
                                         out_result_list,
                                         collector,
                                         true);
            }
            EdgeDirection::DIR_IN => {
                let mut keyed_message_list = HashMap::new();
                let mut partition_vertex_list = Vec::with_capacity(data.len());
                self.context.assign_empty_vertex_partition(&mut partition_vertex_list);
                for m in data.into_iter() {
                    self.context.assign_in_vertex_partition(m.get_id(), &mut partition_vertex_list);
                    let message = self.before_requirement.process_requirement(m);
                    keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                }
                let in_result_list = self.global_graph.as_ref().get_in_edges(self.context.get_si(),
                                                                             partition_vertex_list,
                                                                             &self.label_list,
                                                                             None,
                                                                             None,
                                                                             None,
                                                                             self.range_limit);

                self.collect_result_list(keyed_message_list,
                                         in_result_list,
                                         collector,
                                         false);
            }
            EdgeDirection::DIR_NONE => {
                let mut keyed_message_list = HashMap::new();

                let mut partition_out_vertex_list = Vec::with_capacity(data.len());
                let mut partition_in_vertex_list = Vec::with_capacity(data.len());
                self.context.assign_empty_vertex_partition(&mut partition_in_vertex_list);
                for m in data.into_iter() {
                    self.context.assign_out_vertex_partition(m.get_id(), &mut partition_out_vertex_list);
                    self.context.assign_in_vertex_partition(m.get_id(), &mut partition_in_vertex_list);
                    let message = self.before_requirement.process_requirement(m);
                    keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                }

                let out_result_list = self.global_graph.as_ref().get_out_edges(self.context.get_si(),
                                                                               partition_out_vertex_list,
                                                                               &self.label_list,
                                                                               None,
                                                                               None,
                                                                               None,
                                                                               self.range_limit);
                self.collect_result_list(keyed_message_list.clone(),
                                         out_result_list,
                                         collector,
                                         true);

                let in_result_list = self.global_graph.as_ref().get_in_edges(self.context.get_si(),
                                                                             partition_in_vertex_list,
                                                                             &self.label_list,
                                                                             None,
                                                                             None,
                                                                             None,
                                                                             self.range_limit);

                self.collect_result_list(keyed_message_list,
                                         in_result_list,
                                         collector,
                                         false);
            }
        }
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

pub struct VertexDirectionCountOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    input_shuffle: StreamShuffleType<F>,
    stream_index: i32,
    direction: EdgeDirection,
    label_list: Vec<u32>,
    filter_manager: Arc<FilterManager>,
    before_requirement: RequirementManager,
    after_requirement: Arc<RequirementManager>,
    context: TaskContext,
    global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}


impl<V, VI, E, EI, F> VertexDirectionCountOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               input_shuffle: StreamShuffleType<F>,
               direction: EdgeDirection,
               base: &OperatorBase,
               schema: Arc<Schema>,
               context: TaskContext,
               global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>, ) -> Self {
        let label_list = base.get_argument().get_int_value_list().iter().map(move |v| *v as u32).collect_vec();
        let filter_manager = Arc::new(FilterManager::new(base.get_logical_compare(),
                                                         schema));
        let before_requirement = RequirementManager::new(base.get_before_requirement().to_vec());
        let after_requirement = Arc::new(RequirementManager::new(base.get_after_requirement().to_vec()));

        VertexDirectionCountOperator {
            id: base.get_id(),
            input_id,
            input_shuffle,
            stream_index,
            direction,
            label_list,
            filter_manager,
            before_requirement,
            after_requirement,
            context,
            global_graph,
        }
    }

    fn compute_count_list(&self, edge_list: Box<Iterator<Item=(i64, EI)>>, vertex_count_list: &mut HashMap<i64, i64>) {
        for (srcid, mut ei) in edge_list {
            let mut count = 0 as i64;
            while let Some(e) = ei.next() {
                if self.filter_manager.filter_native_edge(&e) {
                    count += 1;
                }
            }
            let curr_count = vertex_count_list.entry(srcid).or_insert(0);
            *curr_count += count;
        }
    }
}

impl<V, VI, E, EI, F> Operator for VertexDirectionCountOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for VertexDirectionCountOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.input_shuffle.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        let len = data.len();
        let (mut message_list, mut vertex_count_list) = {
            match self.direction {
                EdgeDirection::DIR_OUT => {
                    let mut keyed_message_list = HashMap::new();
                    let mut partition_vertex_list = Vec::with_capacity(data.len());
                    for m in data.into_iter() {
                        if let Some(partition_id) = self.context.get_partition_id(m.get_id()) {
                            self.context.assign_out_vertex_partition(m.get_id(), &mut partition_vertex_list);
                            let message = self.before_requirement.process_requirement(m);
                            keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                        }
                    }
                    let out_result_list = self.global_graph.as_ref().get_out_edges(self.context.get_si(),
                                                                                   partition_vertex_list,
                                                                                   &self.label_list,
                                                                                   None,
                                                                                   None,
                                                                                   None,
                                                                                   0);
                    let mut vertex_count_list = HashMap::new();
                    self.compute_count_list(out_result_list, &mut vertex_count_list);
                    (keyed_message_list, vertex_count_list)
                }
                EdgeDirection::DIR_IN => {
                    let mut keyed_message_list = HashMap::new();
                    let mut partition_vertex_list = Vec::with_capacity(data.len());
                    self.context.assign_empty_vertex_partition(&mut partition_vertex_list);
                    for m in data.into_iter() {
                        self.context.assign_in_vertex_partition(m.get_id(), &mut partition_vertex_list);
                        let message = self.before_requirement.process_requirement(m);
                        keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                    }
                    let in_result_list = self.global_graph.as_ref().get_in_edges(self.context.get_si(),
                                                                                 partition_vertex_list,
                                                                                 &self.label_list,
                                                                                 None,
                                                                                 None,
                                                                                 None,
                                                                                 0);
                    let mut vertex_count_list = HashMap::new();
                    self.compute_count_list(in_result_list, &mut vertex_count_list);

                    (keyed_message_list, vertex_count_list)
                }
                _ => {
                    let mut keyed_message_list = HashMap::new();

                    let mut partition_out_vertex_list = Vec::with_capacity(data.len());
                    let mut partition_in_vertex_list = Vec::with_capacity(data.len());
                    self.context.assign_empty_vertex_partition(&mut partition_in_vertex_list);
                    for m in data.into_iter() {
                        self.context.assign_out_vertex_partition(m.get_id(), &mut partition_out_vertex_list);
                        self.context.assign_in_vertex_partition(m.get_id(), &mut partition_in_vertex_list);
                        let message = self.before_requirement.process_requirement(m);
                        keyed_message_list.entry(message.get_id()).or_insert(vec![]).push(message);
                    }

                    let mut vertex_count_list = HashMap::new();
                    let out_result_list = self.global_graph.as_ref().get_out_edges(self.context.get_si(),
                                                                                   partition_out_vertex_list,
                                                                                   &self.label_list,
                                                                                   None,
                                                                                   None,
                                                                                   None,
                                                                                   0);
                    self.compute_count_list(out_result_list, &mut vertex_count_list);
                    let in_result_list = self.global_graph.as_ref().get_in_edges(self.context.get_si(),
                                                                                 partition_in_vertex_list,
                                                                                 &self.label_list,
                                                                                 None,
                                                                                 None,
                                                                                 None,
                                                                                 0);
                    self.compute_count_list(in_result_list, &mut vertex_count_list);

                    (keyed_message_list, vertex_count_list)
                }
            }
        };
        let mut result_list = Vec::with_capacity(len);
        for (mid, mlist) in message_list.drain() {
            let count = {
                if let Some(count_val) = vertex_count_list.remove(&mid) {
                    count_val
                } else { 0 }
            };
            for mut m in mlist.into_iter() {
                let mut result = RawMessage::from_value(ValuePayload::Long(count));
                self.after_requirement.process_take_extend_entity(&mut m, &mut result);
                result_list.push(self.after_requirement.process_requirement(result));
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}
