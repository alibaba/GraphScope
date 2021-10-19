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
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::message::RawMessage;
use dataflow::manager::filter::FilterManager;
use dataflow::manager::range::{RangeManager, parse_range_manager};
use dataflow::manager::dedup::{DedupManager, parse_dedup_manager};
use dataflow::manager::requirement::RequirementManager;

use maxgraph_common::proto::query_flow::OperatorBase;
use std::sync::Arc;
use maxgraph_store::api::graph_schema::Schema;

pub enum EdgeVertexType {
    OUTV,
    INV,
    OTHERV,
    BOTHV,
}

pub struct EdgeDirectionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    shuffle_type: StreamShuffleType<F>,
    stream_index: i32,
    op_type: EdgeVertexType,
    filter_manager: FilterManager,
    range_manager: Option<RangeManager>,
    dedup_manager: Option<DedupManager>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
}

impl<F> EdgeDirectionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               op_type: EdgeVertexType,
               base: &OperatorBase,
               schema: Arc<Schema>,
               debug_flag: bool) -> Self {
        let filter_manager = FilterManager::new(base.get_logical_compare(), schema);
        let range_manager = parse_range_manager(base);
        let dedup_manager = parse_dedup_manager(base, debug_flag);
        let before_requirement = RequirementManager::new(base.get_before_requirement().to_vec());
        let after_requirement = RequirementManager::new(base.get_after_requirement().to_vec());

        EdgeDirectionOperator {
            id: base.get_id(),
            input_id,
            shuffle_type,
            stream_index,
            op_type,
            filter_manager,
            range_manager,
            dedup_manager,
            before_requirement,
            after_requirement,
        }
    }
}

impl<F> Operator for EdgeDirectionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for EdgeDirectionOperator<F>
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
            match self.op_type {
                EdgeVertexType::OUTV |
                EdgeVertexType::INV |
                EdgeVertexType::OTHERV => {
                    if let Some(mut result) = {
                        if let Some(edge) = message.get_edge() {
                            match self.op_type {
                                EdgeVertexType::OUTV => {
                                    Some(RawMessage::from_vertex_id(edge.get_src_label(), edge.get_src_id()))
                                }
                                EdgeVertexType::INV => {
                                    Some(RawMessage::from_vertex_id(edge.get_dst_label(), edge.get_dst_id()))
                                }
                                EdgeVertexType::OTHERV => {
                                    // for other v
                                    if edge.get_is_out() {
                                        Some(RawMessage::from_vertex_id(edge.get_dst_label(), edge.get_dst_id()))
                                    } else {
                                        Some(RawMessage::from_vertex_id(edge.get_src_label(), edge.get_src_id()))
                                    }
                                }
                                _ => {
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    } {
                        if self.filter_manager.filter_message(&result) {
                            if let Some(ref mut dedup) = self.dedup_manager {
                                if !dedup.check_dedup(result.get_id()) {
                                    continue;
                                }
                            }
                            if let Some(ref mut range) = self.range_manager {
                                let bulk = message.get_bulk() as usize;
                                if range.range_finish() || range.range_filter_with_bulk(bulk) {
                                    continue;
                                }
                                let val = range.check_range_with_bulk(bulk);
                                message.update_with_bulk(val as i64);
                            }
                            self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                            result_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                }
                EdgeVertexType::BOTHV => {
                    let curr_result_list = {
                        if let Some(edge) = message.get_edge() {
                            vec![RawMessage::from_vertex_id(edge.get_src_label(), edge.get_src_id()),
                                 RawMessage::from_vertex_id(edge.get_dst_label(), edge.get_dst_id())]
                        } else {
                            vec![]
                        }
                    };
                    let mut message_list = vec![];
                    for mut result in curr_result_list {
                        if self.filter_manager.filter_message(&result) {
                            if let Some(ref mut dedup) = self.dedup_manager {
                                if !dedup.check_dedup(result.get_id()) {
                                    continue;
                                }
                            }
                            if let Some(ref mut range) = self.range_manager {
                                let bulk = message.get_bulk() as usize;
                                if range.range_finish() || range.range_filter_with_bulk(bulk) {
                                    continue;
                                }
                                let val = range.check_range_with_bulk(bulk);
                                message.update_with_bulk(val as i64);
                            }
                            self.after_requirement.process_extend_entity(&message, &mut result);
                            message_list.push(self.after_requirement.process_requirement(result));
                        }
                    }
                    result_list.append(&mut message_list);
                }
            }
        }
        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}
