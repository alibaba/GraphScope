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
use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::proto::message::DfsCommand;

use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraEntryEntity};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::range::parse_range_manager;
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, SourceOperator, MessageCollector};

use protobuf::parse_from_bytes;
use protobuf::Message;
use dataflow::operator::sourcestep::vertex::{SourceVertexOperator, SourceVertexIdOperator};

pub struct DfsRepeatGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    data_iter: Box<Iterator<Item=RawMessage> + Send>,
    debug_flag: bool,
}

impl<F> DfsRepeatGraphOperator<F>
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
        let label_list = base.get_argument().get_int_value_list().to_vec().into_iter().map(move |v| v as u32).collect();
        let id_list = base.get_argument().get_long_value_list().to_vec();
        let filter_manager = FilterManager::new(base.get_logical_compare(),
                                                context.get_schema().clone());
        let range_manager = parse_range_manager(base);
        let after_requirement = RequirementManager::new(base.get_after_requirement().to_vec());

        let exec_local_flag = context.get_exec_local_flag();
        let partition_manager = context.get_graph_partition_manager();
        let worker_index = context.get_index();
        let task_context = TaskContext::new(worker_index as u32,
                                            context.get_snapshot_id(),
                                            partition_manager.clone(),
                                            context.get_partition_ids().as_ref().to_vec(),
                                            exec_local_flag,
                                            context.get_debug_flag());

        let data_iter = {
            if id_list.is_empty() {
                let mut source = SourceVertexOperator::new(
                    base.get_id(),
                    task_context,
                    label_list,
                    filter_manager,
                    range_manager,
                    after_requirement,
                    context.get_store().clone());
                source.execute()
            } else {
                let mut source = SourceVertexIdOperator::new(base.get_id(),
                                                             task_context,
                                                             label_list,
                                                             id_list,
                                                             filter_manager,
                                                             range_manager,
                                                             after_requirement,
                                                             context.get_store().clone());
                source.execute()
            }
        };

        return DfsRepeatGraphOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            data_iter,
            debug_flag: context.get_debug_flag(),
        };
    }
}

impl<F> Operator for DfsRepeatGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for DfsRepeatGraphOperator<F>
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
        for message in data.into_iter() {
            match message.get_message_type() {
                RawMessageType::DFSCMD => {
                    if let Some(value) = message.get_value() {
                        let dfs_command = parse_from_bytes::<DfsCommand>(value.get_bytes().unwrap()).expect("parse dfs command");
                        let batch_size = dfs_command.get_batch_size();
                        let mut result_list = Vec::with_capacity(batch_size as usize);
                        let mut count = 0_i64;
                        while count < batch_size {
                            if let Some(v) = self.data_iter.next() {
                                result_list.push(v);
                            } else {
                                break;
                            }
                            count += 1;
                        }
                        if count > 0 {
                            if self.debug_flag {
                                info!("deal dfs command {:?} and generate {:?} result message", &dfs_command, result_list.len());
                            }
                            let cmd_message = RawMessage::from_value_type(ValuePayload::Bytes(dfs_command.write_to_bytes().unwrap()), RawMessageType::DFSCMD);
                            let result_list_message = RawMessage::from_value_type(ValuePayload::List(result_list), RawMessageType::LIST);
                            collector.collect_iterator(Box::new(Some(RawMessage::from_value_type(
                                ValuePayload::Entry(ExtraEntryEntity::new(cmd_message, result_list_message)),
                                RawMessageType::ENTRY)).into_iter()));
                        } else {
                            if self.debug_flag {
                                info!("deal dfs command finish for no data generated");
                            }
                        }
                    } else {
                        error!("deal dfs command fail {:?}", &message);
                    }
                }
                _ => {
                    error!("receive invalid message {:?} not dfs command", &message);
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct DfsRepeatCmdOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
}

impl<F> DfsRepeatCmdOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        return DfsRepeatCmdOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
        };
    }
}

impl<F> Operator for DfsRepeatCmdOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for DfsRepeatCmdOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(value) = message.take_value() {
                if let Ok(dfs_cmd_data) = value.take_entry() {
                    collector.collect_iterator(Box::new(Some(dfs_cmd_data.take_key()).into_iter()));
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct DfsRepeatDataOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
}

impl<F> DfsRepeatDataOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        return DfsRepeatDataOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
        };
    }
}

impl<F> Operator for DfsRepeatDataOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for DfsRepeatDataOperator<F>
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
        for mut message in data.into_iter() {
            if let Some(value) = message.take_value() {
                if let Ok(dfs_cmd_data) = value.take_entry() {
                    let mut list_value_message = dfs_cmd_data.take_value();
                    if let Some(value_list) = list_value_message.take_value() {
                        if let Ok(value_list_result) = value_list.take_list() {
                            collector.collect_iterator(Box::new(value_list_result.into_iter()));
                        }
                    }
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}
