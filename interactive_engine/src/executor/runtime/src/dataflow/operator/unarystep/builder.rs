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

#![allow(unused_variables)]

use maxgraph_common::proto::query_flow::*;
use maxgraph_common::proto::message::{EdgeDirection, CompareType, MetapathSampleArg};
use maxgraph_store::api::prelude::{Vertex, Edge};

use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::lambda::*;
use dataflow::builder::UnaryOperator;
use dataflow::operator::shuffle::*;
use dataflow::operator::unary::build_unary_operator_node;
use dataflow::operator::unarystep::vertex::*;
use dataflow::operator::unarystep::edge::*;
use dataflow::operator::unarystep::object::*;
use dataflow::operator::unarystep::aggregate::*;
use dataflow::operator::unarystep::property::*;
use dataflow::operator::unarystep::filter::*;
use dataflow::operator::unarystep::select::*;
use dataflow::operator::unarystep::dedup::*;
use dataflow::operator::unarystep::order::{OrderOperator, OrderLocalOperator};
use dataflow::operator::unarystep::chain::UnaryChainOperator;

use std::sync::Arc;
use protobuf::{ProtobufEnum, parse_from_bytes};
use std::cell::RefCell;
use std::rc::Rc;
use dataflow::operator::unarystep::dfs::{DfsRepeatGraphOperator, DfsRepeatCmdOperator, DfsRepeatDataOperator};
use dataflow::operator::unarystep::subgraph::{SubGraphOperator, CacheOperator};
use dataflow::operator::unarystep::output::WriteOdpsOperator;
use dataflow::operator::unarystep::limitstop::GlobalLimitStopOperator;
use std::collections::HashMap;
use dataflow::operator::unarystep::enterkey::{EnterKeyOperator, ByKeyEntryOperator, KeyMessageOperator};
use dataflow::operator::unarystep::sample::MetapathSampleOperator;
use dataflow::operator::unarystep::barrier::BarrierOperator;
use std::path::PathBuf;
use dataflow::manager::lambda::LambdaManager;
use dataflow::operator::unarystep::lambda::LambdaOperator;
use maxgraph_store::api::MVGraph;
use dataflow::operator::unarystep::vineyard::VineyardStreamOperator;
use dataflow::operator::unarystep::vineyard_writer::{VineyardWriteVertexOperator, VineyardWriteEdgeOperator};

pub fn build_unary_chain_operator<V, VI, E, EI, F>(input_id: i32,
                                                   stream_index: i32,
                                                   shuffle_type: &InputEdgeShuffle,
                                                   base: &OperatorBase,
                                                   query_id: &str,
                                                   script: &str,
                                                   context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let unary_chain_shuffle = {
        match shuffle_type.get_shuffle_type() {
            InputShuffleType::FORWARD_TYPE => {
                StreamShuffleCompositeType::composite(Some(StreamShuffleType::forward()), None)
            }
            InputShuffleType::SHUFFLE_BY_ID_TYPE => {
                StreamShuffleCompositeType::composite(Some(StreamShuffleType::exchange(context.unwrap().get_route().clone(),
                                                                                       shuffle_type.get_shuffle_value())),
                                                      None)
            }
            InputShuffleType::SHUFFLE_BY_KEY_TYPE => {
                StreamShuffleCompositeType::composite(None, Some(StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(),
                                                                                                shuffle_type.get_shuffle_value())))
            }
            InputShuffleType::SHUFFLE_BY_CONST_TYPE => {
                StreamShuffleCompositeType::composite(Some(StreamShuffleType::constant()), None)
            }
            InputShuffleType::BROADCAST_TYPE => {
                StreamShuffleCompositeType::composite(Some(StreamShuffleType::broadcast()), None)
            }
        }
    };
    let mut chain_operator_list = vec![];
    let mut forward_edge_shuffle = InputEdgeShuffle::new();
    forward_edge_shuffle.set_shuffle_type(InputShuffleType::FORWARD_TYPE);
    for chain_function in base.get_chained_function() {
        if let Some(unary_op) = build_unary_operator_node(chain_function,
                                                          query_id,
                                                          script,
                                                          context,
                                                          input_id,
                                                          stream_index,
                                                          &forward_edge_shuffle,
                                                          chain_function.get_operator_type()) {
            chain_operator_list.push(unary_op);
        } else {
            error!("create unary operator {:?} in chain fail", chain_function.get_operator_type());
        }
    }
    return Some(Box::new(UnaryChainOperator::new(input_id,
                                                 stream_index,
                                                 unary_chain_shuffle,
                                                 base,
                                                 chain_operator_list)));
}

pub fn build_vertex_direction_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    _shuffle_type: &InputEdgeShuffle,
    direction: EdgeDirection,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_disable = base.get_argument().get_exec_local_disable();
    let exec_local_flag = context.unwrap().get_exec_local_flag() && !exec_local_disable;
    let input_shuffle = {
        if exec_local_flag {
            StreamShuffleType::forward()
        } else {
            match direction {
                EdgeDirection::DIR_OUT => {
                    StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
                }
                _ => StreamShuffleType::broadcast(),
            }
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(VertexDirectionOperator::new(input_id,
                                                      stream_index,
                                                      input_shuffle,
                                                      direction,
                                                      base,
                                                      context.unwrap().get_schema().clone(),
                                                      task_context,
                                                      context.unwrap().get_store().clone())));
}

pub fn build_vertex_direction_edge_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    _shuffle_type: &InputEdgeShuffle,
    direction: EdgeDirection,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.unwrap().get_exec_local_flag() && !base.get_argument().get_exec_local_disable();
    let input_shuffle = {
        if exec_local_flag {
            StreamShuffleType::forward()
        } else {
            match direction {
                EdgeDirection::DIR_OUT => StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0),
                _ => StreamShuffleType::broadcast(),
            }
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(VertexDirectionEdgeOperator::new(input_id,
                                                          stream_index,
                                                          input_shuffle,
                                                          direction,
                                                          base,
                                                          context.unwrap().get_schema().clone(),
                                                          task_context,
                                                          context.unwrap().get_store().clone())));
}

pub fn build_edge_direction_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    _shuffle_type: &InputEdgeShuffle,
    direction: EdgeVertexType,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(EdgeDirectionOperator::new(input_id,
                                                    stream_index,
                                                    input_shuffle,
                                                    direction,
                                                    base,
                                                    context.unwrap().get_schema().clone(),
                                                    context.unwrap().get_debug_flag())));
}

pub fn build_vertex_direction_count_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    _shuffle_type: &InputEdgeShuffle,
    direction: EdgeDirection,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.unwrap().get_exec_local_flag();
    let input_shuffle = {
        if exec_local_flag {
            StreamShuffleType::forward()
        } else {
            match direction {
                EdgeDirection::DIR_OUT => StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0),
                _ => StreamShuffleType::broadcast(),
            }
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(VertexDirectionCountOperator::new(input_id,
                                                           stream_index,
                                                           input_shuffle,
                                                           direction,
                                                           base,
                                                           context.unwrap().get_schema().clone(),
                                                           task_context,
                                                           context.unwrap().get_store().clone())));
}

pub fn build_count_local_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(CountLocalOperator::new(base.get_id(), input_id, stream_index, input_shuffle, base)));
}

pub fn build_count_operator(input_id: i32,
                            stream_index: i32,
                            base: &OperatorBase) -> Option<Box<UnaryOperator>> {
    let input_shuffle = StreamShuffleType::exchange(
        Arc::new(move |_val: &i64| {
            0 as u64
        }), 0);
    return Some(Box::new(CountGlobalOperator::new(base.get_id(), input_id, stream_index, input_shuffle, base)));
}

pub fn build_group_count_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0);
    return Some(Box::new(GroupCountOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_count_by_key_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(), 0);
    return Some(Box::new(CountByKeyOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_select_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(SelectOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_select_one_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(SelectOneOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_unfold_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(UnfoldOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_prop_value_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let remote_graph_flag = context.unwrap().get_exec_local_flag();
    let input_shuffle = {
        if base.get_argument().get_bool_flag() || remote_graph_flag {
            StreamShuffleType::forward()
        } else {
            StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        remote_graph_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(PropValueOperator::new(input_id,
                                                stream_index,
                                                input_shuffle,
                                                base,
                                                context.unwrap().get_schema().clone(),
                                                task_context,
                                                context.unwrap().get_store().clone())));

}

pub fn build_prop_key_value_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(PropKeyValueOperator::new(input_id, stream_index, input_shuffle, base, context.unwrap())));
}

pub fn build_prop_map_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let remote_graph_flag = context.unwrap().get_exec_local_flag();
    let prop_local = base.get_argument().get_bool_flag();
    let input_shuffle = {
        if prop_local || remote_graph_flag {
            StreamShuffleType::forward()
        } else {
            StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        remote_graph_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(PropMapOperator::new(input_id,
                                              stream_index,
                                              input_shuffle,
                                              prop_local,
                                              base,
                                              context.unwrap().get_schema().clone(),
                                              task_context,
                                              context.unwrap().get_store().clone())));
}

pub fn build_prop_fill_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let remote_graph_flag = context.unwrap().get_exec_local_flag();
    let input_shuffle = {
        if remote_graph_flag {
            StreamShuffleType::forward()
        } else {
            StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        remote_graph_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(PropFillOperator::new(input_id,
                                               stream_index,
                                               input_shuffle,
                                               base,
                                               task_context,
                                               context.unwrap().get_store().clone())));
}

pub fn build_filter_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let filter_manager = FilterManager::new(base.get_logical_compare(),
                                            context.unwrap().get_schema().clone());
    let exec_local_flag = context.unwrap().get_exec_local_flag();
    let shuffle_bool_value = base.get_argument().get_bool_value();
    let (shuffle_flag, input_shuffle) = {
        if exec_local_flag {
            (!filter_manager.get_related_propid_list().is_empty(), StreamShuffleType::forward())
        } else if shuffle_bool_value || filter_manager.get_related_propid_list().is_empty() {
            (false, StreamShuffleType::forward())
        } else {
            (true, StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0))
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(FilterOperator::new(base,
                                             input_id,
                                             stream_index,
                                             input_shuffle,
                                             shuffle_flag,
                                             task_context,
                                             filter_manager,
                                             context.unwrap().get_store().clone())));

}

pub fn build_lambda_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    query_id: &str,
    script: &str,
    lambda_type: LambdaType,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let lambda_index = base.get_argument().get_str_value();
    let shuffle_bool_value = base.get_argument().get_bool_value();
    let (shuffle_flag, input_shuffle) = {
        if shuffle_bool_value {
            (false, StreamShuffleType::forward())
        } else {
            (true, StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0))
        }
    };

    return Some(Box::new(LambdaOperator::new(base, input_id, stream_index, input_shuffle,
                                             shuffle_flag, context.unwrap(),
                                             context.unwrap().get_lambda_manager().clone(), lambda_index.to_string(), lambda_type)));
}

pub fn build_where_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();

    let values = base.get_argument().get_int_value_list();
    let compare_type = CompareType::from_i32(values[0]).unwrap();
    let label_id = values[1];
    return Some(Box::new(WhereOperator::new(input_id, stream_index, input_shuffle, base, 0, label_id, compare_type)));
}

pub fn build_where_label_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    let values = base.get_argument().get_int_value_list();
    let compare_type = CompareType::from_i32(values[0]).unwrap();
    let start_label_id = values[1];
    let label_id = values[2];
    return Some(Box::new(WhereOperator::new(input_id, stream_index, input_shuffle, base, start_label_id, label_id, compare_type)));
}

pub fn build_simple_path_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(SimplePathOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_path_out_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(PathOutOperator::new(input_id, stream_index, input_shuffle, base, context.unwrap().get_schema().clone())));
}

pub fn build_dedup_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let prop_id = base.get_argument().get_int_value();
    let input_shuffle = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(),
                                                       prop_id);
    return Some(Box::new(DedupOperator::new(input_id, stream_index, input_shuffle, base, prop_id)));
}

pub fn build_combiner_range_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let early_stop_argument = base.get_early_stop_argument();
    let input_shuffle: StreamShuffleCompositeType<F> = StreamShuffleCompositeType::composite(Some(StreamShuffleType::forward()),
                                                                                             None);
    let range_operator = Box::new(RangeOperator::new(input_id, stream_index, input_shuffle, base));
    if early_stop_argument.get_global_stop_flag() {
        return Some(Box::new(GlobalLimitStopOperator::new(range_operator,
                                                          context.unwrap().get_early_stop_state().clone())));
    } else {
        return Some(range_operator);
    }
}

pub fn build_range_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleCompositeType::composite(None,
                                                              Some(StreamShuffleKeyType::constant(context.unwrap().get_route().clone())));
    return Some(Box::new(RangeOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_range_local_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(RangeLocalOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_fold_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(FoldOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_fold_store_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(FoldStoreOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_foldmap_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(FoldMapOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_order_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(OrderOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_order_local_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(OrderLocalOperator::new(input_id, stream_index, input_shuffle, base)));
}

pub fn build_column_operator<V, VI, E, EI, F>(
    input_id: i32,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_shuffle: StreamShuffleType<F> = StreamShuffleType::forward();

    let argument = base.get_argument();
    let mut label_id_name_list = HashMap::new();
    let int_value_list = argument.get_int_value_list();
    let str_value_list = argument.get_str_value_list();
    for i in 0..int_value_list.len() {
        let label_id = int_value_list.get(i).unwrap().clone();
        let label_name = str_value_list.get(i).unwrap().clone();
        label_id_name_list.insert(label_id, label_name);
    }

    return Some(Box::new(ColumnOperator::new(input_id,
                                             stream_index,
                                             input_shuffle,
                                             base,
                                             label_id_name_list)));
}

pub fn build_label_value_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    query_id: &str,
    script: &str,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let label_id = base.get_argument().get_int_value();
    let input_shuffle = StreamShuffleType::exchange(context.unwrap().get_route().clone(),
                                                    label_id);
    let op_base_payload = base.get_argument().get_payload();
    let mut forward_edge_shuffle = InputEdgeShuffle::new();
    forward_edge_shuffle.set_shuffle_type(InputShuffleType::FORWARD_TYPE);
    let label_value_operator = {
        if op_base_payload.is_empty() {
            None
        } else {
            let label_value_base = parse_from_bytes::<OperatorBase>(op_base_payload).expect("parse label value operator");
            build_unary_operator_node(&label_value_base,
                                      query_id,
                                      script,
                                      context,
                                      0,
                                      0,
                                      &forward_edge_shuffle,
                                      label_value_base.get_operator_type())
        }
    };
    return Some(Box::new(LabelValueOperator::new(input_id,
                                                 stream_index,
                                                 input_shuffle,
                                                 base,
                                                 label_value_operator)));
}

pub fn build_enter_key_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.unwrap().get_exec_local_flag();
    let enter_key_argument = parse_from_bytes::<EnterKeyArgumentProto>(base.get_argument().get_payload())
        .expect("parse enter key argument");
    let shuffle_type = {
        if exec_local_flag {
            StreamShuffleType::forward()
        } else {
            match enter_key_argument.get_enter_key_type() {
                EnterKeyTypeProto::KEY_PROP_LABEL => {
                    if enter_key_argument.get_prop_label_id() > 0 {
                        StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
                    } else {
                        StreamShuffleType::forward()
                    }
                }
                EnterKeyTypeProto::KEY_PROP_VAL_MAP => {
                    StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
                }
                _ => {
                    StreamShuffleType::forward()
                }
            }
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(EnterKeyOperator::new(base,
                                               input_id,
                                               stream_index,
                                               context.unwrap().get_schema().clone(),
                                               task_context,
                                               shuffle_type,
                                               enter_key_argument,
                                               context.unwrap().get_store().clone())));
}

pub fn build_bykey_entry_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(ByKeyEntryOperator::new(base,
                                                 input_id,
                                                 stream_index,
                                                 shuffle_type)));
}

pub fn build_key_message_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(KeyMessageOperator::new(base,
                                                 input_id,
                                                 stream_index,
                                                 shuffle_type)));
}

pub fn build_combiner_sum_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    let shuffle_composite_type = StreamShuffleCompositeType::composite(Some(shuffle_type), None);
    return Some(Box::new(SumOperator::new(base,
                                          input_id,
                                          stream_index,
                                          shuffle_composite_type)));
}

pub fn build_sum_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    let shuffle_composite_type = StreamShuffleCompositeType::composite(None, Some(shuffle_type));
    return Some(Box::new(SumOperator::new(base,
                                          input_id,
                                          stream_index,
                                          shuffle_composite_type)));
}

pub fn build_max_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(MaxOperator::new(base,
                                          input_id,
                                          stream_index,
                                          shuffle_type)));
}

pub fn build_min_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    return Some(Box::new(MinOperator::new(base,
                                          input_id,
                                          stream_index,
                                          shuffle_type)));
}

pub fn build_constant_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(ConstantOperator::new(input_id,
                                               stream_index,
                                               shuffle_type,
                                               base)));
}

pub fn build_properties_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let prop_local_flag = base.get_argument().get_bool_flag();
    let shuffle_type = {
        if prop_local_flag {
            StreamShuffleType::forward()
        } else {
            StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0)
        }
    };

    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        prop_local_flag,
                                        context.unwrap().get_debug_flag());
    return Some(Box::new(PropertiesOperator::new(input_id,
                                                 stream_index,
                                                 shuffle_type,
                                                 base,
                                                 prop_local_flag,
                                                 task_context,
                                                 context.unwrap().get_store().clone())));
}

pub fn build_dfs_repeat_graph_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let vertex_flag = base.get_argument().get_bool_value();
    if vertex_flag {
        let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
        let dfs_repeat_operator = DfsRepeatGraphOperator::new(input_id,
                                                              stream_index,
                                                              shuffle_type,
                                                              base,
                                                              context.unwrap());
        return Some(Box::new(dfs_repeat_operator));
    } else {
        error!("not support to read and send edge in dfs yet.");
        return None;
    }
}

pub fn build_dfs_repeat_cmd_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    let dfs_repeat_operator = DfsRepeatCmdOperator::new(input_id,
                                                        stream_index,
                                                        shuffle_type,
                                                        base);
    return Some(Box::new(dfs_repeat_operator));
}

pub fn build_dfs_repeat_data_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    let dfs_repeat_operator = DfsRepeatDataOperator::new(input_id,
                                                         stream_index,
                                                         shuffle_type,
                                                         base);
    return Some(Box::new(dfs_repeat_operator));
}

pub fn build_subgraph_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    let vertex_flag = base.get_argument().get_bool_flag();
    return Some(Box::new(SubGraphOperator::new(input_id,
                                               stream_index,
                                               shuffle_type,
                                               base,
                                               context.unwrap().get_subgraph().clone(),
                                               vertex_flag,
                                               context.unwrap().get_debug_flag())));
}

pub fn build_cache_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0);
    return Some(Box::new(CacheOperator::new(input_id,
                                            stream_index,
                                            shuffle_type,
                                            base,
                                            context.unwrap().get_cache_store().clone())));
}

pub fn build_write_odps_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0);
    return Some(Box::new(WriteOdpsOperator::new(input_id,
                                                stream_index,
                                                shuffle_type,
                                                base)));
}

pub fn build_sample_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type = StreamShuffleKeyType::constant(context.unwrap().get_route().clone());
    let amount_sample = base.get_argument().get_int_value();
    return Some(Box::new(MetapathSampleOperator::new(base,
                                                     input_id,
                                                     stream_index,
                                                     shuffle_type,
                                                     amount_sample)));
}

pub fn build_barrier_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::forward();
    let barrier_size = base.get_argument().get_int_value();
    return Some(Box::new(BarrierOperator::new(base,
                                              input_id,
                                              stream_index,
                                              shuffle_type,
                                              barrier_size)));
}

pub fn build_vineyard_stream_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let graph_name = base.get_argument().get_str_value().to_owned();
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::constant();
    let stream_operator = VineyardStreamOperator::new(base.get_id(),
                                                      input_id,
                                                      shuffle_type,
                                                      stream_index,
                                                      graph_name);
    return Some(Box::new(stream_operator));
}

pub fn build_vineyard_output_vertex_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.unwrap().get_exec_local_flag();
    let graph_name = base.get_argument().get_str_value().to_owned();
    let partition_manager = context.unwrap().get_graph_partition_manager();
    let worker_index = context.unwrap().get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.unwrap().get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.unwrap().get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.unwrap().get_debug_flag());
    let shuffle_type = StreamShuffleType::exchange(context.unwrap().get_route().clone(), 0);
    return Some(Box::new(VineyardWriteVertexOperator::new(base.get_id(),
                                                          input_id,
                                                          shuffle_type,
                                                          stream_index,
                                                          context.unwrap().get_store().clone(),
                                                          graph_name,
                                                          task_context)));
}

pub fn build_vineyard_output_edge_operator<V, VI, E, EI, F>(
    input_id: i32,
    _input_shuffle_type: &InputEdgeShuffle,
    stream_index: i32,
    base: &OperatorBase,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.unwrap().get_exec_local_flag();
    let graph_name = base.get_argument().get_str_value().to_owned();
    let worker_index = context.unwrap().get_index();
    let shuffle_type: StreamShuffleType<F> = StreamShuffleType::broadcast();
    return Some(Box::new(VineyardWriteEdgeOperator::new(base.get_id(),
                                                        input_id,
                                                        shuffle_type,
                                                        stream_index,
                                                        context.unwrap().get_store().clone(),
                                                        graph_name,
                                                        worker_index as i32,
                                                        context.unwrap().get_subgraph().clone(),
                                                        context.unwrap().get_debug_flag())));
}
