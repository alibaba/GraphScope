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

use maxgraph_store::api::prelude::{Vertex, Edge};
use maxgraph_common::proto::query_flow;
use dataflow::builder::BinaryOperator;
use dataflow::manager::context::RuntimeContext;
use dataflow::operator::binarystep::join::*;
use dataflow::operator::shuffle::{StreamShuffleType, StreamShuffleKeyType};

pub fn build_union_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_forward = StreamShuffleType::<F>::forward();
    let right_forward = StreamShuffleType::forward();
    return Some(Box::new(UnionOperator::new(operator.get_base(),
                                            operator.get_left_input_operator_id(),
                                            operator.get_left_stream_index(),
                                            left_forward,
                                            operator.get_right_input_operator_id(),
                                            operator.get_right_stream_index(),
                                            right_forward)));
}

pub fn build_direct_filter_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_exchange = StreamShuffleKeyType::exchange(
        context.unwrap().get_route().clone(),
        0);
    let right_exchange = StreamShuffleKeyType::exchange(
        context.unwrap().get_route().clone(),
        0);
    return Some(Box::new(DirectFilterOperator::new(operator.get_base(),
                                                   operator.get_left_input_operator_id(),
                                                   operator.get_left_stream_index(),
                                                   left_exchange,
                                                   operator.get_right_input_operator_id(),
                                                   operator.get_right_stream_index(),
                                                   right_exchange)));
}

pub fn build_direct_filter_negate_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_exchange = StreamShuffleKeyType::exchange(
        context.unwrap().get_route().clone(),
        0);
    let right_exchange = StreamShuffleKeyType::exchange(
        context.unwrap().get_route().clone(),
        0);
    return Some(Box::new(DirectFilterNegateOperator::new(operator.get_base(),
                                                         operator.get_left_input_operator_id(),
                                                         operator.get_left_stream_index(),
                                                         left_exchange,
                                                         operator.get_right_input_operator_id(),
                                                         operator.get_right_stream_index(),
                                                         right_exchange)));
}

pub fn build_join_store_filter_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    _context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {

    let left_forward = StreamShuffleType::<F>::forward();
    let right_broadcast = StreamShuffleType::broadcast();
    return Some(Box::new(JoinStoreFilterOperator::new(operator.get_base(),
                                                      operator.get_left_input_operator_id(),
                                                      operator.get_left_stream_index(),
                                                      left_forward,
                                                      operator.get_right_input_operator_id(),
                                                      operator.get_right_stream_index(),
                                                      right_broadcast)));
}

pub fn build_right_zero_join<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {

    let left_exchange = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(), 0);
    let right_forward  = StreamShuffleType::forward();
    return Some(Box::new(RightZeroJoinOperator::new(operator.get_base(),
                                                    operator.get_left_input_operator_id(),
                                                    operator.get_left_stream_index(),
                                                    left_exchange,
                                                    operator.get_right_input_operator_id(),
                                                    operator.get_right_stream_index(),
                                                    right_forward)));
}

pub fn build_join_label_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_exchange = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(), 0);
    let right_forward: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(JoinLabelOperator::new(operator.get_base(),
                                                operator.get_left_input_operator_id(),
                                                operator.get_left_stream_index(),
                                                left_exchange,
                                                operator.get_right_input_operator_id(),
                                                operator.get_right_stream_index(),
                                                right_forward,
                                                false)));
}

pub fn build_join_count_label_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_exchange = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(), 0);
    let right_forward: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(JoinLabelOperator::new(operator.get_base(),
                                                operator.get_left_input_operator_id(),
                                                operator.get_left_stream_index(),
                                                left_exchange,
                                                operator.get_right_input_operator_id(),
                                                operator.get_right_stream_index(),
                                                right_forward,
                                                true)));
}

pub fn build_join_right_value_key_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_exchange = StreamShuffleKeyType::exchange(context.unwrap().get_route().clone(), 0);
    let right_forward: StreamShuffleType<F> = StreamShuffleType::forward();
    return Some(Box::new(JoinRightValueKeyOperator::new(operator.get_base(),
                                                        operator.get_left_input_operator_id(),
                                                        operator.get_left_stream_index(),
                                                        left_exchange,
                                                        operator.get_right_input_operator_id(),
                                                        operator.get_right_stream_index(),
                                                        right_forward)));
}

pub fn build_dfs_finish_join_operator<V, VI, E, EI, F>(
    operator: &query_flow::BinaryOperator,
    context: Option<&RuntimeContext<V, VI, E, EI, F>>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let left_forward: StreamShuffleType<F> = StreamShuffleType::forward();
    let right_broadcast: StreamShuffleType<F> = StreamShuffleType::broadcast();
    return Some(Box::new(DfsFinishJoinOperator::new(operator.get_base(),
                                                    operator.get_left_input_operator_id(),
                                                    operator.get_left_stream_index(),
                                                    left_forward,
                                                    operator.get_right_input_operator_id(),
                                                    operator.get_right_stream_index(),
                                                    right_broadcast,
                                                    context.unwrap().get_debug_flag())));
}

