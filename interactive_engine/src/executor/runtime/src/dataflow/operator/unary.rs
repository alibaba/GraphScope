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

use maxgraph_common::proto::query_flow;
use maxgraph_common::proto::message::EdgeDirection;

use maxgraph_store::api::{Vertex, Edge};

use dataflow::manager::context::RuntimeContext;
use dataflow::builder::UnaryOperator;
use dataflow::operator::unarystep::builder::*;
use dataflow::operator::unarystep::edge::EdgeVertexType;
use maxgraph_common::proto::query_flow::*;
use dataflow::operator::unarystep::limitstop::GlobalStopFilterOperator;
use dataflow::manager::lambda::LambdaType;

pub fn build_unary_operator<V, VI, E, EI, F>(unary: &query_flow::UnaryOperator,
                                             query_id: &str,
                                             script: &str,
                                             context: &RuntimeContext<V, VI, E, EI, F>) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let input_id = unary.get_input_operator_id();
    let stream_index = unary.get_input_stream_index();
    let shuffle_type = unary.get_input_shuffle();
    let base = unary.get_base();
    let operator_type = base.get_operator_type();

    let unary_op = build_unary_operator_node(base, query_id, script, Some(context), input_id, stream_index, shuffle_type, operator_type);
    let early_stop_argument = base.get_early_stop_argument();
    if early_stop_argument.get_global_filter_flag() {
        if unary_op.is_some() {
            return Some(Box::new(GlobalStopFilterOperator::new(
                unary_op.unwrap(),
                context.get_early_stop_state().clone())));
        } else {
            return None;
        }
    } else {
        return unary_op;
    }
}

pub fn build_unary_operator_node<V, VI, E, EI, F>(base: &OperatorBase,
                                                  query_id: &str,
                                                  script: &str,
                                                  context: Option<&RuntimeContext<V, VI, E, EI, F>>,
                                                  input_id: i32,
                                                  stream_index: i32,
                                                  shuffle_type: &InputEdgeShuffle,
                                                  operator_type: OperatorType) -> Option<Box<UnaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    match operator_type {
        query_flow::OperatorType::UNARY_CHAIN => {
            return build_unary_chain_operator(input_id,
                                              stream_index,
                                              shuffle_type,
                                              base,
                                              query_id,
                                              script,
                                              context);
        }
        query_flow::OperatorType::OUT => {
            return build_vertex_direction_operator(input_id,
                                                   stream_index,
                                                   shuffle_type,
                                                   EdgeDirection::DIR_OUT,
                                                   base,
                                                   context);
        }
        query_flow::OperatorType::IN => {
            return build_vertex_direction_operator(input_id,
                                                   stream_index,
                                                   shuffle_type,
                                                   EdgeDirection::DIR_IN,
                                                   base,
                                                   context);
        }
        query_flow::OperatorType::BOTH => {
            return build_vertex_direction_operator(input_id,
                                                   stream_index,
                                                   shuffle_type,
                                                   EdgeDirection::DIR_NONE,
                                                   base,
                                                   context);
        }
        query_flow::OperatorType::OUT_E => {
            return build_vertex_direction_edge_operator(input_id,
                                                        stream_index,
                                                        shuffle_type,
                                                        EdgeDirection::DIR_OUT,
                                                        base,
                                                        context);
        }
        query_flow::OperatorType::IN_E => {
            return build_vertex_direction_edge_operator(input_id,
                                                        stream_index,
                                                        shuffle_type,
                                                        EdgeDirection::DIR_IN,
                                                        base,
                                                        context);
        }
        query_flow::OperatorType::BOTH_E => {
            return build_vertex_direction_edge_operator(input_id,
                                                        stream_index,
                                                        shuffle_type,
                                                        EdgeDirection::DIR_NONE,
                                                        base,
                                                        context);
        }
        query_flow::OperatorType::OUT_V => {
            return build_edge_direction_operator(input_id,
                                                 stream_index,
                                                 shuffle_type,
                                                 EdgeVertexType::OUTV,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::IN_V => {
            return build_edge_direction_operator(input_id,
                                                 stream_index,
                                                 shuffle_type,
                                                 EdgeVertexType::INV,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::BOTH_V => {
            return build_edge_direction_operator(input_id,
                                                 stream_index,
                                                 shuffle_type,
                                                 EdgeVertexType::BOTHV,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::OTHER_V => {
            return build_edge_direction_operator(input_id,
                                                 stream_index,
                                                 shuffle_type,
                                                 EdgeVertexType::OTHERV,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::OUT_COUNT => {
            return build_vertex_direction_count_operator(input_id,
                                                         stream_index,
                                                         shuffle_type,
                                                         EdgeDirection::DIR_OUT,
                                                         base,
                                                         context);
        }
        query_flow::OperatorType::IN_COUNT => {
            return build_vertex_direction_count_operator(input_id,
                                                         stream_index,
                                                         shuffle_type,
                                                         EdgeDirection::DIR_IN,
                                                         base,
                                                         context);
        }
        query_flow::OperatorType::BOTH_COUNT => {
            return build_vertex_direction_count_operator(input_id,
                                                         stream_index,
                                                         shuffle_type,
                                                         EdgeDirection::DIR_NONE,
                                                         base,
                                                         context);
        }
        query_flow::OperatorType::COUNT_LOCAL => {
            return build_count_local_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::COUNT => {
            return build_count_operator(input_id,
                                        stream_index,
                                        base);
        }
        query_flow::OperatorType::GROUP_COUNT => {
            return build_group_count_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::COUNT_BY_KEY => {
            return build_count_by_key_operator(input_id,
                                               stream_index,
                                               base,
                                               context);
        }
        query_flow::OperatorType::SELECT => {
            return build_select_operator(input_id,
                                         stream_index,
                                         base,
                                         context);
        }
        query_flow::OperatorType::SELECT_ONE => {
            return build_select_one_operator(input_id,
                                             stream_index,
                                             base,
                                             context);
        }
        query_flow::OperatorType::UNFOLD => {
            return build_unfold_operator(input_id,
                                         stream_index,
                                         base,
                                         context);
        }
        query_flow::OperatorType::PROP_VALUE => {
            return build_prop_value_operator(input_id,
                                             stream_index,
                                             base,
                                             context);
        }
        query_flow::OperatorType::PROP_KEY_VALUE => {
            return build_prop_key_value_operator(input_id,
                                                 stream_index,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::PROP_MAP_VALUE => {
            return build_prop_map_operator(input_id,
                                           stream_index,
                                           base,
                                           context);
        }
        query_flow::OperatorType::PROP_FILL => {
            return build_prop_fill_operator(input_id,
                                            stream_index,
                                            base,
                                            context);
        }
        query_flow::OperatorType::HAS |
        query_flow::OperatorType::FILTER => {
            return build_filter_operator(input_id,
                                         stream_index,
                                         base,
                                         context);
        }

        query_flow::OperatorType::LAMBDA_FILTER => {
            return build_lambda_operator(input_id,
                                         stream_index,
                                         base,
                                         query_id,
                                         script,
                                         LambdaType::FILTER,
                                         context);
        }

        query_flow::OperatorType::LAMBDA_MAP => {
            return build_lambda_operator(input_id,
                                         stream_index,
                                         base,
                                         query_id,
                                         script,
                                         LambdaType::MAP,
                                         context);
        }

        query_flow::OperatorType::LAMBDA_FLATMAP => {
            return build_lambda_operator(input_id,
                                         stream_index,
                                         base,
                                         query_id,
                                         script,
                                         LambdaType::FLATMAP,
                                         context);
        }

        query_flow::OperatorType::WHERE => {
            return build_where_operator(input_id,
                                        stream_index,
                                        base,
                                        context);
        }
        query_flow::OperatorType::WHERE_LABEL => {
            return build_where_label_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::SIMPLE_PATH => {
            return build_simple_path_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::PATH_OUT => {
            return build_path_out_operator(input_id,
                                           stream_index,
                                           base,
                                           context);
        }
        query_flow::OperatorType::DEDUP | query_flow::OperatorType::DEDUP_BY_KEY => {
            return build_dedup_operator(input_id,
                                        stream_index,
                                        base,
                                        context);
        }
        query_flow::OperatorType::COMBINER_RANGE => {
            return build_combiner_range_operator(input_id,
                                                 stream_index,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::RANGE | query_flow::OperatorType::RANGE_BY_KEY => {
            return build_range_operator(input_id,
                                        stream_index,
                                        base,
                                        context);
        }
        query_flow::OperatorType::RANGE_LOCAL => {
            return build_range_local_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::FOLD | query_flow::OperatorType::FOLD_BY_KEY => {
            return build_fold_operator(input_id,
                                       stream_index,
                                       base,
                                       context);
        }
        query_flow::OperatorType::FOLD_STORE => {
            return build_fold_store_operator(input_id,
                                             stream_index,
                                             base,
                                             context);
        }
        query_flow::OperatorType::FOLDMAP => {
            return build_foldmap_operator(input_id,
                                          stream_index,
                                          base,
                                          context);
        }
        query_flow::OperatorType::ORDER => {
            return build_order_operator(input_id,
                                        stream_index,
                                        base,
                                        context);
        }
        query_flow::OperatorType::ORDER_LOCAL => {
            return build_order_local_operator(input_id,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::COLUMN => {
            return build_column_operator(input_id,
                                         stream_index,
                                         base,
                                         context);
        }
        query_flow::OperatorType::LABEL_VALUE => {
            return build_label_value_operator(input_id,
                                              shuffle_type,
                                              stream_index,
                                              base,
                                              query_id,
                                              script,
                                              context);
        }
        query_flow::OperatorType::ENTER_KEY => {
            return build_enter_key_operator(input_id,
                                            shuffle_type,
                                            stream_index,
                                            base,
                                            context);
        }
        query_flow::OperatorType::BYKEY_ENTRY => {
            return build_bykey_entry_operator(input_id,
                                              shuffle_type,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::KEY_MESSAGE => {
            return build_key_message_operator(input_id,
                                              shuffle_type,
                                              stream_index,
                                              base,
                                              context);
        }
        query_flow::OperatorType::COMBINER_SUM => {
            return build_combiner_sum_operator(input_id,
                                               shuffle_type,
                                               stream_index,
                                               base,
                                               context);
        }
        query_flow::OperatorType::SUM | query_flow::OperatorType::SUM_BY_KEY => {
            return build_sum_operator(input_id,
                                      shuffle_type,
                                      stream_index,
                                      base,
                                      context);
        }
        query_flow::OperatorType::MAX | query_flow::OperatorType::MAX_BY_KEY => {
            return build_max_operator(input_id,
                                      shuffle_type,
                                      stream_index,
                                      base,
                                      context);
        }
        query_flow::OperatorType::MIN | query_flow::OperatorType::MIN_BY_KEY => {
            return build_min_operator(input_id,
                                      shuffle_type,
                                      stream_index,
                                      base,
                                      context);
        }
        query_flow::OperatorType::CONSTANT => {
            return build_constant_operator(input_id,
                                           shuffle_type,
                                           stream_index,
                                           base,
                                           context);
        }
        query_flow::OperatorType::PROPERTIES => {
            return build_properties_operator(input_id,
                                             shuffle_type,
                                             stream_index,
                                             base,
                                             context);
        }
        query_flow::OperatorType::DFS_REPEAT_GRAPH => {
            return build_dfs_repeat_graph_operator(input_id,
                                                   shuffle_type,
                                                   stream_index,
                                                   base,
                                                   context);
        }
        query_flow::OperatorType::DFS_REPEAT_CMD => {
            return build_dfs_repeat_cmd_operator(input_id,
                                                 shuffle_type,
                                                 stream_index,
                                                 base,
                                                 context);
        }
        query_flow::OperatorType::DFS_REPEAT_DATA => {
            return build_dfs_repeat_data_operator(input_id,
                                                  shuffle_type,
                                                  stream_index,
                                                  base,
                                                  context);
        }
        OperatorType::SUBGRAPH => {
            return build_subgraph_operator(input_id,
                                           shuffle_type,
                                           stream_index,
                                           base,
                                           context);
        }
        OperatorType::CACHE => {
            return build_cache_operator(input_id,
                                        shuffle_type,
                                        stream_index,
                                        base,
                                        context);
        }
        OperatorType::WRITE_ODPS => {
            return build_write_odps_operator(input_id,
                                             shuffle_type,
                                             stream_index,
                                             base,
                                             context);
        }
        OperatorType::SAMPLE => {
            return build_sample_operator(input_id,
                                         shuffle_type,
                                         stream_index,
                                         base,
                                         context);
        }
        OperatorType::BARRIER => {
            return build_barrier_operator(input_id,
                                          shuffle_type,
                                          stream_index,
                                          base,
                                          context);
        }
        OperatorType::GRAPH_VINEYARD_STREAM => {
            return build_vineyard_stream_operator(input_id,
                                                  shuffle_type,
                                                  stream_index,
                                                  base,
                                                  context);
        }
        OperatorType::OUTPUT_VINEYARD_VERTEX => {
            return build_vineyard_output_vertex_operator(input_id,
                                                         shuffle_type,
                                                         stream_index,
                                                         base,
                                                         context);
        }
        OperatorType::OUTPUT_VINEYARD_EDGE => {
            return build_vineyard_output_edge_operator(input_id,
                                                       shuffle_type,
                                                       stream_index,
                                                       base,
                                                       context);
        }
        _ => {
            error!("cant build operator {:?} for operator type {:?}", base, operator_type);
        }
    }

    return None;
}
