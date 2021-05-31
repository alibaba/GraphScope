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

use dataflow::manager::filter::FilterManager;
use dataflow::manager::range::parse_range_manager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::RawMessage;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use dataflow::builder::SourceOperator;
use dataflow::operator::unary::build_unary_operator_node;
use dataflow::operator::sourcestep::chain::SourceChainOperator;
use dataflow::operator::sourcestep::vertex::{SourceVertexOperator, SourceVertexIdOperator};
use dataflow::operator::sourcestep::count::SourceCountOperator;
use dataflow::operator::sourcestep::edge::SourceEdgeOperator;
use dataflow::common::iterator::UnaryIteratorNode;

use dataflow::io::tunnel::*;

use maxgraph_common::proto::query_flow::{InputEdgeShuffle, InputShuffleType, OdpsGraphInput, RuntimeGraphSchemaProto, VertexPrimaryKeyListProto, OperatorType};
use maxgraph_common::proto::message::SubgraphVertexList;
use maxgraph_common::proto::query_flow;

use maxgraph_store::api::{Vertex, Edge, GlobalGraphQuery, LabelId};

use protobuf::parse_from_bytes;
use dataflow::operator::sourcestep::dfs::DfsSourceOperator;
use std::sync::Arc;
use dataflow::operator::sourcestep::vineyard::VineyardBuilderOperator;


pub fn build_source_operator<V, VI, E, EI, F>(source: &query_flow::SourceOperator,
                                query_id: &str,
                                script: &str,
                                context: &RuntimeContext<V, VI, E, EI, F>) -> Option<Box<SourceOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let exec_local_flag = context.get_exec_local_flag();
    let partition_manager = context.get_graph_partition_manager();
    let worker_index = context.get_index();
    let task_context = TaskContext::new(worker_index as u32,
                                        context.get_snapshot_id(),
                                        partition_manager.clone(),
                                        context.get_partition_ids().as_ref().to_vec(),
                                        exec_local_flag,
                                        context.get_debug_flag());
    return build_graph_source_operator(source,
                                       query_id,
                                       script,
                                       context,
                                       task_context,
                                       context.get_store().clone());
}

fn build_graph_source_operator<VV, VVI, EE, EEI, F>(source: &query_flow::SourceOperator,
                                                    query_id: &str,
                                                    script: &str,
                                                    context: &RuntimeContext<VV, VVI, EE, EEI, F>,
                                                    task_context: TaskContext,
                                                    global_graph: Arc<GlobalGraphQuery<V=VV, VI=VVI, E=EE, EI=EEI>>) -> Option<Box<SourceOperator>>
    where VV: 'static + Vertex,
          VVI: 'static + Iterator<Item=VV> + Send,
          EE: 'static + Edge,
          EEI: 'static + Iterator<Item=EE> + Send,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let label_list = source.get_base().get_argument().get_int_value_list().to_vec().into_iter().map(move |v| v as u32).collect();
    let filter_manager = FilterManager::new(source.get_base().get_logical_compare(), context.get_schema().clone());
    let range_manager = parse_range_manager(source.get_base());
    let after_requirement = RequirementManager::new(source.get_base().get_after_requirement().to_vec());
    let id = source.get_base().get_id();
    let op_type = source.get_base().get_operator_type();

    let id_list = {
        let mut source_id_list = source.get_base().get_argument().get_long_value_list().to_vec();
        if op_type == OperatorType::V ||
            op_type == OperatorType::E ||
            op_type == OperatorType::V_COUNT ||
            op_type == OperatorType::E_COUNT ||
            op_type == OperatorType::SOURCE_CHAIN {
            let primary_key_list = parse_from_bytes::<VertexPrimaryKeyListProto>(source.get_base().get_argument().get_payload()).unwrap();
            for primary_key in primary_key_list.get_primary_keys() {
                let label_id = primary_key.get_label_id();
                let key = primary_key.get_primary_key_value().to_string();
                if let Some((_, vertex_id)) = context.get_graph_partition_manager().get_vertex_id_by_primary_key(label_id as LabelId, &key) {
                    source_id_list.push(vertex_id);
                }
            }
        }
        source_id_list
    };

    match op_type {
        query_flow::OperatorType::V | query_flow::OperatorType::V_COUNT => {
            if id_list.is_empty() {
                let mut source = SourceVertexOperator::new(id,
                                                           task_context,
                                                           label_list,
                                                           filter_manager,
                                                           range_manager,
                                                           after_requirement,
                                                           global_graph);
                if op_type == query_flow::OperatorType::V {
                    return Some(Box::new(source));
                } else {
                    return Some(Box::new(SourceCountOperator::new(id, source.execute().count() as i64)));
                }
            } else {
                let mut source = SourceVertexIdOperator::new(id,
                                                             task_context,
                                                             label_list,
                                                             id_list,
                                                             filter_manager,
                                                             range_manager,
                                                             after_requirement,
                                                             global_graph);
                if op_type == query_flow::OperatorType::V {
                    return Some(Box::new(source));
                } else {
                    return Some(Box::new(SourceCountOperator::new(id, source.execute().count() as i64)));
                }
            }
        }
        query_flow::OperatorType::E | query_flow::OperatorType::E_COUNT => {
            let fetch_prop_flag = source.get_base().get_argument().get_bool_value();
            let mut source = SourceEdgeOperator::new(id,
                                                     task_context,
                                                     label_list,
                                                     filter_manager,
                                                     range_manager,
                                                     after_requirement,
                                                     id_list,
                                                     fetch_prop_flag,
                                                     global_graph);
            if op_type == query_flow::OperatorType::E {
                return Some(Box::new(source));
            } else {
                return Some(Box::new(SourceCountOperator::new(id, source.execute().count() as i64)));
            }
        }
        query_flow::OperatorType::ESTIMATE_COUNT => {
            unimplemented!();
        }
        query_flow::OperatorType::DFS_SOURCE => {
            let id = source.get_base().get_id();
            let batch_size = source.get_base().get_argument().get_long_value();
            return Some(Box::new(DfsSourceOperator::new(id, batch_size)));
        }
        query_flow::OperatorType::SOURCE_CHAIN => {
            let source_iterator = if id_list.is_empty() {
                let mut op = SourceVertexOperator::new(id,
                                                       task_context,
                                                       label_list,
                                                       filter_manager,
                                                       range_manager,
                                                       after_requirement,
                                                       global_graph);
                op.execute()
            } else {
                SourceVertexIdOperator::new(
                    source.get_base().get_id(),
                    task_context,
                    label_list,
                    id_list,
                    filter_manager,
                    range_manager,
                    after_requirement,
                    global_graph).execute()
            };
            let mut unary_iterator_chain: Option<Box<Iterator<Item=RawMessage> + Send>> = None;
            let mut chain_list = source.get_base().get_chained_function().to_vec();
            let mut forward_edge_shuffle = InputEdgeShuffle::new();
            forward_edge_shuffle.set_shuffle_type(InputShuffleType::FORWARD_TYPE);
            let first_unary = chain_list.remove(0);
            let op_type = first_unary.get_operator_type();
            if let Some(first_node) = build_unary_operator_node(&first_unary,
                                                                query_id,
                                                                script,
                                                                Some(context),
                                                                0,
                                                                0,
                                                                &forward_edge_shuffle,
                                                                op_type) {
                unary_iterator_chain = Some(Box::new(UnaryIteratorNode::new(Some(source_iterator), None, first_node)));
                for opbase in chain_list.into_iter() {
                    let op_type = opbase.get_operator_type();
                    if let Some(unary_node) = build_unary_operator_node(&opbase,
                                                                        query_id,
                                                                        script,
                                                                        Some(context),
                                                                        0,
                                                                        0,
                                                                        &forward_edge_shuffle,
                                                                        op_type) {
                        let curr_uanry_iterator_chain: Option<Box<Iterator<Item=RawMessage> + Send>> = Some(Box::new(UnaryIteratorNode::new(
                            unary_iterator_chain,
                            None,
                            unary_node)));
                        unary_iterator_chain = curr_uanry_iterator_chain;
                    }
                }
                return Some(Box::new(SourceChainOperator::new(id, unary_iterator_chain)));
            } else {
                error!("create first node in chain fail");
                return None;
            }
        }
        query_flow::OperatorType::SUBGRAPH_SOURCE => {
            let base = source.get_base();
            let vertex_label_list = parse_from_bytes::<SubgraphVertexList>(base.get_argument().get_payload());
            if vertex_label_list.is_err() {
                error!("parse subgraph vertex list error: {:?}", vertex_label_list);
                return None;
            } else {
                let mut edge_labels = context.get_subgraph().edge_labels.borrow_mut();
                if base.get_argument().get_int_value_list().is_empty() {
                    edge_labels.push(None);
                } else {
                    for edge_label in base.get_argument().get_int_value_list() {
                        edge_labels.push(Some(*edge_label as u32));
                    }
                }

                let vertex_label_value_list = vertex_label_list.unwrap();
                let mut label_id_list = vec![];
                for label in vertex_label_value_list.get_source_vertex_list() {
                    let label_id = *label as u32;
                    if !label_id_list.contains(&label_id) {
                        label_id_list.push(label_id);
                    }
                }
                for label in vertex_label_value_list.get_target_vertex_list() {
                    let label_id = *label as u32;
                    if !label_id_list.contains(&label_id) {
                        label_id_list.push(label_id);
                    }
                }

                let source_vertex_operator = SourceVertexOperator::new(base.get_id(),
                                                                       task_context,
                                                                       label_id_list,
                                                                       FilterManager::new(&vec![], context.get_schema().clone()),
                                                                       None,
                                                                       RequirementManager::new(vec![]),
                                                                       global_graph);
                return Some(Box::new(source_vertex_operator));
            }
        }
        query_flow::OperatorType::GRAPH_SOURCE => {
            let graph_input = parse_from_bytes::<OdpsGraphInput>(source.get_base().get_argument().get_payload());
            if graph_input.is_err() {
                error!("parse edge input config error: {:?}", graph_input);
                return None;
            } else {
                let edge_reader = EdgeReader::new(graph_input.unwrap(), context.get_index(), context.get_peers());
                if edge_reader.is_ok() {
                    let edge_reader = edge_reader.unwrap();
                    let source = SourceChainOperator::new(source.get_base().get_id(), Some(Box::new(edge_reader)));
                    return Some(Box::new(source));
                } else {
                    error!("open edge reader error, please check your input");
                    return None;
                }
            }
        }
        query_flow::OperatorType::GRAPH_VINEYARD_BUILDER => {
            let graph_name = source.get_base().get_argument().get_str_value().to_string();
            let runtime_schema = parse_from_bytes::<RuntimeGraphSchemaProto>(source.get_base().get_argument().get_payload()).unwrap();
            let vineyard_builder = VineyardBuilderOperator::new(id,
                                                                graph_name,
                                                                runtime_schema,
                                                                task_context.get_worker_index() as i32);
            return Some(Box::new(vineyard_builder));
        }
        _ => {
            error!("not support for source type {:?}", &op_type);
            return None;
        }
    }
}
