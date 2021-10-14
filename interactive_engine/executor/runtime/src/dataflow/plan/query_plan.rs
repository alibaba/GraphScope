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
use maxgraph_store::api::{Vertex, Edge};

use dataflow::builder::DataflowBuilder;
use dataflow::manager::context::RuntimeContext;
use dataflow::operator::unary::build_unary_operator;
use dataflow::operator::binary::build_binary_operator;
use dataflow::operator::repeat::build_loop_operator;

use std::collections::HashMap;
use dataflow::program::ProgramOperatorDelegate;
use dataflow::operator::sourcestep::builder::build_source_operator;

pub struct QueryFlowPlan {
    query_plan: query_flow::QueryPlan,
}

impl QueryFlowPlan {
    pub fn new(query_plan: query_flow::QueryPlan) -> Self {
        QueryFlowPlan {
            query_plan,
        }
    }

    fn build_operator_list(&self) -> (HashMap<i32, &query_flow::UnaryOperator>, HashMap<i32, &query_flow::BinaryOperator>) {
        let mut unary_list = HashMap::new();
        let mut binary_list = HashMap::new();
        for unary in self.query_plan.get_unary_op().iter() {
            unary_list.insert(unary.get_base().get_id(), unary);
        }
        for binary in self.query_plan.get_binary_op().iter() {
            binary_list.insert(binary.get_base().get_id(), binary);
        }

        (unary_list, binary_list)
    }

    pub fn build<V, VI, E, EI, F>(&self,
                      builder: &mut impl DataflowBuilder,
                      query_id: &str,
                      script: &str,
                      context: &RuntimeContext<V, VI, E, EI, F>) -> Result<(), String>
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync {
        let operator_id_list = self.query_plan.get_operator_id_list();
        let source = self.query_plan.get_source_op();
        let (unary_list, binary_list) = self.build_operator_list();
        for operator_id in operator_id_list.iter() {
            if source.get_base().get_id() == *operator_id {
                if let Some(op_source) = build_source_operator(source, query_id, script, context) {
                        builder.add_source(op_source);
                    } else {
                        let err_msg = format!("build source operator {} fail.", operator_id);
                        error!("{}", err_msg);
                        return Err(err_msg);
                    }
                } else if let Some(unary) = unary_list.get(operator_id) {
                    let unary_op_type = unary.get_base().get_operator_type();
                    match unary_op_type {
                        query_flow::OperatorType::REPEAT => {
                            if let Some(loop_unary) = build_loop_operator(unary, context) {
                                builder.add_loop(loop_unary, script, context);
                            } else {
                                let err_msg = format!("build loop operator {:?} fail", operator_id);
                                error!("{}", err_msg);
                                return Err(err_msg);
                            }
                        }
                        query_flow::OperatorType::BRANCH_OPTION => {
                            let err_msg = format!("BRANCH_OPTION is not implemented branch yet.");
                            error!("{}",err_msg);
                            return Err(err_msg);
                        }

                        query_flow::OperatorType::PROGRAM_GRAPH_CC |
                        query_flow::OperatorType::PROGRAM_CC |
                        query_flow::OperatorType::PROGRAM_GRAPH_LPA |
                        query_flow::OperatorType::PROGRAM_GRAPH_HITS |
                        query_flow::OperatorType::PROGRAM_GRAPH_PAGERANK |
                        query_flow::OperatorType::PROGRAM_GRAPH_SHORTESTPATH |
                        query_flow::OperatorType::PROGRAM_GRAPH_ALLPATH |
                        query_flow::OperatorType::PROGRAM_GRAPH_PEERPRESSURE => {
                            let program_operator = ProgramOperatorDelegate::new(
                                unary.get_base().get_id(),
                                unary.get_input_operator_id(),
                                unary.get_input_stream_index(),
                                unary.get_base().get_operator_type(),
                                unary.get_base().get_argument().get_payload().to_vec());
                            builder.add_program(Box::new(program_operator), context)?;
                        }
                        _ => {
                            if let Some(op_unary) = build_unary_operator(unary,  query_id, script, context) {
                                builder.add_unary(op_unary);
                            } else {
                                error!("build unary operator {:?} fail", operator_id);
                            }
                        }
                    }
            } else if let Some(binary) = binary_list.get(operator_id) {
                if let Some(op_binary) = build_binary_operator(binary, context) {
                    builder.add_binary(op_binary);
                } else {
                    let err_msg = format!("build binary operator {:?} fail", operator_id);
                    error!("{}", err_msg);
                    return Err(err_msg);
                }
            } else {
                let err_msg = format!("cant found operator for id {:?}", operator_id);
                error!("{}", err_msg);
                return Err(err_msg);
            }
        }
        Ok(())
    }
}
