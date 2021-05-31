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
use maxgraph_store::api::prelude::{Vertex, Edge};

use dataflow::builder::LoopOperator;
use dataflow::plan::query_plan::QueryFlowPlan;
use dataflow::manager::context::RuntimeContext;

use protobuf::parse_from_bytes;

pub fn build_loop_operator<V, VI, E, EI, F>(unary: &query_flow::UnaryOperator,
                                            _context: &RuntimeContext<V, VI, E, EI, F>) -> Option<LoopOperator>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let base = unary.get_base();
    let repeat_argument = parse_from_bytes::<query_flow::RepeatArgumentProto>(
        base.get_argument().get_payload())
        .expect("parse repeat argument");
    let repeat_body = repeat_argument.get_plan();
    let query_flow_plan = QueryFlowPlan::new(repeat_body.clone());
    let loop_operator = LoopOperator::new(base.get_id(),
                                          unary.get_input_operator_id(),
                                          unary.get_input_stream_index(),
                                          repeat_argument.get_feedback_id(),
                                          0,
                                          {
                                              if repeat_argument.get_leave_id() == 0 {
                                                  None
                                              } else {
                                                  Some(repeat_argument.get_leave_id())
                                              }
                                          },
                                          Some(0),
                                          repeat_argument.get_loop_limit(),
                                          !repeat_argument.get_emit_flag(),
                                          query_flow_plan);
    return Some(loop_operator);
}
