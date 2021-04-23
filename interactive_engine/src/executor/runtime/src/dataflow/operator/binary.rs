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
use dataflow::manager::context::RuntimeContext;
use dataflow::builder::BinaryOperator;
use dataflow::operator::binarystep::builder::*;
use maxgraph_store::api::{Vertex, Edge};

pub fn build_binary_operator<V, VI, E, EI, F>(binary_operator: &query_flow::BinaryOperator, context: &RuntimeContext<V, VI, E, EI, F>) -> Option<Box<BinaryOperator>>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    let operator_type = binary_operator.get_base().get_operator_type();
    match operator_type {
        query_flow::OperatorType::UNION => {
            return build_union_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_DIRECT_FILTER => {
            return build_direct_filter_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_DIRECT_FILTER_NEGATE => {
            return build_direct_filter_negate_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_STORE_FILTER => {
            return build_join_store_filter_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_RIGHT_ZERO_JOIN => {
            return build_right_zero_join(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_LABEL => {
            return build_join_label_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_COUNT_LABEL => {
            return build_join_count_label_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::JOIN_RIGHT_VALUE_KEY => {
            return build_join_right_value_key_operator(binary_operator, Some(context));
        }
        query_flow::OperatorType::DFS_FINISH_JOIN => {
            return build_dfs_finish_join_operator(binary_operator, Some(context));
        }
        _ => {
            error!("Not suport binary operator {:?}", &operator_type);
        }
    }
    return None;
}
