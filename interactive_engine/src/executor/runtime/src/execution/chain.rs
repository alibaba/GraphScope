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

use common::message;
use common::message::ClassType;
use common::message::OrderComparatorList;
use common::message::PopType;
use common::message::PropertyType;
use common::message::PropKeyValueType;
use common::query_flow::ChainedFunction;
use common::query_flow::CompareType;
use common::query_flow::OperatorBase;
use common::query_flow::OperatorType;
use FlowMessage;
use maxgraph_store::graph::api::prelude::*;
use protobuf::ProtobufEnum;
use std::sync::Arc;
use steps::filter::simple_path;
use steps::filter::where_by_start_label;
use steps::filter::where_in_label;
use steps::flat_map::*;
use steps::map::count_local_label;
use steps::map::get_v_from_edge;
use steps::map::order_local;
use steps::map::path_out;
use steps::map::prop_map_value;
use steps::map::select;
use steps::map::select_one;
use steps::map::prop_key_value;
use steps::source::scan;
use utils::*;

pub fn build_source_chain<V, VI, E, EI>(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>, partition_ids: Arc<Vec<PartitionId>>, functions: &[ChainedFunction]) -> Vec<FlowMessage>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    let func = build_unary_chain(graph, partition_ids, functions.to_vec());
    let empty: FlowMessage = FlowMessage {
        id: 0,
        class_type: ClassType::VERTEX,
        value: Box::new(message::Vertex::new()),
    };
    func(empty)
}

#[inline]
pub fn unary_chain<V, VI, E, EI>(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>, partition_ids: Arc<Vec<PartitionId>>, base: &OperatorBase) -> impl Fn(FlowMessage) -> Vec<FlowMessage>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    build_unary_chain(graph, partition_ids, base.get_chained_function().to_vec())
}

#[inline]
pub fn build_unary_chain<V, VI, E, EI>(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>, partition_ids: Arc<Vec<PartitionId>>, functions: Vec<ChainedFunction>) -> impl Fn(FlowMessage) -> Vec<FlowMessage>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    let mut chained_funcs = vec![];
    for func in functions {
        chained_funcs.push(build_chain(graph.clone(), partition_ids.clone(), func));
    }
    move |message| {
        let mut input = vec![message];
        for chained_func in &chained_funcs {
            if input.is_empty() {
                break;
            }
            input = chained_func(input);
        }
        input
    }
}

#[inline]
fn build_chain<V, VI, E, EI>(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>, partition_ids: Arc<Vec<PartitionId>>, base: ChainedFunction) -> impl Fn(Vec<FlowMessage>) -> Vec<FlowMessage>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    move |mut input| {
        match base.get_function_type() {
            OperatorType::V => {
                let (label_ids, select_ids) = get_func_flat_map_arguments(&base);
                let ids = base.get_argument().get_long_value_list();
                let mut limit = usize::max_value();
                if base.has_range_limit() {
                    let range_limit = base.get_range_limit();
                    limit = (range_limit.get_range_end() - range_limit.get_range_start()) as usize;
                }

                let logical_regex_pair = parse_regex_filter_pair(base.get_compare_list());
                let logic_filters = logical_regex_pair.0;
                let regex_filters = logical_regex_pair.1;
                scan(graph.as_ref(), &label_ids, partition_ids.as_ref(), ids, logic_filters.as_slice(), regex_filters.as_slice(), &select_ids, limit)
            }
            OperatorType::IN => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| in_v(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::OUT => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| out(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::BOTH_E => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| both_e(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::IN_E => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| in_e(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::OUT_E => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| out_e(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::IN_V => {
                let (_label, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).map(move |x| get_v_from_edge(OperatorType::IN_V, &select_ids, x)).collect()
            }
            OperatorType::OUT_V => {
                let (_label, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).map(move |x| get_v_from_edge(OperatorType::OUT_V, &select_ids, x)).collect()
            }
            OperatorType::OTHER_V => {
                let (_label, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).map(move |x| get_v_from_edge(OperatorType::OTHER_V, &select_ids, x)).collect()
            }
            OperatorType::BOTH_V => {
                let (_label, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).flat_map(move |x| both_v(&select_ids, x)).collect()
            }
            OperatorType::BOTH => {
                let (label, select_ids) = get_func_flat_map_arguments(&base);
                let range = get_func_range(&base);
                input.drain(..).flat_map(|ref x| both(graph.as_ref(), &label, &select_ids, x, partition_ids.as_ref(), range)).collect()
            }
            OperatorType::COUNT_LOCAL => {
                let label_ids = base.get_argument().get_int_value_list().to_vec();
                input.drain(..).map(move |x| count_local_label(&label_ids, x)).collect()
            }
            OperatorType::SELECT => {
                let total_select_count = base.get_argument().get_int_value();
                let select_ids = base.get_argument().get_int_value_list().to_vec();
                input.drain(..).map(move |x| select(total_select_count, &select_ids, x)).collect()
            }
            OperatorType::SELECT_ONE => {
                let pop_type = PopType::from_i32(base.get_argument().get_int_value()).unwrap();
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                let label_ids = base.get_argument().get_int_value_list().to_vec();
                input.drain(..).map(move |x| select_one(&pop_type, &label_ids, &select_ids, x)).collect()
            }
            OperatorType::UNFOLD => {
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).flat_map(move |x| unfold(x, &select_ids)).collect()
            }
            OperatorType::PROP_FILL => {
                input.drain(..).map(|mut x| {
                    fill_property(graph.as_ref(), &mut x, &vec![]);
                    x
                }).collect()
            }
            OperatorType::PROP_VALUE => {
                let prop_ids = base.get_argument().get_int_value_list().to_vec();
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                let graph = graph.clone();
                input.drain(..).flat_map(move |x| prop_value(graph.as_ref(), &prop_ids, &select_ids, x, true)).collect()
            }
            OperatorType::PROP_KEY_VALUE => {
                let prop_key_value_type = PropKeyValueType::from_i32(base.get_argument().get_int_value()).expect("property type");
                let graph = graph.clone();
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                let schema = graph.get_schema(65535).unwrap();
                input.drain(..).map(move |x| prop_key_value(graph.as_ref(), &prop_key_value_type, x, &select_ids, schema.as_ref())).collect()
            }
            OperatorType::PROP_MAP_VALUE => {
                let property_type = PropertyType::from_i32(base.get_argument().get_int_value()).expect("property type");
                let include_tokens = base.get_argument().get_bool_value();
                let prop_ids = base.get_argument().get_int_value_list().to_vec();
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                let graph = graph.clone();
                input.drain(..).map(move |x| prop_map_value(graph.as_ref(), &property_type, include_tokens, &prop_ids, &select_ids, x, false)).collect()
            }
            OperatorType::HAS => {
                let input = input.drain(..).filter(|x| filter(graph.as_ref(), base.get_compare_list(), &x));
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                if select_ids.is_empty() {
                    input.collect()
                } else {
                    input.map(move |x| add_select_labels_in_message(x, &select_ids)).collect()
                }
            }
            OperatorType::FILTER => {
                input.drain(..).filter(|x| filter(graph.as_ref(), base.get_compare_list(), &x)).collect()
            }
            OperatorType::WHERE => {
                let values = base.get_argument().get_int_value_list();
                let compare_type = CompareType::from_i32(values[0]).unwrap();
                let label_id = values[1];
                input.drain(..).filter(|ref x| where_in_label(&compare_type, &label_id, x)).collect()
            }
            OperatorType::WHERE_LABEL => {
                let values = base.get_argument().get_int_value_list();
                let compare_type = CompareType::from_i32(values[0]).unwrap();
                let start_label_id = values[1];
                let label_id = values[2];
                input.drain(..).filter(move |x| where_by_start_label(&compare_type, &start_label_id, &label_id, x)).collect()
            }
            OperatorType::SIMPLE_PATH => {
                input.drain(..).filter(|ref x| simple_path(x)).collect()
            }
            OperatorType::PATH_OUT => {
                let path_out_value_list = base.get_argument().get_path_out_value().to_vec();
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);
                input.drain(..).map(move |x| path_out(x, &path_out_value_list, &select_ids)).collect()
            }

            OperatorType::ORDER_LOCAL => {
                use protobuf::parse_from_bytes;
                let payload = base.get_argument().get_payload();
                let comparators = parse_from_bytes::<OrderComparatorList>(payload).expect("parse comparator list");
                let (_label_ids, select_ids) = get_func_flat_map_arguments(&base);

                let graph = graph.clone();
                input.drain(..).map(move |x| order_local(graph.as_ref(), &comparators, &select_ids, x)).collect()
            }
            OperatorType::RANGE_LOCAL => {
                let range_args = base.get_argument().get_long_value_list().to_vec();
                let low = range_args[0] as usize;
                use std::cmp::min;
                let high = min(input.len(), range_args[1] as usize);
                if low > high {
                    panic!("wrong range: {:?}", range_args)
                }
                input.drain(low..high).collect()
            }
            _ => {
                panic!("chain function {:?} not supported yet", base.get_function_type());
            }
        }
    }
}
