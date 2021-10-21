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

#[cfg(test)]
mod tests {
    use maxgraph_store::schema::prelude::SchemaBuilder;
    use maxgraph_common::proto::query_flow::{OperatorBase, EarlyStopArgument};
    use maxgraph_common::proto::message::Value;

    use dataflow::operator::unarystep::dedup::RangeOperator;
    use dataflow::test::{build_modern_mock_graph, build_test_route};
    use dataflow::manager::context::{EarlyStopState, RuntimeContext};
    use dataflow::operator::shuffle::{StreamShuffleKeyType, StreamShuffleCompositeType};

    use std::sync::Arc;
    use std::cell::RefCell;
    use std::rc::Rc;
    use dataflow::operator::unarystep::limitstop::GlobalLimitStopOperator;
    use dataflow::builder::{UnaryOperator, MessageCollector};
    use dataflow::message::{RawMessage, RawMessageType};
    use store::store_service::StoreServiceManager;
    use dataflow::operator::collector::MessageLocalCollector;
    use dataflow::manager::lambda::LambdaManager;
    use execution::build_worker_partition_ids;
    use store::store_delegate::StoreDelegate;
    use store::remote_store_service::RemoteStoreServiceManager;
    use maxgraph_store::api::graph_partition::ConstantPartitionManager;
    use maxgraph_store::api::MVGraph;
    use store::global_schema::LocalGraphSchema;

    #[test]
    fn test_limit_stop_flag() {
        let store = Arc::new(build_modern_mock_graph());
        let route = Arc::new(build_test_route());
        let state = EarlyStopState::new();
        let schema_builder = SchemaBuilder::new();

        let worker_partition_ids = build_worker_partition_ids(&store.as_ref().get_partitions(), route.as_ref(), 0);
        let context = RuntimeContext::new("test".to_owned(),
                                          store,
                                          Arc::new(LocalGraphSchema::new(schema_builder.build())),
                                          route,
                                          0,
                                          true,
                                          0,
                                          0,
                                          false,
                                          Arc::new(LambdaManager::new("query_id", None)),
                                          worker_partition_ids,
                                          Arc::new(RemoteStoreServiceManager::empty()),
                                          Arc::new(ConstantPartitionManager::new()),
                                          None);

        let mut range_base = OperatorBase::new();
        let mut range_argument = Value::new();
        range_argument.mut_long_value_list().push(0);
        range_argument.mut_long_value_list().push(1);
        range_base.set_argument(range_argument);
        let mut stop_argument = EarlyStopArgument::new();
        stop_argument.set_global_stop_flag(true);
        range_base.set_early_stop_argument(stop_argument);

        let shuffle = StreamShuffleCompositeType::composite(None,
                                                            Some(StreamShuffleKeyType::exchange(context.get_route().clone(), 0)));
        let range_operator = RangeOperator::new(0,
                                                0,
                                                shuffle,
                                                &range_base);
        let mut stop_flag_operator = GlobalLimitStopOperator::new(Box::new(range_operator),
                                                                  context.get_early_stop_state().clone());
        let curr_stop_state = context.get_early_stop_state().clone();

        let mut data1 = vec![];
        {
            let mut collector1: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data1));
            stop_flag_operator.execute(vec![RawMessage::new(RawMessageType::VERTEX)], &mut collector1);
        }
        assert_eq!(true, data1.into_iter().next().is_some());
        assert_eq!(false, curr_stop_state.check_global_stop());

        let mut data2 = vec![];
        {
            let mut collector2: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data2));
            stop_flag_operator.execute(vec![RawMessage::new(RawMessageType::VERTEX)], &mut collector2);
        }
        assert_eq!(false, data2.into_iter().next().is_some());
        assert_eq!(true, curr_stop_state.check_global_stop());

        let mut data3 = vec![];
        {
            let mut collector3: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data3));
            stop_flag_operator.execute(vec![RawMessage::new(RawMessageType::VERTEX)], &mut collector3);
        }
        assert_eq!(false, data3.into_iter().next().is_some());
        assert_eq!(true, curr_stop_state.check_global_stop());
    }
}
