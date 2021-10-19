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
    use maxgraph_common::proto::message::{DfsCommand, Value};
    use maxgraph_common::proto::query_flow::OperatorBase;
    use maxgraph_store::schema::prelude::SchemaBuilder;

    use dataflow::operator::binarystep::join::DfsFinishJoinOperator;
    use dataflow::operator::shuffle::StreamShuffleType;
    use dataflow::builder::{SourceOperator, BinaryOperator, UnaryOperator, MessageCollector};
    use dataflow::message::{RawMessageType, RawMessage, ValuePayload};
    use dataflow::operator::unarystep::dfs::DfsRepeatGraphOperator;
    use dataflow::manager::context::{EarlyStopState, RuntimeContext};
    use dataflow::test::{build_test_route, build_modern_mock_graph};

    use protobuf::parse_from_bytes;
    use protobuf::Message;
    use std::sync::Arc;
    use std::rc::Rc;
    use std::cell::RefCell;
    use store::store_service::StoreServiceManager;
    use dataflow::operator::collector::MessageLocalCollector;
    use dataflow::manager::lambda::LambdaManager;
    use execution::{build_worker_partition_ids};
    use maxgraph_store::api::{PartitionId, MVGraph};
    use store::store_delegate::StoreDelegate;
    use store::remote_store_service::RemoteStoreServiceManager;
    use maxgraph_store::api::graph_partition::ConstantPartitionManager;
    use store::global_schema::LocalGraphSchema;
    use dataflow::operator::sourcestep::dfs::DfsSourceOperator;

    #[test]
    fn test_dfs_cmd_source() {
        let mut dfs_cmd_op = DfsSourceOperator::new(1, 10);
        let mut source_iter = dfs_cmd_op.execute();
        let dfs_cmd = source_iter.next().unwrap();
        assert_eq!(RawMessageType::DFSCMD, dfs_cmd.get_message_type());

        let value = dfs_cmd.get_value().unwrap();
        let bytes = value.get_bytes().unwrap();
        let dfs_cmd_proto = parse_from_bytes::<DfsCommand>(bytes).expect("parse dfs command proto");
        assert_eq!(0, dfs_cmd_proto.get_send_count());
        assert_eq!(10, dfs_cmd_proto.get_batch_size());
    }

    #[allow(unused_must_use)]
    #[test]
    fn test_dfs_finish_join_operator() {
        let route = Arc::new(build_test_route());
        let mut base = OperatorBase::new();
        let mut argument = Value::new();
        argument.set_long_value(100);
        base.set_argument(argument);

        let mut dfs_finish_op = DfsFinishJoinOperator::new(&base,
                                                           0,
                                                           0,
                                                           StreamShuffleType::exchange(route.clone(), 0),
                                                           1,
                                                           0,
                                                           StreamShuffleType::exchange(route.clone(), 0),
                                                           true);
        let mut start_dfs_cmd = DfsCommand::new();
        start_dfs_cmd.set_batch_size(10);
        let start_command = RawMessage::from_value_type(ValuePayload::Bytes(start_dfs_cmd.write_to_bytes().unwrap()),
                                                        RawMessageType::DFSCMD);
        dfs_finish_op.execute_left(start_command);
        let next_command1 = parse_from_bytes::<DfsCommand>(
            dfs_finish_op.finish().next().unwrap().get_value().unwrap().get_bytes().unwrap()).expect("parse command");
        assert_eq!(100, next_command1.get_batch_size());

        let nextcount = RawMessage::from_value(ValuePayload::Long(10));
        dfs_finish_op.execute_right(nextcount);
        dfs_finish_op.execute_left(RawMessage::from_value_type(ValuePayload::Bytes(next_command1.write_to_bytes().unwrap()),
                                                               RawMessageType::DFSCMD));
        let next_command2 = parse_from_bytes::<DfsCommand>(
            dfs_finish_op.finish().next().unwrap().get_value().unwrap().get_bytes().unwrap()).expect("parse command");
        assert_eq!(200, next_command2.get_batch_size());
    }

    #[test]
    fn test_dfs_repeat_graph_operator() {
        let mut dfs_base = OperatorBase::new();
        let mut argument = Value::new();
        argument.set_bool_value(true);
        dfs_base.set_argument(argument);

        let store = Arc::new(build_modern_mock_graph());
        let route = Arc::new(build_test_route());
        let state = EarlyStopState::new();
        let schema_builder = SchemaBuilder::new();
        let partition_id_list = store.as_ref().get_partitions();
        let worker_partition_ids = build_worker_partition_ids(&partition_id_list, route.as_ref(), 0);
        let context = RuntimeContext::new("test".to_owned(),
                                          store,
                                          Arc::new(LocalGraphSchema::new(schema_builder.build())),
                                          route.clone(),
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

        let shuffle = StreamShuffleType::exchange(route, 0);
        let mut dfs_repeat_operator = DfsRepeatGraphOperator::new(
            0,
            0,
            shuffle,
            &dfs_base,
            &context);
        let mut dfs_command1 = DfsCommand::new();
        dfs_command1.set_batch_size(3);
        let mut data1 = vec![];
        {
            let mut collector1: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data1));
            let dfs_message1 = RawMessage::from_value_type(ValuePayload::Bytes(dfs_command1.write_to_bytes().unwrap()),
                                                           RawMessageType::DFSCMD);
            dfs_repeat_operator.execute(vec![dfs_message1], &mut collector1);
        }
        let iter1_result = data1.into_iter().next().unwrap();
        assert_eq!(RawMessageType::ENTRY, iter1_result.get_message_type());
        assert_eq!(3, iter1_result.get_entry_value().unwrap().get_value().get_list_value().unwrap().len());

        let mut data2 = vec![];
        {
            let mut collector2: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data2));
            let mut dfs_command2 = DfsCommand::new();
            dfs_command2.set_send_count(3);
            dfs_command2.set_batch_size(6);
            let dfs_message2 = RawMessage::from_value_type(ValuePayload::Bytes(dfs_command2.write_to_bytes().unwrap()),
                                                           RawMessageType::DFSCMD);
            dfs_repeat_operator.execute(vec![dfs_message2], &mut collector2);
        }
        assert_eq!(1, data2.len());

        let mut data3 = vec![];
        {
            let mut collector3: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data3));
            let mut dfs_command3 = DfsCommand::new();
            dfs_command3.set_send_count(9);
            dfs_command3.set_batch_size(6);
            let dfs_message3 = RawMessage::from_value_type(ValuePayload::Bytes(dfs_command3.write_to_bytes().unwrap()),
                                                           RawMessageType::DFSCMD);
            dfs_repeat_operator.execute(vec![dfs_message3], &mut collector3);
        }
        assert_eq!(0, data3.len());
    }
}
