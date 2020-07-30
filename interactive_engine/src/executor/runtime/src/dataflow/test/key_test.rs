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
    use maxgraph_common::proto::query_flow::{OperatorBase, OperatorType, RequirementValue, RequirementType, EnterKeyArgumentProto, EnterKeyTypeProto};
    use dataflow::test::{build_test_route, MockSchema, MockGraph, build_modern_mock_graph, build_modern_mock_schema};
    use dataflow::operator::shuffle::StreamShuffleType;
    use dataflow::operator::unarystep::enterkey::{KeyMessageOperator, EnterKeyOperator};
    use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraKeyEntity};
    use std::sync::Arc;
    use dataflow::builder::{UnaryOperator, MessageCollector};
    use dataflow::operator::collector::MessageLocalCollector;
    use maxgraph_common::proto::message::Value;
    use std::collections::HashMap;
    use maxgraph_store::schema::Schema;
    use maxgraph_store::api::{MVGraph, Vertex, Edge};
    use protobuf::Message;
    use dataflow::manager::context::{RuntimeContext, TaskContext};
    use store::store_service::StoreServiceManager;
    use dataflow::manager::lambda::LambdaManager;
    use execution::build_worker_partition_ids;
    use store::store_delegate::StoreDelegate;
    use store::remote_store_service::RemoteStoreServiceManager;
    use maxgraph_store::api::graph_partition::ConstantPartitionManager;
    use store::global_store::GlobalStore;
    use store::global_schema::LocalGraphSchema;

    #[test]
    fn test_enter_key_self_operator() {
        let mut key_base = OperatorBase::new();
        key_base.set_operator_type(OperatorType::ENTER_KEY);

        let mut key_arg = EnterKeyArgumentProto::new();
        key_arg.set_uniq_flag(true);
        key_arg.set_enter_key_type(EnterKeyTypeProto::KEY_SELF);
        let mut argument = Value::new();
        argument.set_payload(key_arg.write_to_bytes().unwrap());
        key_base.set_argument(argument);

        let schema = Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema())));
        let graph = Arc::new(build_modern_mock_graph());
        let route = Arc::new(build_test_route());
        let input_shuffle = StreamShuffleType::exchange(route.clone(), 0);

        let worker_partition_ids = build_worker_partition_ids(&graph.as_ref().get_partitions(), route.as_ref(), 0);
        let context = RuntimeContext::new("test".to_owned(),
                                          graph.clone(),
                                          schema.clone(),
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

        let global_store = Arc::new(GlobalStore::new(Arc::new(RemoteStoreServiceManager::empty()),
                                                     graph,
                                                     true));
        let task_context = TaskContext::new(0,
                                            context.get_snapshot_id(),
                                            context.get_graph_partition_manager().clone(),
                                            vec![0, 1],
                                            false,
                                            context.get_debug_flag());


        let mut key_operator = EnterKeyOperator::new(&key_base,
                                                     0,
                                                     0,
                                                     schema,
                                                     task_context,
                                                     input_shuffle,
                                                     key_arg,
                                                     global_store);

        let mut message = RawMessage::new(RawMessageType::VERTEX);
        message.add_label_entity(RawMessage::from_value(ValuePayload::Int(1)), -100);

        let mut data = vec![];
        {
            let mut local_executor: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
            key_operator.execute(vec![message], &mut local_executor);
        }
        if let Some(result) = data.into_iter().next() {
            let extra_key = ExtraKeyEntity::from_payload(result.get_extend_key_payload().unwrap());
            let key_val = extra_key.take_message();
            println!("key value => {:?}", &key_val);
            if let Some(label_val) = key_val.get_label_entity_by_id(-100) {
                println!("label value => {:?}", label_val.get_message());
            } else {
                assert_eq!(true, false);
            }
            key_operator_with_message(result);
        } else {
            assert_eq!(true, false);
        }
    }

    #[test]
    fn test_key_message_operator() {
        let mut message = RawMessage::new(RawMessageType::VERTEX);
        message.set_extend_key_message(message.clone(), true);
        key_operator_with_message(message);
    }

    fn key_operator_with_message(message: RawMessage) {
        let mut key_base = OperatorBase::new();
        key_base.set_operator_type(OperatorType::KEY_MESSAGE);
        let mut key_del_req = RequirementValue::new();
        key_del_req.set_req_type(RequirementType::KEY_DEL);
        key_base.mut_after_requirement().push(key_del_req);
        let route = build_test_route();
        let input_shuffle = StreamShuffleType::exchange(Arc::new(route), 0);
        let mut key_operator = KeyMessageOperator::new(&key_base, 0, 0, input_shuffle);

        let mut data = vec![];
        {
            let mut local_executor: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
            key_operator.execute(vec![message], &mut local_executor);
        }
        if let Some(result) = data.into_iter().next() {
            if let Some(key_vec) = result.get_extend_key_payload() {
                println!("get key_vec=>{:?}", key_vec);
            } else {
                println!("get key_vec null");
            }
            println!("result => {:?}", &result);
        } else {
            assert_eq!(true, false);
        }
    }
}
