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
    use super::*;
    use dataflow::test::{build_modern_mock_schema, build_test_route, build_modern_mock_graph};
    use dataflow::operator::sourcestep::vineyard::VineyardBuilderOperator;
    use std::sync::Arc;
    use dataflow::builder::{SourceOperator, UnaryOperator, MessageCollector};
    use dataflow::message::{RawMessageType, RawMessage, ValuePayload, ExtraEdgeEntity, PropertyEntity};
    use dataflow::operator::unarystep::vineyard::VineyardStreamOperator;
    use dataflow::operator::shuffle::StreamShuffleType;
    use dataflow::operator::collector::MessageLocalCollector;
    use dataflow::operator::unarystep::vineyard_writer::{VineyardWriteVertexOperator, VineyardWriteEdgeOperator};
    use store::global_store::GlobalStore;
    use store::remote_store_service::RemoteStoreServiceManager;
    use dataflow::manager::context::TaskContext;
    use maxgraph_store::api::graph_partition::{ConstantPartitionManager, FixedStorePartitionManager};
    use maxgraph_store::api::MVGraphQuery;
    use dataflow::message::subgraph::SubGraph;
    use dataflow::operator::unarystep::subgraph::SubGraphOperator;
    use maxgraph_common::proto::query_flow::{OperatorBase, RuntimeGraphSchemaProto};

    #[test]
    fn test_vineyard_builder() {
        let schema = build_modern_mock_schema();
        let mut builder_operator = VineyardBuilderOperator::new(1,
                                                                "test".to_owned(),
                                                                RuntimeGraphSchemaProto::new(),
                                                                0);
        let mut iter = builder_operator.execute();
        if let Some(mut val) = iter.next() {
            assert_eq!(RawMessageType::VALUE, val.get_message_type());
            if let Some(value) = val.take_value() {
                if let Ok(llist) = value.take_list_long() {
                    assert_eq!(2, llist.len());
                    assert_eq!(111, *llist.get(0).unwrap());
                    assert_eq!(222, *llist.get(1).unwrap());
                } else {
                    assert_eq!(false, true, "Get List<Long> value fail");
                }
            } else {
                assert_eq!(false, true, "Get value payload fail");
            }
        } else {
            assert_eq!(false, true, "There's no result for build");
        }
    }

    #[test]
    fn test_vineyard_stream() {
        let route = build_test_route();
        let shuffle_type = StreamShuffleType::exchange(Arc::new(route), 0);
        let mut stream_operator = VineyardStreamOperator::new(1,
                                                              1,
                                                              shuffle_type,
                                                              1,
                                                              "test".to_owned());
        let input_message_list = vec![RawMessage::from_value(ValuePayload::ListLong(vec![1, 2])),
                                      RawMessage::from_value(ValuePayload::ListLong(vec![3, 4])),
                                      RawMessage::from_value(ValuePayload::ListLong(vec![5, 6]))];
        let mut result_list = vec![];
        {
            let mut collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut result_list));
            stream_operator.execute(input_message_list, &mut collector);
            let mut finish_result = stream_operator.finish();
            if let Some(result) = finish_result.next() {
                assert_eq!(RawMessageType::VALUE, result.get_message_type());
                let result_value = result.get_value().unwrap().get_long().unwrap();
                assert_eq!(333, result_value);
            } else {
                assert_eq!(false, true, "There's no result for stream operator");
            }
            assert_eq!(true, finish_result.next().is_none());
        }
        assert_eq!(0, result_list.len());
    }

    #[test]
    fn test_vineyard_write_vertex() {
        let route = build_test_route();
        let shuffle_type = StreamShuffleType::exchange(Arc::new(route), 0);

        let graph = Arc::new(build_modern_mock_graph());
        let global_store = GlobalStore::new(Arc::new(RemoteStoreServiceManager::empty()),
                                            graph.clone(),
                                            true);
        let partition_list = vec![0, 1, 2, 3];
        let partition_manager = FixedStorePartitionManager::new(graph.clone(), partition_list.clone());
        let context = TaskContext::new(0,
                                       0,
                                       Arc::new(partition_manager),
                                       partition_list.clone(),
                                       false,
                                       true);

        let mut write_vertex = VineyardWriteVertexOperator::new(1,
                                                                1,
                                                                shuffle_type,
                                                                0,
                                                                Arc::new(global_store),
                                                                "test".to_owned(),
                                                                context);
        let mut result_list = vec![];
        let input_vertex_list = graph.as_ref().scan(0, None)
            .map(|v| RawMessage::from_vertex(v)).collect();
        {
            let mut collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut result_list));
            write_vertex.execute(input_vertex_list, &mut collector);
        }
        assert_eq!(0, result_list.len());
        let mut finish_result_list = write_vertex.finish();
        if let Some(result) = finish_result_list.next() {
            println!("{:?}", result);
        } else {
            assert_eq!(false, true, "There's no result for write vineyard vertex");
        }
        assert_eq!(true, finish_result_list.next().is_none());
    }

    #[test]
    fn test_vineyard_write_edge() {
        let route = Arc::new(build_test_route());
        let shuffle_type = StreamShuffleType::exchange(route.clone(), 0);

        let subgraph = Arc::new(SubGraph::new());
        let subgraph_shuffle = StreamShuffleType::exchange(route.clone(), 0);
        let subgraph_base = OperatorBase::new();
        let mut subgraph_op: Box<UnaryOperator> = Box::new(SubGraphOperator::new(0,
                                                                   0,
                                                                   subgraph_shuffle,
                                                                   &subgraph_base,
                                                                   subgraph.clone(),
                                                                   true,
                                                                   true));
        let mut edge_message_list = vec![];
        let mut e1 = RawMessage::from_edge_id(111, 1, true, 11, 1, 22, 2);
        e1.add_property_value(1, ValuePayload::Long(1111));
        e1.add_property_value(2, ValuePayload::String("aaa".to_owned()));
        edge_message_list.push(e1);

        let mut e2 = RawMessage::from_edge_id(222, 1, true, 11, 1, 33, 3);
        e2.add_property_value(1, ValuePayload::Long(2222));
        e2.add_property_value(2, ValuePayload::String("bbb".to_owned()));
        edge_message_list.push(e2);

        let mut e3 = RawMessage::from_edge_id(333, 2, true, 22, 2, 33, 3);
        e3.add_property_value(1, ValuePayload::Long(3333));
        e3.add_property_value(2, ValuePayload::String("ccc".to_owned()));
        edge_message_list.push(e3);

        let mut e4 = RawMessage::from_edge_id(444, 2, true, 33, 3, 44, 4);
        e4.add_property_value(1, ValuePayload::Long(4444));
        e4.add_property_value(2, ValuePayload::String("ddd".to_owned()));
        edge_message_list.push(e4);

        let mut e5 = RawMessage::from_edge_id(555, 1, true, 44, 4, 55, 5);
        e5.add_property_value(1, ValuePayload::Long(5555));
        e5.add_property_value(2, ValuePayload::String("eee".to_owned()));
        edge_message_list.push(e5);

        let mut e6 = RawMessage::from_edge_id(666, 1, true, 55, 5, 66, 6);
        e6.add_property_value(1, ValuePayload::Long(6666));
        e6.add_property_value(2, ValuePayload::String("fff".to_owned()));
        edge_message_list.push(e6);

        let mut subgraph_result_list = vec![];
        let mut finish_list = {
            let mut collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut subgraph_result_list));
            subgraph_op.execute(edge_message_list,
                                &mut collector);
            subgraph_op.finish()
        };
        while let Some(v) = finish_list.next() {
            subgraph_result_list.push(v);
        }
        assert_eq!(subgraph_result_list.len(), 11);

        println!("edge list {:?} in subgraph", subgraph.edge_prop_list.borrow());

        let graph = Arc::new(build_modern_mock_graph());
        let mut write_edge = VineyardWriteEdgeOperator::new(1,
                                                            1,
                                                            shuffle_type,
                                                            0,
                                                            graph,
                                                            "test".to_owned(),
                                                            0,
                                                            subgraph,
                                                            true);
        let mut result_list = vec![];
        {
            let mut collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut result_list));
            write_edge.execute(vec![], &mut collector);
        }
        assert_eq!(0, result_list.len());
        let mut finish_result_list = write_edge.finish();
        if let Some(result) = finish_result_list.next() {
            assert_eq!(6, result.get_value().unwrap().get_long().unwrap());
            println!("{:?}", result);
        } else {
            assert_eq!(false, true, "There's no result for write vineyard edge");
        }
        assert_eq!(true, finish_result_list.next().is_none());
    }
}
