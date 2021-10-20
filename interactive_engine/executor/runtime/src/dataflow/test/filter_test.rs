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
    use maxgraph_store::api::prelude::Property;

    use maxgraph_common::proto::query_flow::{OperatorBase, OperatorType};
    use maxgraph_common::proto::message;

    use dataflow::manager::filter::FilterManager;

    use dataflow::test::{LocalStoreVertex, build_test_route, MockSchema, build_modern_mock_graph, MockGraph, build_modern_mock_schema};
    use dataflow::message::{RawMessage, ValuePayload, RawMessageType, IdMessage};
    use maxgraph_common::proto::message::{VariantType, Value, CompareType};
    use dataflow::operator::unarystep::dedup::DedupOperator;
    use std::sync::Arc;
    use dataflow::operator::shuffle::{StreamShuffleKeyType, StreamShuffleType};
    use dataflow::builder::{UnaryOperator, MessageCollector};
    use utils::{PROP_KEY, PROP_VALUE};
    use std::collections::HashMap;
    use dataflow::operator::collector::MessageLocalCollector;
    use dataflow::operator::unarystep::filter::FilterOperator;
    use dataflow::manager::context::{RuntimeContext, TaskContext};
    use store::store_service::StoreServiceManager;
    use maxgraph_store::schema::Schema;
    use maxgraph_store::schema::prelude::DataType;
    use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
    use dataflow::manager::lambda::LambdaManager;
    use execution::build_worker_partition_ids;
    use maxgraph_store::api::MVGraph;
    use store::store_delegate::StoreDelegate;
    use store::remote_store_service::RemoteStoreServiceManager;
    use maxgraph_store::api::graph_partition::ConstantPartitionManager;
    use store::global_store::GlobalStore;
    use store::global_schema::LocalGraphSchema;

    #[test]
    fn test_filter_manager_or() {
        let id_id = 1;
        let age_id = 2;

        let mut or_compare = message::LogicalCompare::new();
        or_compare.set_compare(message::CompareType::OR_RELATION);

        let mut age_compare = message::LogicalCompare::new();
        age_compare.set_prop_id(age_id);
        age_compare.set_compare(message::CompareType::GT);
        let mut age_value = message::Value::new();
        age_value.set_value_type(message::VariantType::VT_INT);
        age_value.set_int_value(29);
        age_compare.set_value(age_value);
        or_compare.mut_child_compare_list().push(age_compare);

        let mut id_compare = message::LogicalCompare::new();
        id_compare.set_prop_id(id_id);
        id_compare.set_compare(message::CompareType::GT);
        let mut id_value = message::Value::new();
        id_value.set_value_type(message::VariantType::VT_LONG);
        id_value.set_long_value(3);
        id_compare.set_value(id_value);
        or_compare.mut_child_compare_list().push(id_compare);

        let compare_list = vec![or_compare];
        let filter_manager = FilterManager::new(&compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        filter_native_vertex(&filter_manager, id_id, age_id);
        filter_message_vertex(&filter_manager, id_id, age_id);
    }

    fn filter_native_vertex(filter_manager: &FilterManager, id_id: i32, age_id: i32) {
        let mut v1 = LocalStoreVertex::new(1, 1);
        v1.add_property(id_id as u32, Property::Long(1));
        v1.add_property(age_id as u32, Property::Int(29));
        let v1_flag = filter_manager.filter_native_vertex(&v1);
        assert_eq!(false, v1_flag);

        let mut v2 = LocalStoreVertex::new(2, 1);
        v2.add_property(id_id as u32, Property::Long(5));
        v2.add_property(age_id as u32, Property::Int(29));
        let v2_flag = filter_manager.filter_native_vertex(&v2);
        assert_eq!(true, v2_flag);

        let mut v3 = LocalStoreVertex::new(3, 1);
        v3.add_property(id_id as u32, Property::Long(1));
        v3.add_property(age_id as u32, Property::Int(30));
        let v3_flag = filter_manager.filter_native_vertex(&v3);
        assert_eq!(true, v3_flag);

        let mut v4 = LocalStoreVertex::new(4, 1);
        v4.add_property(id_id as u32, Property::Long(6));
        v4.add_property(age_id as u32, Property::Int(30));
        let v4_flag = filter_manager.filter_native_vertex(&v4);
        assert_eq!(true, v4_flag);
    }

    fn filter_message_vertex(filter_manager: &FilterManager, id_id: i32, age_id: i32) {
        let v1 = LocalStoreVertex::new(1, 1);
        let mut vm1 = RawMessage::from_vertex(v1);
        vm1.add_native_property(id_id, Property::Long(1));
        vm1.add_native_property(age_id, Property::Int(29));
        let v1_flag = filter_manager.filter_message(&vm1);
        assert_eq!(false, v1_flag);

        let v2 = LocalStoreVertex::new(2, 1);
        let mut vm2 = RawMessage::from_vertex(v2);
        vm2.add_native_property(id_id, Property::Long(5));
        vm2.add_native_property(age_id, Property::Int(29));
        let v2_flag = filter_manager.filter_message(&vm2);
        assert_eq!(true, v2_flag);

        let v3 = LocalStoreVertex::new(3, 1);
        let mut vm3 = RawMessage::from_vertex(v3);
        vm3.add_native_property(id_id, Property::Long(1));
        vm3.add_native_property(age_id, Property::Int(30));
        let v3_flag = filter_manager.filter_message(&vm3);
        assert_eq!(true, v3_flag);

        let v4 = LocalStoreVertex::new(4, 1);
        let mut vm4 = RawMessage::from_vertex(v4);
        vm4.add_native_property(id_id, Property::Long(6));
        vm4.add_native_property(age_id, Property::Int(30));
        let v4_flag = filter_manager.filter_message(&vm4);
        assert_eq!(true, v4_flag);
    }

    #[test]
    fn test_filter_manager_has() {
        let id_id = 1;
        let age_id = 2;

        let mut has_compare = message::LogicalCompare::new();
        has_compare.set_prop_id(age_id);
        has_compare.set_compare(message::CompareType::EXIST);

        let compare_list = vec![has_compare];
        let filter_manager = FilterManager::new(&compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let mut v1 = LocalStoreVertex::new(1, 1);
        v1.add_property(id_id as u32, Property::Long(1));
        v1.add_property(age_id as u32, Property::Int(29));
        let v1_flag = filter_manager.filter_native_vertex(&v1);
        assert_eq!(true, v1_flag);

        let mut vm1 = RawMessage::from_vertex(v1);
        vm1.add_native_property(id_id, Property::Long(1));
        vm1.add_native_property(age_id, Property::Int(29));
        let vm1_flag = filter_manager.filter_message(&vm1);
        assert_eq!(true, vm1_flag);

        let mut v2 = LocalStoreVertex::new(2, 1);
        v2.add_property(id_id as u32, Property::Long(5));
        let v2_flag = filter_manager.filter_native_vertex(&v2);
        assert_eq!(false, v2_flag);

        let mut vm2 = RawMessage::from_vertex(v2);
        vm2.add_native_property(id_id, Property::Long(1));
        let vm2_flag = filter_manager.filter_message(&vm2);
        assert_eq!(false, vm2_flag);
    }

    #[test]
    fn test_filter_manager_text_contains_not() {
        let id_id = 1;
        let name_id = 2;

        let mut has_compare = message::LogicalCompare::new();
        has_compare.set_prop_id(name_id);
        has_compare.set_compare(message::CompareType::CONTAINS);
        let mut text_value = Value::new();
        text_value.set_value_type(VariantType::VT_STRING);
        text_value.set_bool_flag(true);
        text_value.set_str_value("abc".to_owned());
        has_compare.set_value(text_value);
        has_compare.set_field_type(VariantType::VT_STRING);

        let compare_list = vec![has_compare];
        let filter_manager = FilterManager::new(&compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let mut v1 = LocalStoreVertex::new(1, 1);
        v1.add_property(id_id as u32, Property::Long(1));
        v1.add_property(name_id as u32, Property::String("bacdef".to_owned()));
        let v1_flag = filter_manager.filter_native_vertex(&v1);
        assert_eq!(true, v1_flag);

        let mut v2 = LocalStoreVertex::new(2, 1);
        v2.add_property(id_id as u32, Property::Long(1));
        v2.add_property(name_id as u32, Property::String("babcdef".to_owned()));
        let v2_flag = filter_manager.filter_native_vertex(&v2);
        assert_eq!(false, v2_flag);

        let mut vm1 = RawMessage::from_vertex(v1);
        vm1.add_native_property(id_id, Property::Long(1));
        vm1.add_native_property(name_id, Property::String("bacdef".to_owned()));
        let vm1_flag = filter_manager.filter_message(&vm1);
        assert_eq!(true, vm1_flag);

        let mut vm2 = RawMessage::from_vertex(v2);
        vm2.add_native_property(id_id, Property::Long(1));
        vm2.add_native_property(name_id, Property::String("babcdef".to_owned()));
        let vm2_flag = filter_manager.filter_message(&vm2);
        assert_eq!(false, vm2_flag);
    }

    #[test]
    fn test_filter_vertex_prop_has_key() {
        let mut has_key_compare = message::LogicalCompare::new();
        has_key_compare.set_prop_id(PROP_KEY);
        has_key_compare.set_compare(message::CompareType::EQ);
        let mut value_k = Value::new();
        value_k.set_str_value("age".to_owned());
        value_k.set_value_type(VariantType::VT_STRING);
        has_key_compare.set_value(value_k);
        has_key_compare.set_field_type(VariantType::VT_STRING);

        let mut has_val_compare = message::LogicalCompare::new();
        has_val_compare.set_prop_id(PROP_VALUE);
        has_val_compare.set_compare(message::CompareType::EQ);
        let mut value_v = Value::new();
        value_v.set_int_value(30);
        value_v.set_value_type(VariantType::VT_INT);
        has_val_compare.set_value(value_v);
        has_val_compare.set_field_type(VariantType::VT_INT);

        let schema = build_modern_mock_schema();
        let age_id = schema.get_prop_id("age").unwrap();

        let compare_list = vec![has_key_compare, has_val_compare];
        let filter_manager = FilterManager::new(&compare_list, Arc::new(LocalGraphSchema::new(Arc::new(schema))));

        let mut p1 = RawMessage::new(RawMessageType::PROP);
        p1.set_id(IdMessage::new(0, age_id as i64));
        p1.set_value(ValuePayload::Int(30));
        let v1_flag = filter_manager.filter_message(&p1);
        assert_eq!(true, v1_flag);

        let mut p2 = RawMessage::new(RawMessageType::PROP);
        p2.set_id(IdMessage::new(0, age_id as i64));
        p2.set_value(ValuePayload::Int(35));
        let v2_flag = filter_manager.filter_message(&p2);
        assert_eq!(false, v2_flag);
    }

    #[test]
    fn test_filter_manager_list_contains() {
        let id_id = 1;
        let int_list = 2;
        let long_list = 3;
        let str_list = 4;

        let mut int_contains = message::LogicalCompare::new();
        int_contains.set_prop_id(int_list);
        int_contains.set_compare(CompareType::LIST_CONTAINS);
        int_contains.set_field_type(VariantType::VT_INT);
        let mut int_value = Value::new();
        int_value.set_value_type(VariantType::VT_INT);
        int_value.set_int_value(3);
        int_contains.set_value(int_value);

        let int_compare_list = vec![int_contains];
        let int_filter = FilterManager::new(&int_compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let mut int_message1 = RawMessage::new(RawMessageType::VERTEX);
        int_message1.add_native_property(int_list, Property::ListInt(vec![1, 2, 3, 4, 5]));
        assert_eq!(true, int_filter.filter_message(&int_message1));
        let mut int_message2 = RawMessage::new(RawMessageType::VERTEX);
        int_message2.add_native_property(int_list, Property::ListInt(vec![1, 2, 4, 5]));
        assert_eq!(false, int_filter.filter_message(&int_message2));

        let mut long_contains = message::LogicalCompare::new();
        long_contains.set_prop_id(long_list);
        long_contains.set_compare(CompareType::LIST_CONTAINS);
        long_contains.set_field_type(VariantType::VT_LONG);
        let mut long_value = Value::new();
        long_value.set_value_type(VariantType::VT_LONG);
        long_value.set_long_value(3);
        long_contains.set_value(long_value);

        let long_compare_list = vec![long_contains];
        let long_filter = FilterManager::new(&long_compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let mut long_message1 = RawMessage::new(RawMessageType::VERTEX);
        long_message1.add_native_property(long_list, Property::ListLong(vec![1, 2, 3, 4, 5]));
        assert_eq!(true, long_filter.filter_message(&long_message1));
        let mut long_message2 = RawMessage::new(RawMessageType::VERTEX);
        long_message2.add_native_property(long_list, Property::ListLong(vec![1, 2, 4, 5]));
        assert_eq!(false, long_filter.filter_message(&long_message2));

        let mut str_contains = message::LogicalCompare::new();
        str_contains.set_prop_id(str_list);
        str_contains.set_compare(CompareType::LIST_CONTAINS);
        str_contains.set_field_type(VariantType::VT_STRING);
        let mut str_value = Value::new();
        str_value.set_value_type(VariantType::VT_STRING);
        str_value.set_str_value("bcd".to_owned());
        str_contains.set_value(str_value);

        let str_compare_list = vec![str_contains];
        let str_filter = FilterManager::new(&str_compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let mut str_message1 = RawMessage::new(RawMessageType::VERTEX);
        str_message1.add_native_property(str_list, Property::ListString(vec!["abc".to_owned(), "bcd".to_owned(), "cde".to_owned()]));
        assert_eq!(true, str_filter.filter_message(&str_message1));
        let mut str_message2 = RawMessage::new(RawMessageType::VERTEX);
        str_message2.add_native_property(str_list, Property::ListString(vec!["abc".to_owned(), "cde".to_owned()]));
        assert_eq!(false, str_filter.filter_message(&str_message2));
    }

    #[test]
    fn test_filter_manager_label() {
        let id_id = 1;
        let age_id = 2;
        let label_id = -1000;

        let mut eq_compare = message::LogicalCompare::new();
        eq_compare.set_prop_id(label_id);
        eq_compare.set_compare(message::CompareType::EQ);
        eq_compare.set_field_type(VariantType::VT_LONG);
        let mut compare_value = Value::new();
        compare_value.set_value_type(VariantType::VT_LONG);
        compare_value.set_long_value(100);
        eq_compare.set_value(compare_value);

        let compare_list = vec![eq_compare];
        let filter_manager = FilterManager::new(&compare_list, Arc::new(LocalGraphSchema::new(Arc::new(build_modern_mock_schema()))));

        let v1 = LocalStoreVertex::new(1, 1);
        let mut vm1 = RawMessage::from_vertex(v1);
        vm1.add_native_property(id_id, Property::Long(1));
        vm1.add_native_property(age_id, Property::Int(29));
        vm1.add_label_entity(RawMessage::from_value(ValuePayload::Long(100)), label_id);
        assert_eq!(true, filter_manager.filter_message(&vm1));

        let v2 = LocalStoreVertex::new(1, 1);
        let mut vm2 = RawMessage::from_vertex(v2);
        vm2.add_native_property(id_id, Property::Long(1));
        vm2.add_native_property(age_id, Property::Int(29));
        vm2.add_label_entity(RawMessage::from_value(ValuePayload::Long(1000)), label_id);
        assert_eq!(false, filter_manager.filter_message(&vm2));

        let v3 = LocalStoreVertex::new(1, 1);
        let mut vm3 = RawMessage::from_vertex(v3);
        vm3.add_native_property(id_id, Property::Long(1));
        vm3.add_native_property(age_id, Property::Int(29));
        assert_eq!(false, filter_manager.filter_message(&vm3));
    }

    #[test]
    fn test_filter_dedup_op() {
        let prop_id = -10;
        let route = Arc::new(build_test_route());
        let input_shuffle = StreamShuffleKeyType::exchange(
            route,
            prop_id);
        let dedup_base = OperatorBase::new();
        let mut dedup_op = DedupOperator::new(0, 0, input_shuffle, &dedup_base, prop_id);

        let mut m1 = RawMessage::new(RawMessageType::VERTEX);
        m1.add_label_entity(RawMessage::from_value(ValuePayload::Long(1)), prop_id);

        let mut m2 = RawMessage::new(RawMessageType::VERTEX);
        m2.add_label_entity(RawMessage::from_value(ValuePayload::Long(2)), prop_id);

        let mut m3 = RawMessage::new(RawMessageType::VERTEX);
        m3.add_label_entity(RawMessage::from_value(ValuePayload::Long(1)), prop_id);

        let mut m4 = RawMessage::new(RawMessageType::VERTEX);
        m4.add_label_entity(RawMessage::from_value(ValuePayload::Long(1)), prop_id);

        let mut m5 = RawMessage::new(RawMessageType::VERTEX);
        m5.add_label_entity(RawMessage::from_value(ValuePayload::Long(2)), prop_id);

        let mut m6 = RawMessage::new(RawMessageType::VERTEX);
        m6.add_label_entity(RawMessage::from_value(ValuePayload::Long(6)), prop_id);

        let mut data1 = vec![];
        {
            let mut collector1: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data1));
            dedup_op.execute(vec![m1], &mut collector1);
        }
        assert_eq!(true, data1.into_iter().next().is_some());

        let mut data2 = vec![];
        {
            let mut collector2: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data2));
            dedup_op.execute(vec![m2], &mut collector2);
        }
        assert_eq!(true, data2.into_iter().next().is_some());

        let mut data3 = vec![];
        {
            let mut collector3: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data3));
            dedup_op.execute(vec![m3], &mut collector3);
        }
        assert_eq!(true, data3.into_iter().next().is_none());

        let mut data4 = vec![];
        {
            let mut collector4: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data4));
            dedup_op.execute(vec![m4], &mut collector4);
        }
        assert_eq!(true, data4.into_iter().next().is_none());

        let mut data5 = vec![];
        {
            let mut collector5: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data5));
            dedup_op.execute(vec![m5], &mut collector5);
        }
        assert_eq!(true, data5.into_iter().next().is_none());

        let mut data6 = vec![];
        {
            let mut collector6: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data6));
            dedup_op.execute(vec![m6], &mut collector6);
        }
        assert_eq!(true, data6.into_iter().next().is_some());
    }
}
