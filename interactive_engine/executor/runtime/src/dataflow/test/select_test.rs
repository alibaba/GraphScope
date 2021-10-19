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
    use std::sync::Arc;

    use dataflow::operator::unarystep::select::SelectOneOperator;
    use dataflow::test::build_test_route;
    use dataflow::operator::shuffle::StreamShuffleType;
    use dataflow::message::{RawMessage, ValuePayload, RawMessageType, ExtraEntryEntity};
    use dataflow::operator::collector::MessageLocalCollector;
    use dataflow::builder::{UnaryOperator, MessageCollector};

    use maxgraph_common::proto::query_flow::OperatorBase;
    use maxgraph_common::proto::message::Value;

    #[test]
    fn test_select_one() {
        let mut base = OperatorBase::new();
        let mut argument = Value::new();
        argument.mut_int_value_list().push(-100);
        argument.set_str_value("aaa".to_string());
        argument.set_bool_value(true);
        base.set_argument(argument);

        let route = Arc::new(build_test_route());
        let input_shuffle = StreamShuffleType::exchange(route, 0);
        let mut select_one_op = SelectOneOperator::new(0, 0, input_shuffle, &base);

        let mut m1 = RawMessage::from_vertex_id(0, 1);
        m1.add_label_entity(RawMessage::from_value(ValuePayload::Int(123)), -100);
        let mut m1ret = vec![];
        {
            let mut c1: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut m1ret));
            select_one_op.execute(vec![m1], &mut c1);
        }
        let mut ret1itor = m1ret.into_iter();
        let ret1 = ret1itor.next().unwrap();
        assert_eq!(123, ret1.get_value().unwrap().get_int().unwrap());
        assert_eq!(true, ret1itor.next().is_none());

        let m2 = RawMessage::from_value_type(
            ValuePayload::Map(vec![ExtraEntryEntity::new(
                RawMessage::from_value(ValuePayload::String("aaa".to_string())),
                RawMessage::from_vertex_id(0, 2))]),
            RawMessageType::MAP);
        let mut m2ret = vec![];
        {
            let mut c2: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut m2ret));
            select_one_op.execute(vec![m2], &mut c2);
        }
        let mut ret2itor = m2ret.into_iter();
        let ret2 = ret2itor.next().unwrap();
        assert_eq!(RawMessageType::VERTEX, ret2.get_message_type());
        assert_eq!(2, ret2.get_id());
        assert_eq!(true, ret2itor.next().is_none());

        let mut m3 = RawMessage::from_value_type(
            ValuePayload::Map(vec![ExtraEntryEntity::new(
                RawMessage::from_value(ValuePayload::String("bbb".to_string())),
                RawMessage::from_vertex_id(0, 2))]),
            RawMessageType::MAP);
        m3.add_label_entity(RawMessage::from_value(ValuePayload::Int(123)), -100);
        let mut m3ret = vec![];
        {
            let mut c3: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut m3ret));
            select_one_op.execute(vec![m3], &mut c3);
        }
        let mut ret3itor = m3ret.into_iter();
        let ret3 = ret3itor.next().unwrap();
        assert_eq!(123, ret3.get_value().unwrap().get_int().unwrap());
        assert_eq!(true, ret3itor.next().is_none());
    }
}
