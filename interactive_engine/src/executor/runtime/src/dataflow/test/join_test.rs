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
    use maxgraph_common::proto::query_flow;

    use dataflow::operator::binarystep::join::*;
    use dataflow::operator::shuffle::*;
    use dataflow::builder::BinaryOperator;
    use dataflow::message::{RawMessage, ValuePayload, ExtraKeyEntity};

    use dataflow::test::build_test_route;

    use std::sync::Arc;

    #[test]
    fn test_join_right_value_key_operator() {
        let route = Arc::new(build_test_route());
        let operator_base = query_flow::OperatorBase::new();
        let shuffle_key = StreamShuffleKeyType::constant(route.clone());
        let shuffle = StreamShuffleType::exchange(route.clone(), 0);
        let mut join_right_value_key_op = JoinRightValueKeyOperator::new(
            &operator_base,
            0,
            0,
            shuffle_key,
            0,
            0,
            shuffle);

        let key1 = RawMessage::from_value(ValuePayload::String("key1".to_owned()));
        let key2 = RawMessage::from_value(ValuePayload::String("key2".to_owned()));

        let mut left1 = RawMessage::from_value(ValuePayload::Int(1));
        left1.set_extend_key_message(key1, true);

        let mut left2 = RawMessage::from_value(ValuePayload::Int(2));
        left2.set_extend_key_message(key2, true);

        let key_payload1 = left1.get_extend_key_payload().unwrap().clone();
        let key_payload2 = left2.get_extend_key_payload().unwrap().clone();

        let mut right1 = RawMessage::from_value(ValuePayload::Int(3));
        right1.set_extend_key_payload(key_payload1);

        let mut right2 = RawMessage::from_value(ValuePayload::Int(4));
        right2.set_extend_key_payload(key_payload2);

        let left_iter1 = join_right_value_key_op.execute_left(left1);
        let right_iter1 = join_right_value_key_op.execute_right(right1);
        assert_eq!(left_iter1.count(), 0);
        assert_eq!(right_iter1.count(), 0);

        let right_iter2 = join_right_value_key_op.execute_right(right2);
        let mut left_iter2 = join_right_value_key_op.execute_left(left2);
        assert_eq!(right_iter2.count(), 0);

        let mut left2_message = left_iter2.next().unwrap();
        let left2_key_entity = ExtraKeyEntity::from_payload(&left2_message.take_extend_key_payload().unwrap());
        let left2_key = left2_key_entity.take_message();
        assert_eq!(left2_key.get_value().unwrap().get_int().unwrap(), 4);
        assert_eq!(left2_message.get_value().unwrap().get_int().unwrap(), 2);
        assert_eq!(left_iter2.count(), 0);

        let mut finish_iter = join_right_value_key_op.finish();
        let mut finish_message = finish_iter.next().unwrap();
        let finish_key_entity = ExtraKeyEntity::from_payload(&finish_message.take_extend_key_payload().unwrap());
        let finish_key = finish_key_entity.take_message();
        assert_eq!(finish_key.get_value().unwrap().get_int().unwrap(), 3);
        assert_eq!(finish_message.get_value().unwrap().get_int().unwrap(), 1);
        assert_eq!(finish_iter.count(), 0);
    }

    #[test]
    fn test_join_label_operator() {

    }
}
