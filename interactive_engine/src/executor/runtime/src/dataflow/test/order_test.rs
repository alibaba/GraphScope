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
    use dataflow::manager::order::OrderManager;
    use maxgraph_common::proto::message::{OrderComparatorList, OrderComparator, OrderType};
    use dataflow::message::{RawMessage, ValuePayload};
    use maxgraph_common::proto::query_flow::{RequirementValue, RequirementType};
    use dataflow::manager::requirement::RequirementManager;

    #[test]
    fn test_order_label_value() {
        let label_id = -10;

        let mut compare_list = OrderComparatorList::new();
        let mut order = OrderComparator::new();
        order.set_prop_id(label_id);
        order.set_order_type(OrderType::DESC);
        compare_list.mut_order_comparator().push(order);

        let mut order_manager = OrderManager::new(compare_list, 0, false);

        let mut v1 = RawMessage::from_vertex_id(1, 10);
        v1.add_label_entity(RawMessage::from_value(ValuePayload::Long(3)), label_id);

        let mut v2 = RawMessage::from_vertex_id(1, 11);
        v2.add_label_entity(RawMessage::from_value(ValuePayload::Long(2)), label_id);

        let mut v3 = RawMessage::from_vertex_id(1, 12);
        v3.add_label_entity(RawMessage::from_value(ValuePayload::Long(1)), label_id);

        let mut v4 = RawMessage::from_vertex_id(1, 13);
        v4.add_label_entity(RawMessage::from_value(ValuePayload::Long(0)), label_id);

        order_manager.add_message(v4);
        order_manager.add_message(v1);
        order_manager.add_message(v3);
        order_manager.add_message(v2);

        let result_list = order_manager.get_result_list();
        assert_eq!(4, result_list.len());
        assert_eq!(10, result_list.get(0).unwrap().get_id());
        assert_eq!(13, result_list.get(3).unwrap().get_id());
    }

    #[test]
    fn test_requirement_manager() {
        let mut req_list = vec![];

        let mut req_path = RequirementValue::new();
        req_path.set_req_type(RequirementType::PATH_ADD);
        req_list.push(req_path);

        let mut req_label_start = RequirementValue::new();
        req_label_start.set_req_type(RequirementType::LABEL_START);
        req_list.push(req_label_start);

        let requirement_manager = RequirementManager::new(req_list);
        let requirement_list = requirement_manager.get_requirement_list();
        assert_eq!(2, requirement_list.len());
        assert_eq!(RequirementType::LABEL_START, requirement_list.get(0).unwrap().get_req_type());
        assert_eq!(RequirementType::PATH_ADD, requirement_list.get(1).unwrap().get_req_type());
    }
}
