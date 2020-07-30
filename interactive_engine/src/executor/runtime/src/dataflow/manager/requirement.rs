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

use maxgraph_common::proto::query_flow::RequirementValue;
use maxgraph_common::proto::query_flow::RequirementType;

use dataflow::message::RawMessage;

use protobuf::ProtobufEnum;

#[derive(Clone)]
pub struct RequirementManager {
    requirement_list: Vec<RequirementValue>,
}

impl RequirementManager {
    pub fn new(mut requirement_list: Vec<RequirementValue>) -> Self {
        requirement_list.sort_by(|a, b| a.get_req_type().value().cmp(&b.get_req_type().value()));
        RequirementManager {
            requirement_list
        }
    }

    pub fn get_requirement_list(&self) -> &Vec<RequirementValue> {
        &self.requirement_list
    }

    pub fn process_extend_entity(&self, source: &RawMessage, target: &mut RawMessage) {
        if let Some(extend) = source.get_extend_entity() {
            if extend.valid_entity() {
                target.merge_extend_entity(extend.clone());
            }
        }
        target.set_bulk_value(source.get_bulk());
    }

    pub fn process_take_extend_entity(&self, source: &mut RawMessage, target: &mut RawMessage) {
        if let Some(extend) = source.take_extend_entity() {
            if extend.valid_entity() {
                target.merge_extend_entity(extend);
            }
        }
        target.set_bulk_value(source.get_bulk());
    }

    pub fn process_requirement(&self, mut message: RawMessage) -> RawMessage {
        if !self.requirement_list.is_empty() {
            let mut curr_label_id_list = vec![];
            for requirement in self.requirement_list.iter() {
                match requirement.get_req_type() {
                    RequirementType::PATH_ADD | RequirementType::PATH_START => {
                        if curr_label_id_list.is_empty() {
                            message.add_path_entity(message.build_message_value(), None);
                        } else {
                            message.add_path_entity(message.build_message_value(), Some(curr_label_id_list.to_vec()));
                        }
                    }
                    RequirementType::LABEL_START => {
                        let label_id_list = requirement.get_req_argument().get_int_value_list();
                        for label_id in label_id_list {
                            let label_entity = {
                                message.build_message_value()
                            };
                            if *label_id > -1000 {
                                curr_label_id_list.push(*label_id);
                            }
                            message.add_label_entity(label_entity, *label_id);
                        }
                    }
                    RequirementType::LABEL_DEL => {
                        let label_id_list = requirement.get_req_argument().get_int_value_list();
                        for label_id in label_id_list {
                            message.delete_label_by_id(*label_id);
                        }
                    }
                    RequirementType::KEY_DEL => {
                        message.take_extend_key_payload();
                    }
                }
            }
        }

        return message;
    }
}
