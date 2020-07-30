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

use maxgraph_common::proto::{query_flow, message};

use dataflow::builder::{Operator, BinaryOperator, InputStreamShuffle};
use dataflow::message::{RawMessage, ValuePayload, RawMessageType, ExtraExtendEntity};
use dataflow::manager::requirement::RequirementManager;
use dataflow::operator::shuffle::{StreamShuffleType, StreamShuffleKeyType};

use std::collections::*;
use protobuf::{ProtobufEnum, parse_from_bytes};
use protobuf::Message;
use utils::MAX_BATCH_SIZE;
use dataflow::message::primitive::Write;

pub struct UnionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
}

impl<F> UnionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>) -> Self {
        UnionOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
        }
    }
}

impl<F> Operator for UnionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for UnionOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        let message = self.before_requirement.process_requirement(message);
        Box::new(Some(self.after_requirement.process_requirement(message)).into_iter())
    }

    fn execute_right(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        let message = self.before_requirement.process_requirement(message);
        Box::new(Some(self.after_requirement.process_requirement(message)).into_iter())
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        Box::new(None.into_iter())
    }
}

pub struct DirectFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleKeyType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleKeyType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    right_key_list: HashSet<Vec<u8>>,
    left_value_list: Vec<RawMessage>,
}

impl<F> DirectFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleKeyType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleKeyType<F>) -> Self {
        DirectFilterOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            right_key_list: HashSet::new(),
            left_value_list: vec![],
        }
    }
}

impl<F> Operator for DirectFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for DirectFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        let message = self.before_requirement.process_requirement(message);
        if let Some(extra_key) = message.get_extend_key_payload() {
            if self.right_key_list.contains(extra_key) {
                return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
            } else {
                self.left_value_list.push(message);
            }
        } else {
            let shuffle_payload = message.get_shuffle_id().into_bytes();
            if self.right_key_list.contains(&shuffle_payload) {
                return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
            } else {
                self.left_value_list.push(message);
            }
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(extra_key) = message.take_extend_key_payload() {
            self.right_key_list.insert(extra_key);
        } else {
            let shuffle_payload = message.get_shuffle_id().into_bytes();
            self.right_key_list.insert(shuffle_payload);
        }
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for left_val in self.left_value_list.drain(..) {
            if let Some(extra_key) = left_val.get_extend_key_payload() {
                if self.right_key_list.contains(extra_key) {
                    result_list.push(self.after_requirement.process_requirement(left_val));
                }
            } else {
                let shuffle_payload = left_val.get_shuffle_id().into_bytes();
                if self.right_key_list.contains(&shuffle_payload) {
                    result_list.push(self.after_requirement.process_requirement(left_val));
                }
            }
        }
        return Box::new(result_list.into_iter());
    }
}

pub struct DirectFilterNegateOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleKeyType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleKeyType<F>,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    right_key_list: HashSet<Vec<u8>>,
    left_value_list: Vec<RawMessage>,
}

impl<F> DirectFilterNegateOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleKeyType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleKeyType<F>) -> Self {
        DirectFilterNegateOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            right_key_list: HashSet::new(),
            left_value_list: vec![],
        }
    }
}

impl<F> Operator for DirectFilterNegateOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for DirectFilterNegateOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        let message = self.before_requirement.process_requirement(message);
        if let Some(extra_key) = message.get_extend_key_payload() {
            if !self.right_key_list.contains(extra_key) {
                self.left_value_list.push(message);
            }
        } else {
            let shuffle_payload = message.get_shuffle_id().into_bytes();
            if !self.right_key_list.contains(&shuffle_payload) {
                self.left_value_list.push(message);
            }
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(extra_key) = message.take_extend_key_payload() {
            self.right_key_list.insert(extra_key);
        } else {
            let shuffle_payload = message.get_shuffle_id().into_bytes();
            self.right_key_list.insert(shuffle_payload);
        }
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for left_val in self.left_value_list.drain(..) {
            if let Some(extra_key) = left_val.get_extend_key_payload() {
                if !self.right_key_list.contains(extra_key) {
                    result_list.push(self.after_requirement.process_requirement(left_val));
                }
            } else {
                let shuffle_payload = left_val.get_shuffle_id().into_bytes();
                if !self.right_key_list.contains(&shuffle_payload) {
                    result_list.push(self.after_requirement.process_requirement(left_val));
                }
            }
        }
        return Box::new(result_list.into_iter());
    }
}

pub struct JoinStoreFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    after_requirement: RequirementManager,
    right_key_list: HashSet<i64>,
    left_value_list: Vec<RawMessage>,
    compare_type: message::CompareType,
}

impl<F> JoinStoreFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>) -> Self {
        let compare_type = message::CompareType::from_i32(base.get_argument().get_int_value()).unwrap();
        JoinStoreFilterOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            right_key_list: HashSet::new(),
            left_value_list: vec![],
            compare_type,
        }
    }
}

impl<F> Operator for JoinStoreFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for JoinStoreFilterOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        match self.compare_type {
            message::CompareType::WITHIN => {
                if self.right_key_list.contains(&message.get_id()) {
                    return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
                } else {
                    self.left_value_list.push(message);
                }
            }
            message::CompareType::WITHOUT => {
                if self.right_key_list.contains(&message.get_id()) {
                    return Box::new(None.into_iter());
                }
                self.left_value_list.push(message);
            }
            _ => {
                error!("Not support compare type here {:?}", &self.compare_type);
            }
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(value) = message.take_value() {
            if let Ok(list) = value.take_list_long() {
                for value in list.into_iter() {
                    self.right_key_list.insert(value);
                }
            }
        }
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for value in self.left_value_list.drain(..) {
            match self.compare_type {
                message::CompareType::WITHIN => {
                    if self.right_key_list.contains(&value.get_id()) {
                        result_list.push(self.after_requirement.process_requirement(value));
                    }
                }
                message::CompareType::WITHOUT => {
                    if !self.right_key_list.contains(&value.get_id()) {
                        result_list.push(self.after_requirement.process_requirement(value));
                    }
                }
                _ => {}
            }
        }

        return Box::new(result_list.into_iter());
    }
}

pub struct RightZeroJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleKeyType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    after_requirement: RequirementManager,
    left_value_list: HashMap<Vec<u8>, Option<ExtraExtendEntity>>,
    right_value_list: HashSet<Vec<u8>>,
}

impl<F> RightZeroJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleKeyType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>) -> Self {
        RightZeroJoinOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            left_value_list: HashMap::new(),
            right_value_list: HashSet::new(),
        }
    }
}

impl<F> Operator for RightZeroJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for RightZeroJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(extra_key) = message.take_extend_key_payload() {
            if !self.right_value_list.contains(&extra_key) {
                self.left_value_list.insert(extra_key, message.take_extend_entity());
            }
        } else {
            error!("there's no extra key in {:?}", &message);
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(right_key) = message.get_extend_key_payload() {
            self.right_value_list.insert(right_key.clone());
        } else {
            error!("there's no key in right value {:?}", &message);
        }
        return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for (key, val) in self.left_value_list.drain() {
            if !self.right_value_list.contains(&key) {
                let mut zero_message = RawMessage::from_value(ValuePayload::Long(0));
                zero_message.set_extend_key_payload(key);
                if let Some(extend_val) = val {
                    zero_message.set_extend_entity(extend_val);
                }
                result_list.push(self.after_requirement.process_requirement(zero_message));
            }
        }

        return Box::new(result_list.into_iter());
    }
}

pub struct JoinLabelOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleKeyType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    after_requirement: RequirementManager,
    left_value_list: Vec<RawMessage>,
    right_value_list: HashMap<Vec<u8>, RawMessage>,
    label_id: i32,
    zero_flag: bool,
}

impl<F> JoinLabelOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleKeyType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>,
               zero_flag: bool) -> Self {
        let label_id = base.get_argument().get_int_value();
        JoinLabelOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            left_value_list: Vec::new(),
            right_value_list: HashMap::new(),
            label_id,
            zero_flag,
        }
    }
}

impl<F> Operator for JoinLabelOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for JoinLabelOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(extra_key) = message.get_extend_key_payload() {
            if let Some(right_value) = self.right_value_list.remove(extra_key) {
                message.add_label_entity(right_value, self.label_id);
                return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
            } else {
                self.left_value_list.push(message);
            }
        } else {
            error!("there's no extra key in {:?}", &message);
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(right_key) = message.take_extend_key_payload() {
            self.right_value_list.insert(right_key, message);
        } else {
            error!("there's no key in right value {:?}", &message);
        }
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for mut left_value in self.left_value_list.drain(..) {
            if let Some(left_key) = left_value.take_extend_key_payload() {
                if let Some(right_value) = self.right_value_list.remove(&left_key) {
                    left_value.add_label_entity(right_value, self.label_id);
                    result_list.push(self.after_requirement.process_requirement(left_value));
                } else if self.zero_flag {
                    left_value.add_label_entity(RawMessage::from_value(ValuePayload::Long(0)), self.label_id);
                    result_list.push(self.after_requirement.process_requirement(left_value));
                }
            }
        }

        return Box::new(result_list.into_iter());
    }
}

pub struct JoinRightValueKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleKeyType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    after_requirement: RequirementManager,
    left_value_list: Vec<RawMessage>,
    right_value_list: HashMap<Vec<u8>, RawMessage>,
}

impl<F> JoinRightValueKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleKeyType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>) -> Self {
        JoinRightValueKeyOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            left_value_list: Vec::new(),
            right_value_list: HashMap::new(),
        }
    }
}

impl<F> Operator for JoinRightValueKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for JoinRightValueKeyOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(right_key) = {
            if let Some(extra_key) = message.get_extend_key_payload() {
                if let Some(right_value) = self.right_value_list.remove(extra_key) {
                    Some(right_value)
                } else {
                    None
                }
            } else {
                error!("cant get extra key from left message {:?}", &message);
                None
            }
        } {
            message.set_extend_key_message(right_key, false);
            return Box::new(Some(self.after_requirement.process_requirement(message)).into_iter());
        } else {
            self.left_value_list.push(message);
        }
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, mut message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        if let Some(extra_key) = message.take_extend_key_payload() {
            self.right_value_list.insert(extra_key, message);
        } else {
            error!("cant get extra key from right message {:?}", &message);
        }
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        let mut result_list = Vec::with_capacity(self.left_value_list.len());
        for mut m in self.left_value_list.drain(..) {
            if let Some(right_key) = {
                if let Some(extra_key) = m.get_extend_key_payload() {
                    if let Some(right_value) = self.right_value_list.remove(extra_key) {
                        Some(right_value)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } {
                m.set_extend_key_message(right_key, false);
                result_list.push(m);
            }
        }

        return Box::new(result_list.into_iter());
    }
}

pub struct DfsFinishJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    left_input_id: i32,
    left_stream_index: i32,
    left_shuffle: StreamShuffleType<F>,
    right_input_id: i32,
    right_stream_index: i32,
    right_shuffle: StreamShuffleType<F>,
    right_value_list: Vec<RawMessage>,
    left_value_list: Vec<RawMessage>,
    expect_result_count: i64,
    debug_flag: bool,
}

impl<F> DfsFinishJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(base: &query_flow::OperatorBase,
               left_input_id: i32,
               left_stream_index: i32,
               left_shuffle: StreamShuffleType<F>,
               right_input_id: i32,
               right_stream_index: i32,
               right_shuffle: StreamShuffleType<F>,
               debug_flag: bool) -> Self {
        DfsFinishJoinOperator {
            id: base.get_id(),
            left_input_id,
            left_stream_index,
            left_shuffle,
            right_input_id,
            right_stream_index,
            right_shuffle,
            right_value_list: vec![],
            left_value_list: vec![],
            expect_result_count: base.get_argument().get_long_value(),
            debug_flag,
        }
    }
}

impl<F> Operator for DfsFinishJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> BinaryOperator for DfsFinishJoinOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_left_input_id(&self) -> i32 {
        self.left_input_id
    }

    fn get_left_stream_index(&self) -> i32 {
        self.left_stream_index
    }

    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.left_shuffle.clone())
    }

    fn get_right_input_id(&self) -> i32 {
        self.right_input_id
    }

    fn get_right_stream_index(&self) -> i32 {
        self.right_stream_index
    }

    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.right_shuffle.clone())
    }

    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        self.left_value_list.push(message);
        return Box::new(None.into_iter());
    }

    fn execute_right(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>> {
        self.right_value_list.push(message);
        return Box::new(None.into_iter());
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>> {
        if let Some(left_value) = self.left_value_list.pop() {
            if let Some(value) = left_value.get_value() {
                if let Ok(value_bytes) = value.get_bytes() {
                    let dfs_command = parse_from_bytes::<message::DfsCommand>(value_bytes).expect("parse dfs command");
                    if let Some(right_value) = self.right_value_list.pop() {
                        // recompute batch size via count value
                        if let Some(rv) = right_value.get_value() {
                            if let Ok(delta_result_count) = rv.get_long() {
                                let mut result_command = message::DfsCommand::new();
                                let current_result_count = dfs_command.get_send_count() + delta_result_count;
                                if current_result_count < self.expect_result_count {
                                    let mut batch_size = dfs_command.get_batch_size();
                                    if delta_result_count < self.expect_result_count / 2 && batch_size < MAX_BATCH_SIZE {
                                        batch_size *= 2;
                                    }
                                    result_command.set_batch_size(batch_size);
                                    result_command.set_send_count(current_result_count);
                                    if self.debug_flag {
                                        info!("send new command=>{:?} with delta records {:?}", &result_command, delta_result_count);
                                    }
                                    return Box::new(Some(RawMessage::from_value_type(ValuePayload::Bytes(result_command.write_to_bytes().unwrap()),
                                                                                     RawMessageType::DFSCMD)).into_iter());
                                } else {
                                    if self.debug_flag {
                                        info!("send new command=>{:?} with delta records {:?} finish", &result_command, delta_result_count);
                                    }
                                }
                            }
                        }
                    } else {
                        // if there's no record, increase the batch size
                        let mut result_command = message::DfsCommand::new();
                        let mut new_batch_size = dfs_command.get_batch_size();
                        if new_batch_size < MAX_BATCH_SIZE {
                            new_batch_size *= 10;
                        }
                        result_command.set_batch_size(new_batch_size);
                        result_command.set_send_count(dfs_command.get_send_count());
                        if self.debug_flag {
                            info!("send new command=>{:?} with no record", &result_command);
                        }
                        return Box::new(Some(RawMessage::from_value_type(ValuePayload::Bytes(result_command.write_to_bytes().unwrap()),
                                                                         RawMessageType::DFSCMD)).into_iter());
                    }
                }
            }
        }

        return Box::new(None.into_iter());
    }
}
