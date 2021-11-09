//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::error::FnGenResult;
use crate::process::functions::{JoinKeyGen, KeyFunction};
use crate::process::operator::keyed::KeySelector;
use crate::process::record::{Record, RecordKey};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;

impl JoinKeyGen<Record, RecordKey, Record> for algebra_pb::Join {
    fn gen_left_key(&self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        Ok(Box::new(KeySelector::with(self.left_keys.clone())?))
    }

    fn gen_right_key(&self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        Ok(Box::new(KeySelector::with(self.right_keys.clone())?))
    }

    fn get_join_kind(&self) -> JoinKind {
        unsafe { ::std::mem::transmute(self.kind) }
    }
}
