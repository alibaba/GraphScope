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

mod keyed;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
pub use keyed::KeySelector;

use crate::error::{FnGenError, FnGenResult};
use crate::process::functions::KeyFunction;
use crate::process::record::{Record, RecordKey};

pub trait KeyFunctionGen {
    fn gen_key(self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>>;
}

impl KeyFunctionGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_key(self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::GroupBy(group) => group.gen_key(),
            algebra_pb::logical_plan::operator::Opr::Dedup(dedup) => dedup.gen_key(),
            algebra_pb::logical_plan::operator::Opr::SegApply(_seg_apply) => {
                Err(FnGenError::unsupported_error("`SegApply` opr"))?
            }
            _ => {
                Err(ParsePbError::from(format!("the operator is not a keyed operator, it is {:?}", self)))?
            }
        }
    }
}
