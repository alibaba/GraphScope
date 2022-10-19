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

mod join;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;

use crate::error::FnGenResult;
use crate::process::functions::JoinKeyGen;
use crate::process::record::{Record, RecordKey};

pub trait JoinFunctionGen {
    fn gen_join(self) -> FnGenResult<Box<dyn JoinKeyGen<Record, RecordKey, Record>>>;
}

impl JoinFunctionGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_join(self) -> FnGenResult<Box<dyn JoinKeyGen<Record, RecordKey, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::Join(join) => Ok(Box::new(join)),
            _ => Err(ParsePbError::from(format!("the operatoris not a `Join`, it is {:?} ", self)))?,
        }
    }
}
