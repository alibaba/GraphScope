//
//! Copyright 2022 Alibaba Group Holding Limited.
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
mod apply;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;

use crate::error::{FnGenError, FnGenResult};
use crate::process::functions::ApplyGen;
use crate::process::record::Record;

pub trait RecordLeftJoinGen {
    fn gen_subtask(self) -> FnGenResult<Box<dyn ApplyGen<Record, Vec<Record>, Option<Record>>>>;
}

impl RecordLeftJoinGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_subtask(self) -> FnGenResult<Box<dyn ApplyGen<Record, Vec<Record>, Option<Record>>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::Apply(apply) => Ok(Box::new(apply)),
            algebra_pb::logical_plan::operator::Opr::SegApply(_seg_apply) => {
                Err(FnGenError::unsupported_error("`SegApply` opr"))?
            }
            _ => Err(ParsePbError::from(format!("the operator is not a `SubTask`, it is {:?}", self)))?,
        }
    }
}
