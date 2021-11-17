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

mod accum;
pub mod accumulator;

use crate::error::FnGenResult;
pub use accum::RecordAccumulator;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;

pub trait AccumFactoryGen {
    fn gen_accum(self) -> FnGenResult<RecordAccumulator>;
}

impl AccumFactoryGen for algebra_pb::logical_plan::Operator {
    fn gen_accum(self) -> FnGenResult<RecordAccumulator> {
        if let Some(opr) = self.opr {
            match opr {
                algebra_pb::logical_plan::operator::Opr::GroupBy(group) => group.gen_accum(),
                _ => Err(ParsePbError::from("algebra_pb op is not a accum op").into()),
            }
        } else {
            Err(ParsePbError::from("algebra op is empty").into())
        }
    }
}
