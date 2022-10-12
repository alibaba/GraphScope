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

mod fold;
mod group;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;

use crate::error::FnGenResult;
use crate::process::functions::{FoldGen, GroupGen};
use crate::process::record::{Record, RecordKey};

pub trait GroupFunctionGen {
    fn gen_group(self) -> FnGenResult<Box<dyn GroupGen<Record, RecordKey, Record>>>;
}

pub trait FoldFactoryGen {
    fn gen_fold(self) -> FnGenResult<Box<dyn FoldGen<u64, Record>>>;
}

impl GroupFunctionGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_group(self) -> FnGenResult<Box<dyn GroupGen<Record, RecordKey, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::GroupBy(group) => Ok(Box::new(group)),
            _ => Err(ParsePbError::from(format!("the operator is not a `Group`, it is {:?}", self)))?,
        }
    }
}

impl FoldFactoryGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_fold(self) -> FnGenResult<Box<dyn FoldGen<u64, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::GroupBy(non_key_group) => Ok(Box::new(non_key_group)),
            _ => Err(ParsePbError::from(format!("the operator is not a `Fold`, it is {:?}", self)))?,
        }
    }
}
