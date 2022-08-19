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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::FnGenResult;
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

struct FusedOperator {
    ops: Vec<Box<dyn FilterMapFunction<Record, Record>>>,
}

impl FilterMapFunction<Record, Record> for FusedOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        for op in &self.ops {
            if let Some(record) = op.exec(input)? {
                input = record;
            } else {
                return Ok(None);
            }
        }
        Ok(Some(input))
    }
}

impl FilterMapFuncGen for algebra_pb::FusedOperator {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let mut ops = vec![];
        for op in &self.oprs {
            let op = op
                .opr
                .clone()
                .ok_or(ParsePbError::EmptyFieldError("Node::opr".to_string()))?;
            ops.push(op.gen_filter_map()?);
        }
        Ok(Box::new(FusedOperator { ops }))
    }
}
