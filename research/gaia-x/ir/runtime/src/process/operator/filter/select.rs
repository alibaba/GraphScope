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

use crate::expr::eval::Evaluator;
use crate::process::record::Record;
use ir_common::error::{str_to_dyn_error, ParsePbError};
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FilterFunction, FnResult};
use std::convert::{TryFrom, TryInto};

impl FilterFunction<Record> for algebra_pb::Select {
    fn test(&self, input: &Record) -> FnResult<bool> {
        if let Some(predicate) = self.predicate.clone() {
            // TODO: gen SelectOperator for evaluating
            let eval = Evaluator::try_from(predicate)?;
            eval.eval_bool(Some(input))
                .map_err(|e| str_to_dyn_error(&format!("{}", e)))
        } else {
            Err(str_to_dyn_error("empty content provided"))
        }
    }
}

struct SelectOperator<'a> {
    pub filter: Evaluator<'a>,
}

impl TryFrom<algebra_pb::Select> for SelectOperator<'static> {
    type Error = ParsePbError;

    fn try_from(select_pb: algebra_pb::Select) -> Result<Self, Self::Error> {
        if let Some(predicate) = select_pb.predicate {
            Ok(SelectOperator {
                filter: predicate.try_into()?,
            })
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}
