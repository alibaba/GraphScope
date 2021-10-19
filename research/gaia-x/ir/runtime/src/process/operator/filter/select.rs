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
use crate::process::operator::filter::FilterFuncGen;
use crate::process::record::Record;
use ir_common::error::{str_to_dyn_error, DynResult, ParsePbError};
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FilterFunction, FnResult};
use std::convert::{TryFrom, TryInto};

struct SelectOperator {
    pub filter: Evaluator,
}

impl FilterFunction<Record> for SelectOperator {
    fn test(&self, input: &Record) -> FnResult<bool> {
        // TODO: do we need a stack here?
        let mut stack = vec![];
        self.filter
            .eval_bool(Some(input), &mut stack)
            .map_err(|e| str_to_dyn_error(&format!("{}", e)))
    }
}

impl FilterFuncGen for algebra_pb::Select {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Record>>> {
        if let Some(predicate) = self.predicate {
            Ok(Box::new(SelectOperator {
                filter: predicate.try_into()?,
            }))
        } else {
            Err(str_to_dyn_error("empty content provided"))
        }
    }
}
