//
//! Copyright 2023 Alibaba Group Holding Limited.
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

use std::convert::TryInto;

use graph_proxy::utils::expr::eval_pred::{EvalPred, PEvaluator};
use ir_common::error::ParsePbError;
use ir_common::generated::physical as pb;
use pegasus::api::function::{FilterFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::entry::Entry;
use crate::process::operator::filter::FilterFuncGen;
use crate::process::record::Record;

/// a filter for path until condition
#[derive(Debug)]
struct PathConditionOperator {
    pub filter: PEvaluator,
}

impl FilterFunction<Record> for PathConditionOperator {
    fn test(&self, input: &Record) -> FnResult<bool> {
        if let Some(entry) = input.get(None) {
            if let Some(path) = entry.as_graph_path() {
                // we assume the until condition must be tested on the path end
                let path_end = path.get_path_end();
                let res = self
                    .filter
                    .eval_bool(Some(path_end))
                    .map_err(|e| FnExecError::from(e))?;
                return Ok(res);
            }
        }
        Err(FnExecError::unexpected_data_error(&format!(
            "unexpected input for path until condition {:?}",
            input.get(None),
        )))?
    }
}

impl FilterFuncGen for pb::PathExpand {
    fn gen_filter(self) -> FnGenResult<Box<dyn FilterFunction<Record>>> {
        if let Some(predicate) = self.condition {
            let path_condition_operator = PathConditionOperator { filter: predicate.try_into()? };
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("Runtime path condition operator: {:?}", path_condition_operator);
            }
            Ok(Box::new(path_condition_operator))
        } else {
            Err(ParsePbError::EmptyFieldError("empty path condition pb".to_string()).into())
        }
    }
}
