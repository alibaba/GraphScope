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
use crate::process::record::{ObjectElement, Record};
use ir_common::error::str_to_dyn_error;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FnResult, MapFunction};
use std::convert::{TryFrom, TryInto};

impl MapFunction<Record, Record> for algebra_pb::Project {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        if !self.is_append {
            input = Record::default();
        }
        for expr_alias in self.mappings.iter() {
            // TODO: the tag_option of is_query_given may not necessary
            let (tag_pb, tag_opt) = {
                let expr_alias = expr_alias
                    .alias
                    .as_ref()
                    .ok_or(str_to_dyn_error("expr_alias is missing"))?;
                (expr_alias.alias.clone(), expr_alias.is_query_given)
            };
            let tag = if let Some(tag_pb) = tag_pb {
                Some(
                    tag_pb
                        .try_into()
                        .map_err(|e| str_to_dyn_error(&format!("{}", e)))?,
                )
            } else {
                None
            };
            let expr = expr_alias.expr.clone().unwrap();
            let evaluator =
                Evaluator::try_from(expr).map_err(|e| str_to_dyn_error(&format!("{}", e)))?;
            let mut stack = vec![];
            let projected_result = evaluator
                .eval(Some(&input), &mut stack)
                .map_err(|e| str_to_dyn_error(&format!("{}", e)))?;
            input.append(ObjectElement::Prop(projected_result), tag.clone());
            if tag_opt {
                // TODO: maybe not necessary
                input.insert_key(tag.ok_or(str_to_dyn_error("tag is empty"))?);
            }
        }
        Ok(input)
    }
}
