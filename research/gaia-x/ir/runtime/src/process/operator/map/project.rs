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
use crate::process::operator::map::MapFuncGen;
use crate::process::record::{ObjectElement, Record};
use ir_common::error::{str_to_dyn_error, DynResult};
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};
use std::convert::{TryFrom, TryInto};

struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Evaluator, Option<NameOrId>)>,
}

impl MapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        if !self.is_append {
            input = Record::default();
        }
        for (evaluator, alias) in self.projected_columns.iter() {
            let projected_result = evaluator
                .eval(Some(&input))
                .map_err(|e| str_to_dyn_error(&format!("{}", e)))?;
            input.append(ObjectElement::Prop(projected_result), alias.clone());
        }
        Ok(input)
    }
}

impl MapFuncGen for algebra_pb::Project {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Record, Record>>> {
        let mut projected_columns = Vec::with_capacity(self.mappings.len());
        for expr_alias in self.mappings.into_iter() {
            // TODO: the tag_option of is_query_given may not necessary
            let (alias_pb, _is_given_tag) = {
                let expr_alias = expr_alias
                    .alias
                    .ok_or(str_to_dyn_error("expr_alias is missing"))?;
                (expr_alias.alias, expr_alias.is_query_given)
            };
            let alias = alias_pb.map(|alias| alias.try_into()).transpose()?;
            let expr = expr_alias
                .expr
                .ok_or(str_to_dyn_error("expr eval is missing"))?;
            let evaluator = Evaluator::try_from(expr)?;
            projected_columns.push((evaluator, alias));
        }
        Ok(Box::new(ProjectOperator {
            is_append: self.is_append,
            projected_columns,
        }))
    }
}
