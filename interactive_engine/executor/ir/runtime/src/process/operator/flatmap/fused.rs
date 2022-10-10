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
use pegasus::api::function::{DynIter, FilterMapFunction, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

enum FusedFunc {
    FilterMap(Box<dyn FilterMapFunction<Record, Record>>),
    FlatMap(Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>),
}

impl FlatMapFunction<Record, Record> for FusedFunc {
    type Target = DynIter<Record>;

    fn exec(&self, input: Record) -> FnResult<Self::Target> {
        let mut results = vec![];
        match self {
            FusedFunc::FilterMap(filter_map) => {
                if let Some(record) = filter_map.exec(input)? {
                    results.push(record);
                }
            }
            FusedFunc::FlatMap(flat_map) => {
                results.extend(flat_map.exec(input)?);
            }
        }

        Ok(Box::new(results.into_iter()))
    }
}

struct FusedOperator {
    funcs: Vec<FusedFunc>,
}

impl FlatMapFunction<Record, Record> for FusedOperator {
    type Target = DynIter<Record>;

    fn exec(&self, input: Record) -> FnResult<Self::Target> {
        let mut results: Vec<Record> = vec![];
        let mut temp_container = vec![];
        if self.funcs.is_empty() {
            return Err(Box::new(FnExecError::unexpected_data_error("zero operator in `FusedOperator`")));
        }
        results.extend(self.funcs.first().unwrap().exec(input)?);
        for func in self.funcs.iter().skip(1) {
            for rec in results.drain(..) {
                temp_container.extend(func.exec(rec)?);
            }
            results.extend(temp_container.drain(..));
        }

        Ok(Box::new(results.into_iter()))
    }
}

impl FlatMapFuncGen for algebra_pb::FusedOperator {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let mut funcs = vec![];
        for op in &self.oprs {
            let inner_op = op
                .opr
                .clone()
                .ok_or(ParsePbError::EmptyFieldError("Node::opr".to_string()))?;
            if let Ok(filter_map) = inner_op.clone().gen_filter_map() {
                funcs.push(FusedFunc::FilterMap(filter_map));
            } else if let Ok(flat_map) = inner_op.gen_flat_map() {
                funcs.push(FusedFunc::FlatMap(flat_map));
            } else {
                return Err(FnGenError::unsupported_error(&format!(
                    "neither `FilterMap` or `FlatMap` operator to fuse, the operator is {:?}",
                    op
                )));
            }
        }
        Ok(Box::new(FusedOperator { funcs }))
    }
}
