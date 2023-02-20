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

use pegasus::api::function::{DynIter, FilterMapFunction, FlatMapFunction, FnResult};

use crate::error::FnExecError;
use crate::process::record::Record;

#[allow(dead_code)]
pub enum FusedFunc {
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

#[derive(Default)]
pub struct FusedOperator {
    funcs: Vec<FusedFunc>,
}

#[allow(dead_code)]
impl FusedOperator {
    pub fn new(funcs: Vec<FusedFunc>) -> Self {
        FusedOperator { funcs }
    }

    pub fn with_filter_map_func(mut self, func: Box<dyn FilterMapFunction<Record, Record>>) -> Self {
        self.funcs.push(FusedFunc::FilterMap(func));
        self
    }

    pub fn with_flat_map_func(
        mut self, func: Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>,
    ) -> Self {
        self.funcs.push(FusedFunc::FlatMap(func));
        self
    }
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
