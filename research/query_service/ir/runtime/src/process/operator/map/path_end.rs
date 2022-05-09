//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::map::MapFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct PathEndOperator {
    alias: Option<KeyId>,
}

impl MapFunction<Record, Record> for PathEndOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        let entry = input
            .get(None)
            .ok_or(FnExecError::get_tag_error("current in PathEndOperator"))?
            .clone();
        if self.alias.is_some() {
            input.append_arc_entry(entry.clone(), self.alias.clone());
        }
        Ok(input)
    }
}

impl MapFuncGen for algebra_pb::PathEnd {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let path_end = PathEndOperator { alias };
        debug!("Runtime path end operator: {:?}", path_end);
        Ok(Box::new(path_end))
    }
}
