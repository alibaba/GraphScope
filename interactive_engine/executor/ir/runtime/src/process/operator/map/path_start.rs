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

use graph_proxy::apis::GraphPath;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct PathStartOperator {
    start_tag: Option<KeyId>,
    is_whole_path: bool,
}

impl FilterMapFunction<Record, Record> for PathStartOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if let Some(entry) = input.get(self.start_tag) {
            let v = entry
                .as_graph_vertex()
                .ok_or(FnExecError::unexpected_data_error(
                    "tag does not refer to a graph vertex element",
                ))?;
            let graph_path = GraphPath::new(v.clone(), self.is_whole_path);
            input.append(graph_path, None);
            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}

impl FilterMapFuncGen for algebra_pb::PathStart {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let start_tag = self
            .start_tag
            .map(|tag| tag.try_into())
            .transpose()?;
        let path_start_operator = PathStartOperator { start_tag, is_whole_path: self.is_whole_path };
        debug!("Runtime path start operator: {:?}", path_start_operator);
        Ok(Box::new(path_start_operator))
    }
}
