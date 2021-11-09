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
mod get_v;
mod project;

use crate::error::FnGenResult;
use crate::process::record::Record;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::MapFunction;

pub trait MapFuncGen {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>>;
}

impl MapFuncGen for algebra_pb::logical_plan::Operator {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        if let Some(opr) = self.opr {
            match opr {
                algebra_pb::logical_plan::operator::Opr::Project(project) => project.gen_map(),
                algebra_pb::logical_plan::operator::Opr::Vertex(get_vertex) => get_vertex.gen_map(),
                algebra_pb::logical_plan::operator::Opr::Path(_path) => todo!(),
                algebra_pb::logical_plan::operator::Opr::ShortestPath(_shortest_path) => todo!(),
                _ => Err(ParsePbError::from("algebra_pb op is not a map").into()),
            }
        } else {
            Err(ParsePbError::from("algebra op is empty").into())
        }
    }
}
