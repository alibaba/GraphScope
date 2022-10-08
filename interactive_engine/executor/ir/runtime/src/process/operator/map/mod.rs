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
mod auxilia;
mod expand_intersect;
mod get_v;
mod path_end;
mod path_start;
mod project;

pub use expand_intersect::Intersection;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FilterMapFunction, MapFunction};

use crate::error::FnGenResult;
use crate::process::record::Record;

pub trait MapFuncGen {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>>;
}

impl MapFuncGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::PathEnd(path_end) => path_end.gen_map(),
            _ => Err(ParsePbError::ParseError(format!("the operator: {:?} is not a `Map`", self)))?,
        }
    }
}

pub trait FilterMapFuncGen {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>>;
}

impl FilterMapFuncGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::Vertex(get_vertex) => get_vertex.gen_filter_map(),
            algebra_pb::logical_plan::operator::Opr::PathStart(path_start) => path_start.gen_filter_map(),
            algebra_pb::logical_plan::operator::Opr::Project(project) => project.gen_filter_map(),
            algebra_pb::logical_plan::operator::Opr::Auxilia(auxilia) => auxilia.gen_filter_map(),
            algebra_pb::logical_plan::operator::Opr::Edge(edge_expand) => edge_expand.gen_filter_map(),
            _ => Err(ParsePbError::from(format!("the operator is not a `FilterMap`, it is {:?}", self)))?,
        }
    }
}
