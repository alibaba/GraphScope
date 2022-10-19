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
mod edge_expand;
mod fused;
mod get_v;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{DynIter, FlatMapFunction};

use crate::error::{FnGenError, FnGenResult};
use crate::process::record::Record;

pub trait FlatMapFuncGen {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>>;
}
impl FlatMapFuncGen for algebra_pb::logical_plan::operator::Opr {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        match self {
            algebra_pb::logical_plan::operator::Opr::Edge(edge_expand) => edge_expand.gen_flat_map(),
            algebra_pb::logical_plan::operator::Opr::Vertex(get_vertex) => get_vertex.gen_flat_map(),
            algebra_pb::logical_plan::operator::Opr::Unfold(_unfold) => {
                Err(FnGenError::unsupported_error("`Unfold` opr"))
            }
            algebra_pb::logical_plan::operator::Opr::Fused(fused) => fused.gen_flat_map(),
            _ => Err(ParsePbError::from(format!("the operator is not a `FlatMap`, it is {:?}", self)))?,
        }
    }
}
