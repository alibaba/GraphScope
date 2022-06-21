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

use std::convert::TryInto;

use graph_proxy::apis::{DefaultDetails, DynDetails, Vertex};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::get_v::VOpt;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Record, RecordExpandIter};

#[derive(Debug)]
struct GetBothVOperator {
    start_tag: Option<KeyId>,
    alias: Option<KeyId>,
}

impl FlatMapFunction<Record, Record> for GetBothVOperator {
    type Target = DynIter<Record>;

    fn exec(&self, input: Record) -> FnResult<Self::Target> {
        let entry = input
            .get(self.start_tag)
            .ok_or(FnExecError::get_tag_error("get tag failed in GetVertexOperator"))?;
        if let Some(e) = entry.as_graph_edge() {
            let src_vertex = Vertex::new(
                e.src_id,
                e.get_src_label().map(|l| l.clone()),
                DynDetails::new(DefaultDetails::default()),
            );
            let dst_vertex = Vertex::new(
                e.dst_id,
                e.get_dst_label().map(|l| l.clone()),
                DynDetails::new(DefaultDetails::default()),
            );
            Ok(Box::new(RecordExpandIter::new(
                input,
                self.alias.as_ref(),
                Box::new(vec![src_vertex, dst_vertex].into_iter()),
            )))
        } else {
            Err(FnExecError::unexpected_data_error(
                "Can only apply `GetV` with BothV opt (`Auxilia` instead) on an edge entry",
            ))?
        }
    }
}

impl FlatMapFuncGen for algebra_pb::GetV {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let start_tag = self
            .tag
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        match opt {
            VOpt::Start | VOpt::End | VOpt::Other => {
                Err(ParsePbError::ParseError(format!("GetV with VOpt {:?} is not a flatmap op", opt)))?
            }
            VOpt::Both => {}
        }
        let alias = self
            .alias
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let get_both_v_operator = GetBothVOperator { start_tag, alias };
        debug!("Runtime get_both_v operator: {:?}", get_both_v_operator);
        Ok(Box::new(get_both_v_operator))
    }
}
