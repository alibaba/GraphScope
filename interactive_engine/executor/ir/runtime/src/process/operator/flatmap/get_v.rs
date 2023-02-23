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

use graph_proxy::apis::{DynDetails, Vertex};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra::get_v::VOpt;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::entry::Entry;
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
        if let Some(entry) = input.get(self.start_tag) {
            if let Some(e) = entry.as_edge() {
                let src_vertex =
                    Vertex::new(e.src_id, e.get_src_label().map(|l| l.clone()), DynDetails::default());
                let dst_vertex =
                    Vertex::new(e.dst_id, e.get_dst_label().map(|l| l.clone()), DynDetails::default());
                Ok(Box::new(RecordExpandIter::new(
                    input,
                    self.alias.as_ref(),
                    Box::new(vec![src_vertex, dst_vertex].into_iter()),
                )))
            } else {
                Err(FnExecError::unexpected_data_error(&format!(
                    "Can't apply `GetV` with BothV opt (`Auxilia` instead) on an non-edge entry {:?}",
                    entry
                )))?
            }
        } else {
            Ok(Box::new(vec![].into_iter()))
        }
    }
}

impl FlatMapFuncGen for pb::GetV {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        match opt {
            VOpt::Both => {}
            _ => Err(ParsePbError::from(format!(
                "the `GetV` operator is not a `FlatMap`, which has GetV::VOpt: {:?}",
                opt
            )))?,
        }
        let get_both_v_operator = GetBothVOperator { start_tag: self.tag, alias: self.alias };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime get_both_v operator: {:?}", get_both_v_operator);
        }
        Ok(Box::new(get_both_v_operator))
    }
}
