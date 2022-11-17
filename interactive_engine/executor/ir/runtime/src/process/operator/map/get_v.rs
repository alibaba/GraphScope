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

use graph_proxy::apis::{DynDetails, Vertex};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::get_v::VOpt;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct GetVertexOperator {
    start_tag: Option<KeyId>,
    opt: VOpt,
    alias: Option<KeyId>,
}

impl FilterMapFunction<Record, Record> for GetVertexOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if let Some(entry) = input.get(self.start_tag) {
            if let Some(e) = entry.as_graph_edge() {
                let (id, label) = match self.opt {
                    VOpt::Start => (e.src_id, e.get_src_label()),
                    VOpt::End => (e.dst_id, e.get_dst_label()),
                    VOpt::Other => (e.get_other_id(), e.get_other_label()),
                    VOpt::Both => unreachable!(),
                };
                let vertex = Vertex::new(id, label.map(|l| l.clone()), DynDetails::default());
                input.append(vertex, self.alias.clone());
                Ok(Some(input))
            } else if let Some(graph_path) = entry.as_graph_path() {
                if let VOpt::End = self.opt {
                    let path_end = graph_path
                        .get_path_end()
                        .ok_or(FnExecError::unexpected_data_error("Get path_end failed in path expand"))?
                        .clone();
                    input.append(path_end, self.alias.clone());
                    Ok(Some(input))
                } else {
                    Err(FnExecError::unsupported_error(
                        "Only support `GetV` with VOpt::End on a path entry",
                    ))?
                }
            } else {
                Err(FnExecError::unexpected_data_error(
                    "Can only apply `GetV` (`Auxilia` instead) on an edge or path entry",
                ))?
            }
        } else {
            Ok(None)
        }
    }
}

impl FilterMapFuncGen for algebra_pb::GetV {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let start_tag = self
            .tag
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let opt: VOpt = unsafe { ::std::mem::transmute(self.opt) };
        if let VOpt::Both = opt {
            Err(ParsePbError::from("the `GetV` operator is not a `FilterMap`, which has GetV::VOpt::Both"))?
        }
        let alias = self
            .alias
            .map(|name_or_id| name_or_id.try_into())
            .transpose()?;
        let get_vertex_operator = GetVertexOperator { start_tag, opt, alias };
        if pegasus::get_current_worker().index == 0 {
            debug!("Runtime get_vertex operator: {:?}", get_vertex_operator);
        }
        Ok(Box::new(get_vertex_operator))
    }
}
