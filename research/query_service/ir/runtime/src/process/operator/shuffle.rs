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
use std::sync::Arc;

use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, RouteFunction};

use crate::error::FnExecError;
use crate::graph::element::{GraphElement, VertexOrEdge};
use crate::graph::partitioner::Partitioner;
use crate::process::record::Record;

pub struct RecordRouter {
    p: Arc<dyn Partitioner>,
    num_workers: usize,
    shuffle_key: Option<NameOrId>,
}

impl RecordRouter {
    pub fn new(
        p: Arc<dyn Partitioner>, num_workers: usize, shuffle_key: common_pb::NameOrIdKey,
    ) -> Result<Self, ParsePbError> {
        let shuffle_key = shuffle_key
            .key
            .map(|e| e.try_into())
            .transpose()?;
        debug!("Runtime shuffle number of worker {:?} and shuffle key {:?}", num_workers, shuffle_key);
        Ok(RecordRouter { p, num_workers, shuffle_key })
    }
}

impl RouteFunction<Record> for RecordRouter {
    fn route(&self, t: &Record) -> FnResult<u64> {
        if let Some(entry) = t.get(self.shuffle_key.as_ref()) {
            if let Some(v) = entry.as_graph_vertex() {
                self.p.get_partition(&v.id(), self.num_workers)
            } else if let Some(e) = entry.as_graph_edge() {
                // shuffle e to the partition that contains other_id
                self.p
                    .get_partition(&e.get_other_id(), self.num_workers)
            } else if let Some(p) = entry.as_graph_path() {
                let path_end = p
                    .get_path_end()
                    .ok_or(FnExecError::unexpected_data_error("get path_end failed in shuffle"))?;
                match path_end {
                    VertexOrEdge::V(v) => self.p.get_partition(&v.id(), self.num_workers),
                    VertexOrEdge::E(e) => self
                        .p
                        .get_partition(&e.get_other_id(), self.num_workers),
                }
            } else {
                //TODO(bingqing): deal with other element shuffle
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }
}
