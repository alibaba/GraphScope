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

use std::sync::Arc;

use graph_proxy::apis::GraphElement;
use ir_common::error::ParsePbError;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, RouteFunction};

use crate::error::FnExecError;
use crate::process::entry::{Entry, EntryType};
use crate::process::record::Record;
use crate::router::Router;

pub struct RecordRouter {
    p: Arc<dyn Router>,
    num_workers: usize,
    shuffle_key: Option<KeyId>,
}

impl RecordRouter {
    pub fn new(
        p: Arc<dyn Router>, num_workers: usize, shuffle_key: Option<KeyId>,
    ) -> Result<Self, ParsePbError> {
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime shuffle number of worker {:?} and shuffle key {:?}", num_workers, shuffle_key);
        }
        Ok(RecordRouter { p, num_workers, shuffle_key })
    }
}

impl RouteFunction<Record> for RecordRouter {
    fn route(&self, t: &Record) -> FnResult<u64> {
        if let Some(entry) = t.get(self.shuffle_key.clone()) {
            match entry.get_type() {
                EntryType::Vertex => {
                    let id = entry.id();
                    Ok(self.p.route(&id, self.num_workers)?)
                }
                EntryType::Edge => {
                    let e = entry
                        .as_edge()
                        .ok_or(FnExecError::Unreachable)?;
                    Ok(self.p.route(&e.src_id, self.num_workers)?)
                }
                EntryType::Path => {
                    let p = entry
                        .as_graph_path()
                        .ok_or(FnExecError::Unreachable)?;
                    Ok(self
                        .p
                        .route(&p.get_path_end().id(), self.num_workers)?)
                }
                // TODO:
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }
}
