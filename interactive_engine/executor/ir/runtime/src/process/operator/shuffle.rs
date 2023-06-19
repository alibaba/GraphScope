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

use graph_proxy::apis::partitioner::{PartitionInfo, PartitionedData};
use graph_proxy::apis::ClusterInfo;
use ir_common::error::ParsePbError;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, RouteFunction};

use crate::process::entry::{Entry, EntryType};
use crate::process::record::Record;
use crate::router::Router;

pub struct RecordRouter<P: PartitionInfo, C: ClusterInfo> {
    p: Arc<dyn Router<P = P, C = C>>,
    shuffle_key: Option<KeyId>,
}

impl<P: PartitionInfo, C: ClusterInfo> RecordRouter<P, C> {
    pub fn new(p: Arc<dyn Router<P = P, C = C>>, shuffle_key: Option<KeyId>) -> Result<Self, ParsePbError> {
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime shuffle key {:?}", shuffle_key);
        }
        Ok(RecordRouter { p, shuffle_key })
    }
}

impl<P: PartitionInfo, C: ClusterInfo> RouteFunction<Record> for RecordRouter<P, C> {
    fn route(&self, t: &Record) -> FnResult<u64> {
        if let Some(entry) = t.get(self.shuffle_key.clone()) {
            match entry.get_type() {
                EntryType::Vertex => Ok(self.p.route(
                    entry
                        .as_vertex()
                        .unwrap()
                        .get_partition_key_id(),
                )?),
                EntryType::Edge => Ok(self
                    .p
                    .route(entry.as_edge().unwrap().get_partition_key_id())?),
                EntryType::Path => Ok(self.p.route(
                    entry
                        .as_graph_path()
                        .unwrap()
                        .get_partition_key_id(),
                )?),
                // TODO:
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }
}
