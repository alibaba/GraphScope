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

use crate::apis::ID;
use crate::GraphProxyResult;

pub trait Partitioner: Send + Sync + 'static {
    /// Given the element id and job_workers (number of worker per server),
    /// return the id of worker that is going to process
    fn get_partition(&self, id: &ID, job_workers: usize) -> GraphProxyResult<u64>;
    /// Get the local partition list that is going to be processed by current server.
    fn get_local_partitions(&self) -> GraphProxyResult<Vec<u32>>;
}
