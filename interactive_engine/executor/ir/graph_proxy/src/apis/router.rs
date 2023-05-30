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

/// A router used to route the data to the destination worker.
/// Specifically, given the graph element id to query,
/// it firstly route the data to the server (aka. process) which is able to access the queried data from graph storage,
/// and then pick a worker (aka. thread) in that server to do the query.
pub trait Router: Send + Sync + 'static {
    /// Given the element id and job_workers (number of workers per server),
    /// return the worker id that is going to do the query.
    fn route(&self, id: &ID, job_workers: usize) -> GraphProxyResult<u64>;
}
