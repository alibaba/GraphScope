//
//! Copyright 2020 Alibaba Group Holding Limited.
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

pub use assembly::IRJobAssembly;
use router::Router;

pub mod assembly;
pub mod error;
pub mod process;
pub mod router;

#[macro_use]
extern crate dyn_type;
#[macro_use]
extern crate log;

use std::sync::Arc;

use graph_proxy::apis::cluster_info::ClusterInfo;
use graph_proxy::apis::partitioner::PartitionInfo;
use graph_proxy::apis::{register_graph, ReadGraph};

/// Initialize a job assembly with the given graph, partition info and cluster info.
/// IRJobAssembly provides a `DefaultRouter`, which is a default implementation of `Router` that can be used in most distributed environment.
pub fn initialize_job_assembly<G: ReadGraph + 'static, P: PartitionInfo, C: ClusterInfo>(
    graph: Arc<G>, partition_info: Arc<P>, cluster_info: Arc<C>,
) -> IRJobAssembly<P, C> {
    register_graph(graph);
    let job_assembly = IRJobAssembly::with(partition_info, cluster_info);
    job_assembly
}

// /// Initialize a job assembly with the given graph and router.
pub fn initialize_job_assembly_with_router<G: ReadGraph + 'static, P: PartitionInfo, C: ClusterInfo>(
    graph: Arc<G>, router: Arc<dyn Router<P = P, C = C>>,
) -> IRJobAssembly<P, C> {
    register_graph(graph);
    let job_assembly = IRJobAssembly::new(router);
    job_assembly
}
