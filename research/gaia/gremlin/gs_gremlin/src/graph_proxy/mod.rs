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

mod global_storage;
mod partitioner;

use global_storage::create_gs_store;
use gremlin_core::compiler::GremlinJobCompiler;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex};
use std::sync::Arc;
use crate::graph_proxy::partitioner::{MaxGraphMultiPartition, VineyardMultiPartition};
use std::collections::HashMap;

/// Initialize GremlinJobCompiler for v2
pub fn initialize_job_compiler<V, VI, E, EI>(
    graph_query: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    graph_partitioner: Arc<dyn GraphPartitionManager>,
    num_servers: usize,
    server_index: u64,
) -> GremlinJobCompiler
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    create_gs_store(graph_query, graph_partitioner.clone());
    let partition = MaxGraphMultiPartition::new(graph_partitioner);
    GremlinJobCompiler::new(partition, num_servers, server_index)
}

/// Initialize GremlinJobCompiler for vineyard
pub fn initialize_job_compiler_vineyard<V, VI, E, EI>(
    graph_query: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    graph_partitioner: Arc<dyn GraphPartitionManager>,
    partition_worker_mapping: HashMap<u32, u32>,
    num_servers: usize,
    server_index: u64,
) -> GremlinJobCompiler
    where
        V: Vertex + 'static,
        VI: Iterator<Item = V> + Send + 'static,
        E: Edge + 'static,
        EI: Iterator<Item = E> + Send + 'static,
{
    create_gs_store(graph_query, graph_partitioner.clone());
    let partition = VineyardMultiPartition::new(graph_partitioner, partition_worker_mapping);
    GremlinJobCompiler::new(partition, num_servers, server_index)
}