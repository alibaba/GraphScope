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
use partitioner::{MaxGraphMultiPartition, VineyardMultiPartition};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub trait InitializeJobCompiler {
    fn initialize_job_compiler(&self) -> GremlinJobCompiler;
}

pub struct QueryMaxGraph<V, VI, E, EI> {
    graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
    graph_partitioner: Arc<dyn GraphPartitionManager>,
    num_servers: usize,
    server_index: u64,
}

impl<V, VI, E, EI> QueryMaxGraph<V, VI, E, EI> {
    pub fn new(
        graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
        graph_partitioner: Arc<dyn GraphPartitionManager>,
        num_servers: usize,
        server_index: u64,
    ) -> Self {
        QueryMaxGraph {
            graph_query,
            graph_partitioner,
            num_servers,
            server_index,
        }
    }
}

impl<V, VI, E, EI> InitializeJobCompiler for QueryMaxGraph<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn initialize_job_compiler(&self) -> GremlinJobCompiler {
        create_gs_store(self.graph_query.clone(), self.graph_partitioner.clone());
        let partition = MaxGraphMultiPartition::new(self.graph_partitioner.clone());
        GremlinJobCompiler::new(partition, self.num_servers, self.server_index)
    }
}

pub struct QueryVineyard<V, VI, E, EI> {
    graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
    graph_partitioner: Arc<dyn GraphPartitionManager>,
    partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
    worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
    num_servers: usize,
    server_index: u64,
}

impl<V, VI, E, EI> QueryVineyard<V, VI, E, EI> {
    pub fn new(
        graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
        graph_partitioner: Arc<dyn GraphPartitionManager>,
        partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
        worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
        num_servers: usize,
        server_index: u64,
    ) -> Self {
        QueryVineyard {
            graph_query,
            graph_partitioner,
            partition_worker_mapping,
            worker_partition_list_mapping,
            num_servers,
            server_index,
        }
    }
}

/// Initialize GremlinJobCompiler for vineyard
impl<V, VI, E, EI> InitializeJobCompiler for QueryVineyard<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn initialize_job_compiler(&self) -> GremlinJobCompiler {
        create_gs_store(self.graph_query.clone(), self.graph_partitioner.clone());
        let partition = VineyardMultiPartition::new(
            self.graph_partitioner.clone(),
            self.partition_worker_mapping.clone(),
            self.worker_partition_list_mapping.clone(),
            self.num_servers,
            self.server_index,
        );
        GremlinJobCompiler::new(partition, self.num_servers, self.server_index)
    }
}
