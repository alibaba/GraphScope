//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::collections::HashMap;
use std::sync::Arc;

use global_query::store_api::{Edge, Vertex};
use global_query::{GlobalGraphQuery, GraphPartitionManager};
use graph_proxy::{create_gs_store, VineyardMultiPartition};
use runtime::IRJobAssembly;

use crate::InitializeJobAssembly;

pub struct QueryVineyard<V, VI, E, EI> {
    graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
    graph_partitioner: Arc<dyn GraphPartitionManager>,
    partition_server_index_mapping: HashMap<u32, u32>,
}

#[allow(dead_code)]
impl<V, VI, E, EI> QueryVineyard<V, VI, E, EI> {
    pub fn new(
        graph_query: Arc<dyn GlobalGraphQuery<V = V, VI = VI, E = E, EI = EI>>,
        graph_partitioner: Arc<dyn GraphPartitionManager>,
        partition_server_index_mapping: HashMap<u32, u32>,
    ) -> Self {
        QueryVineyard { graph_query, graph_partitioner, partition_server_index_mapping }
    }
}

/// Initialize GremlinJobCompiler for vineyard
impl<V, VI, E, EI> InitializeJobAssembly for QueryVineyard<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn initialize_job_assembly(&self) -> IRJobAssembly {
        create_gs_store(self.graph_query.clone(), self.graph_partitioner.clone());
        let partitioner = VineyardMultiPartition::new(
            self.graph_partitioner.clone(),
            self.partition_server_index_mapping.clone(),
        );
        IRJobAssembly::new(partitioner)
    }
}
