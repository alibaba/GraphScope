//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::sync::Arc;
use maxgraph_store::api::prelude::*;
use maxgraph_store::config::StoreConfig;
use crate::state::*;
use crate::common::volatile::*;
use maxgraph_common::proto::hb::StoreStatus;
use maxgraph_store::api::graph_partition::GraphPartitionManager;

pub struct Store {
    config: Arc<StoreConfig>,
    sys_info: Arc<SysInfo>,
    status: Volatile<StoreStatus>,
}

impl Store {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        let sys_info = Arc::new(SysInfo::new(config.total_memory_mb << 20));
        Store {
            config,
            sys_info,
            status: Volatile::new(StoreStatus::STARTED),
        }
    }

    pub fn get_config(&self) -> Arc<StoreConfig> {
        self.config.clone()
    }

    pub fn get_sys_info(&self) -> Arc<SysInfo> {
        self.sys_info.clone()
    }

    pub fn get_status(&self) -> StoreStatus {
        self.status.read()
    }

    pub fn set_status(&self, status: StoreStatus) {
        self.status.write(status);
    }

}

pub struct StoreContext<V, VI, E, EI> {
    graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
}

impl<V, VI, E, EI> StoreContext<V, VI, E, EI> {
    pub fn new(graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
               partition_manager: Arc<dyn GraphPartitionManager>) -> Self {
        StoreContext {
            graph,
            partition_manager,
        }
    }

    pub fn get_graph(&self) -> &Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>> {
        &self.graph
    }

    pub fn get_partition_manager(&self) -> &Arc<dyn GraphPartitionManager> {
        &self.partition_manager
    }
}
