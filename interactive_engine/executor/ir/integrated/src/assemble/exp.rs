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

use std::sync::Arc;

use graph_proxy::{apis::PegasusClusterInfo, create_exp_store, SimplePartition};
use runtime::{initialize_job_assembly, IRJobAssembly};

use crate::InitializeJobAssembly;

pub struct QueryExpGraph {
    num_servers: usize,
}

impl QueryExpGraph {
    pub fn new(num_servers: usize) -> Self {
        QueryExpGraph { num_servers }
    }
}

impl InitializeJobAssembly for QueryExpGraph {
    fn initialize_job_assembly(&self) -> IRJobAssembly {
        let cluster_info = Arc::new(PegasusClusterInfo::default());
        let exp_store = create_exp_store(cluster_info.clone());
        let partitioner = Arc::new(SimplePartition { num_servers: self.num_servers });
        initialize_job_assembly(exp_store, partitioner, cluster_info)
    }
}
