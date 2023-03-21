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

use graph_proxy::{create_csr_store, CsrPartition};
use runtime::IRJobAssembly;

use crate::InitializeJobAssembly;

pub struct QueryCsrGraph {
    num_servers: usize,
}

impl QueryCsrGraph {
    pub fn new(num_servers: usize) -> Self {
        QueryCsrGraph { num_servers }
    }
}

impl InitializeJobAssembly for QueryCsrGraph {
    fn initialize_job_assembly(&self) -> IRJobAssembly {
        create_csr_store();
        let partitioner = CsrPartition { num_servers: self.num_servers };
        IRJobAssembly::new(partitioner)
    }
}
