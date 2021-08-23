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

use crate::process::traversal::step::*;
// use crate::process::traversal::step::{BySubJoin, HasAnyJoin};
use crate::generated as pb;
use crate::process::traversal::traverser::Traverser;
use crate::Partitioner;
// use crate::TraverserSinkEncoder;
use pegasus::api::function::*;
use pegasus::api::Source;
use pegasus::result::ResultSink;
use pegasus::BuildJobError;
// use pegasus_common::collections::CollectionFactory;
use pegasus_common::collections::{Collection, Set};
// use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
use pegasus_server::service::JobParser;
use pegasus_server::JobRequest;
use prost::Message;
use std::sync::Arc;

pub struct GremlinJobCompiler {
    partitioner: Arc<dyn Partitioner>,
    num_servers: usize,
    server_index: u64,
}

impl GremlinJobCompiler {
    pub fn new<D: Partitioner>(partitioner: D, num_servers: usize, server_index: u64) -> Self {
        GremlinJobCompiler { partitioner: Arc::new(partitioner), num_servers, server_index }
    }

    pub fn get_num_servers(&self) -> usize {
        self.num_servers
    }

    pub fn get_server_index(&self) -> u64 {
        self.server_index
    }

    pub fn get_partitioner(&self) -> Arc<dyn Partitioner> {
        self.partitioner.clone()
    }
}

impl JobParser<Traverser, Traverser> for GremlinJobCompiler {
    fn parse(
        &self, _plan: &JobRequest, _input: &mut Source<Traverser>, _output: ResultSink<Traverser>,
    ) -> Result<(), BuildJobError> {
        unimplemented!()
    }
}
