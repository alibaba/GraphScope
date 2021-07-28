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
use crate::process::traversal::step::{BySubJoin, HasAnyJoin};
use crate::process::traversal::traverser::Traverser;
use crate::Partitioner;
use crate::{generated as pb, TraverserSinkEncoder};
use pegasus::api::function::*;
use pegasus::BuildJobError;
use pegasus_common::collections::{Collection, CollectionFactory, Set};
use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
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

impl JobCompiler<Traverser> for GremlinJobCompiler {
    fn shuffle(&self, _: &[u8]) -> CompileResult<Box<dyn RouteFunction<Traverser>>> {
        let p = self.partitioner.clone();
        if let Some(worker_id) = pegasus::get_current_worker() {
            let num_workers = worker_id.peers as usize / self.num_servers;
            Ok(Box::new(Router { p, num_workers }))
        } else {
            Err("worker id not found")?
        }
    }

    fn broadcast(&self, _: &[u8]) -> CompileResult<Box<dyn MultiRouteFunction<Traverser>>> {
        Err("Partial broadcast is unimplemented")?
    }

    fn source(&self, src: &[u8]) -> CompileResult<Box<dyn Iterator<Item = Traverser> + Send>> {
        let mut step = decode::<pb::gremlin::GremlinStep>(src)?;
        if let Some(worker_id) = pegasus::get_current_worker() {
            let job_workers = worker_id.peers as usize / self.num_servers;
            let step =
                graph_step_from(&mut step, job_workers, worker_id.index, self.partitioner.clone())?;
            Ok(step.gen_source(worker_id.index as usize))
        } else {
            let step =
                graph_step_from(&mut step, 1, self.server_index as u32, self.partitioner.clone())?;
            Ok(step.gen_source(self.server_index as usize))
        }
    }

    fn map(&self, res: &[u8]) -> CompileResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        step.gen_map().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn flat_map(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>
    {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        step.gen_flat_map().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn filter(&self, res: &[u8]) -> CompileResult<Box<dyn FilterFunction<Traverser>>> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        step.gen_filter().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn left_join(&self, res: &[u8]) -> CompileResult<Box<dyn LeftJoinFunction<Traverser>>> {
        let joiner: pb::gremlin::SubTaskJoiner = decode(res)?;
        match joiner.inner {
            Some(pb::gremlin::sub_task_joiner::Inner::WhereJoiner(_)) => Ok(Box::new(HasAnyJoin)),
            Some(pb::gremlin::sub_task_joiner::Inner::ByJoiner(_)) => Ok(Box::new(BySubJoin)),
            Some(pb::gremlin::sub_task_joiner::Inner::GroupValueJoiner(_)) => {
                Ok(Box::new(GroupBySubJoin))
            }
            Some(pb::gremlin::sub_task_joiner::Inner::SelectByJoiner(_)) => {
                Ok(Box::new(SelectBySubJoin))
            }
            None => Err("join information not found;")?,
        }
    }

    fn compare(&self, res: &[u8]) -> CompileResult<Box<dyn CompareFunction<Traverser>>> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        step.gen_cmp().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn group(
        &self, map_factory: &[u8], _unfold: &[u8], _: &[u8],
    ) -> CompileResult<Box<dyn GroupFunction<Traverser>>> {
        let step = decode::<pb::gremlin::GremlinStep>(map_factory)?;
        step.gen_group().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn fold(
        &self, _: &[u8], unfold: &[u8], _sink: &[u8],
    ) -> CompileResult<Box<dyn FoldFunction<Traverser>>> {
        let step = decode::<pb::gremlin::GremlinStep>(unfold)?;
        step.gen_fold().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn collection_factory(
        &self, _res: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Collection<Traverser>>>>>
    {
        unimplemented!()
    }

    // dedup
    fn set_factory(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>>
    {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        step.gen_collection().map_err(|err| BuildJobError::from(err.to_string()))
    }

    fn sink(&self, _: &[u8]) -> CompileResult<Box<dyn EncodeFunction<Traverser>>> {
        Ok(Box::new(TraverserSinkEncoder))
    }
}

#[inline]
fn decode<T: Message + Default>(binary: &[u8]) -> Result<T, BuildJobError> {
    Ok(T::decode(binary).map_err(|e| format!("protobuf decode failure: {}", e))?)
}
