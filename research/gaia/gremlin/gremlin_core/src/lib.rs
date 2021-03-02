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

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate strum_macros;
#[macro_use]
extern crate enum_dispatch;
extern crate lazy_static;
#[macro_use]
extern crate pegasus_common;
#[macro_use]
extern crate pegasus;
#[macro_use]
extern crate pegasus_config;
#[macro_use]
extern crate log;

extern crate graph_store;

use std::sync::Arc;

use crate::process::traversal::step::*;
use crate::process::traversal::traverser::Traverser;
pub use crate::structure::{get_graph, register_graph};
pub use crate::structure::{Element, GraphProxy, ID};
use pegasus::api::accum::{AccumFactory, Accumulator};
use pegasus::api::function::*;
use pegasus_common::collections::{Collection, CollectionFactory, DrainSet, DrainSetFactory};
use pegasus_server::desc::Resource;
use pegasus_server::factory::{CompileResult, HashKey, JobCompiler};
use prost::Message;

pub mod object;
pub mod process;
pub mod structure;

mod pb_request;
mod result_process;
mod storage;
use crate::result_process::result_to_pb;
pub use object::Object;
pub use pb_request::ProtoReflect;
pub use storage::create_demo_graph;

#[cfg(feature = "proto_inplace")]
mod generated {
    #[path = "common.rs"]
    pub mod common;
    #[path = "gremlin.rs"]
    pub mod gremlin;
    #[path = "protobuf.rs"]
    pub mod protobuf;
}

#[cfg(not(feature = "proto_inplace"))]
mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }

    pub mod gremlin {
        tonic::include_proto!("gremlin");
    }

    pub mod protobuf {
        tonic::include_proto!("protobuf");
    }
}

pub type DynError = Box<dyn std::error::Error + Send>;
pub type DynResult<T> = Result<T, Box<dyn std::error::Error + Send>>;
pub type DynIter<T> = Box<dyn Iterator<Item = DynResult<T>> + Send>;

pub trait Partitioner: Send + Sync + 'static {
    fn get_partition(&self, id: &ID) -> u64;
}

pub struct GremlinJobFactory {
    partitioner: Arc<dyn Partitioner>,
}

impl GremlinJobFactory {
    pub fn new<D: Partitioner>(partitioner: D) -> Self {
        GremlinJobFactory { partitioner: Arc::new(partitioner) }
    }
}

pub struct TraverserSinkEncoder;

impl EncodeFunction<Traverser> for TraverserSinkEncoder {
    fn encode(&self, data: Vec<Traverser>) -> Vec<u8> {
        let result_pb = result_to_pb(data);
        let mut bytes = vec![];
        result_pb.encode_raw(&mut bytes);
        bytes
    }
}

impl JobCompiler<Traverser> for GremlinJobFactory {
    fn shuffle(&self, _: &dyn Resource) -> CompileResult<Box<dyn RouteFunction<Traverser>>> {
        let p = self.partitioner.clone();
        Ok(box_route!(move |t: &Traverser| -> u64 {
            if let Some(e) = t.get_element() {
                p.get_partition(&e.id())
            } else {
                0
            }
        }))
    }

    fn broadcast(&self, _: &dyn Resource) -> CompileResult<Box<dyn MultiRouteFunction<Traverser>>> {
        Err("Partial broadcast is unimplemented")?
    }

    fn source(
        &self, worker_index: u32, src: &dyn Resource,
    ) -> CompileResult<Box<dyn Iterator<Item = Traverser> + Send>> {
        Ok(src
            .as_any_ref()
            .downcast_ref::<GraphVertexStep>()
            .ok_or("Downcast `GraphVertexStep` error")?
            .gen_source(worker_index))
    }

    fn map(&self, res: &dyn Resource) -> CompileResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        Ok(res.as_any_ref().downcast_ref::<MapStep>().ok_or("Downcast `MapStep` error")?.gen())
    }

    fn flat_map(
        &self, res: &dyn Resource,
    ) -> CompileResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>
    {
        Ok(res
            .as_any_ref()
            .downcast_ref::<FlatMapStep>()
            .ok_or("Downcast `FlatMapStep` error")?
            .gen())
    }

    fn filter(&self, res: &dyn Resource) -> CompileResult<Box<dyn FilterFunction<Traverser>>> {
        Ok(res
            .as_any_ref()
            .downcast_ref::<FilterStep>()
            .ok_or("Downcast `FilterStep` error")?
            .gen())
    }

    fn left_join(&self, res: &dyn Resource) -> CompileResult<Box<dyn LeftJoinFunction<Traverser>>> {
        Ok(res
            .as_any_ref()
            .downcast_ref::<JoinFuncGen>()
            .ok_or("Downcast `OrderStep` error")?
            .gen())
    }

    fn compare(&self, res: &dyn Resource) -> CompileResult<Box<dyn CompareFunction<Traverser>>> {
        Ok(res.as_any_ref().downcast_ref::<OrderStep>().ok_or("Downcast `OrderStep` error")?.gen())
    }

    fn key(
        &self, res: &dyn Resource,
    ) -> CompileResult<Box<dyn KeyFunction<Traverser, Target = HashKey<Traverser>>>> {
        Ok(res.as_any_ref().downcast_ref::<GroupStep>().ok_or("Downcast `GraphStep` error")?.gen())
    }

    fn accumulate(
        &self, _: &dyn Resource,
    ) -> CompileResult<
        Box<
            dyn AccumFactory<
                Traverser,
                Traverser,
                Target = Box<dyn Accumulator<Traverser, Traverser>>,
            >,
        >,
    > {
        Err("compile accumulate factory is unimplemented")?
    }

    fn collect(
        &self, _: &dyn Resource,
    ) -> CompileResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Collection<Traverser>>>>>
    {
        Err("compile collection faction is unimplemented")?
    }

    fn set(
        &self, _res: &dyn Resource,
    ) -> CompileResult<
        Box<
            dyn DrainSetFactory<
                Traverser,
                Target = Box<
                    dyn DrainSet<Traverser, Target = Box<dyn Iterator<Item = Traverser> + Send>>,
                >,
            >,
        >,
    > {
        unimplemented!()
    }

    fn sink(&self, _res: &dyn Resource) -> CompileResult<Box<dyn EncodeFunction<Traverser>>> {
        Ok(Box::new(TraverserSinkEncoder))
    }
}
