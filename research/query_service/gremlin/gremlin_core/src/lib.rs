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
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate pegasus_common;
#[macro_use]
extern crate pegasus;
#[macro_use]
extern crate log;
extern crate graph_store;
#[macro_use]
extern crate dyn_type;

use crate::process::traversal::traverser::{ShadeSync, Traverser};
pub use crate::structure::{get_graph, register_graph};
pub use crate::structure::{Element, GraphProxy, ID};

pub mod process;
pub mod structure;

pub mod compiler;
#[macro_use]
pub mod graph_proxy;

use crate::process::traversal::path::ResultPath;

use crate::structure::filter::codec::ParseError;
pub use generated::gremlin::GremlinStep as GremlinStepPb;
pub use graph_proxy::{create_demo_graph, ID_MASK};
pub use graph_store::utils::IterList;
use std::io;

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
pub type DynIter<T> = Box<dyn Iterator<Item = T> + Send>;

impl From<ParseError> for DynError {
    fn from(e: ParseError) -> Self {
        let err: Box<dyn std::error::Error + Send + Sync> = format!("Parse error: {}", e).into();
        err
    }
}

/// A tricky bypassing of Rust's compiler. It is useful to simplify throwing a `DynError`
/// from a `&str` as `Err(str_to_dyn_err('some str'))`
pub fn str_to_dyn_error(str: &str) -> DynError {
    let err: Box<dyn std::error::Error + Send + Sync> = str.into();
    err
}

/// While it is frequently needed to transfer a proto-buf structure into a Rust structure,
/// we use this `trait` to support the transformation while capture any possible error.
pub trait FromPb<T> {
    /// A function to transfer a proto-buf structure into a Rust structure
    /// TODO(longbin) `ParseError` is likely moved out of its current module for its more common usage.
    fn from_pb(t: T) -> Result<Self, ParseError>
    where
        Self: Sized;
}

pub trait Partitioner: Send + Sync + 'static {
    fn get_partition(&self, id: &ID, job_workers: usize) -> DynResult<u64>;
    /// Given job_workers (number of worker per server) and worker_id (worker index),
    /// return the partition list that the worker is going to process
    fn get_worker_partitions(
        &self, job_workers: usize, worker_id: u32,
    ) -> DynResult<Option<Vec<u64>>>;
}

/// A simple partition utility that one server contains a single graph partition
pub struct Partition {
    pub num_servers: usize,
}

impl Partitioner for Partition {
    fn get_partition(&self, id: &ID, workers: usize) -> DynResult<u64> {
        let id_usize = (*id & (ID_MASK)) as usize;
        let magic_num = id_usize / self.num_servers;
        // The partitioning logics is as follows:
        // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
        // to the machine R that holds its data.
        // 2. `R * workers` shifts the worker's id in the machine R.
        // 3. `magic_num % workers` then picks up one of the workers in the machine R
        // to do the computation.
        Ok(((id_usize - magic_num * self.num_servers) * workers + magic_num % workers) as u64)
    }

    fn get_worker_partitions(
        &self, job_workers: usize, worker_id: u32,
    ) -> DynResult<Option<Vec<u64>>> {
        // In graph that one server contains a single graph partition,
        // we assign the first worker on current server to process (scan) the partition,
        // and we assume the partition id is identity to the server id
        if worker_id as usize % job_workers == 0 {
            Ok(Some(vec![worker_id as u64 / job_workers as u64]))
        } else {
            Ok(None)
        }
    }
}

pub fn register_gremlin_types() -> io::Result<()> {
    dyn_type::register_type::<ShadeSync<(Traverser, Traverser)>>()?;
    dyn_type::register_type::<ShadeSync<Vec<Traverser>>>()?;
    dyn_type::register_type::<ResultPath>()?;
    Ok(())
}
