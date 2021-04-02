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
extern crate fasthash;

extern crate graph_store;
use crate::process::traversal::traverser::{ShadeSync, Traverser};
pub use crate::structure::{get_graph, register_graph};
pub use crate::structure::{Element, GraphProxy, ID};
use pegasus::api::accum::{Count, ToList};
use pegasus::api::function::*;
use prost::Message;

pub mod process;
pub mod structure;

pub mod common;
pub mod compiler;
mod result_process;
mod storage;
use crate::common::serde_dyn::register_type;
use crate::result_process::result_to_pb;
use crate::structure::filter::codec::ParseError;
pub use common::object::Object;
pub use generated::gremlin::GremlinStep as GremlinStepPb;
use std::io;
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

/// A tricky bypassing of Rust's compiler. It is useful to simplify throwing a `DynError`
/// from a `&str` as `Err(str_to_dyn_err('some str'))`
fn str_to_dyn_error(str: &str) -> DynError {
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
    fn get_partition(&self, id: &ID, job_workers: u32) -> u64;
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

pub fn register_gremlin_types() -> io::Result<()> {
    register_type::<ShadeSync<(Traverser, Count<Traverser>)>>()?;
    register_type::<ShadeSync<(Traverser, ToList<Traverser>)>>()?;
    register_type::<ShadeSync<(Traverser, Traverser)>>()?;
    register_type::<ShadeSync<Vec<Traverser>>>()?;
    Ok(())
}
