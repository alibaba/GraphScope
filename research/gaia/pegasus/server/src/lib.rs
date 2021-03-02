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
extern crate pegasus_common;
#[macro_use]
extern crate pegasus;
#[macro_use]
extern crate log;
#[cfg(not(feature = "gcip"))]
mod generated {
    pub mod protobuf {
        tonic::include_proto!("protobuf");
    }
}

#[cfg(feature = "gcip")]
mod generated {
    #[path = "protobuf.rs"]
    pub mod protobuf;
}

pub trait AnyData: Data + Eq {
    fn with<T: Data + Eq>(raw: T) -> Self;
}

pub mod client;
pub mod desc;
pub mod factory;
mod materialize;
pub mod rpc;
pub mod service;

pub use client::builder::BinaryResource;
pub use generated::protobuf::job_response::Result as JobResult;
use pegasus::Data;
