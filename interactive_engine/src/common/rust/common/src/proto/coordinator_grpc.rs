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

// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_COORDINATOR_HEARTBEAT: ::grpcio::Method<super::coordinator::HeartbeartRequest, super::coordinator::HeartbeartResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/Coordinator/heartbeat",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct CoordinatorClient {
    client: ::grpcio::Client,
}

impl CoordinatorClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        CoordinatorClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn heartbeat_opt(&self, req: &super::coordinator::HeartbeartRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::coordinator::HeartbeartResponse> {
        self.client.unary_call(&METHOD_COORDINATOR_HEARTBEAT, req, opt)
    }

    pub fn heartbeat(&self, req: &super::coordinator::HeartbeartRequest) -> ::grpcio::Result<super::coordinator::HeartbeartResponse> {
        self.heartbeat_opt(req, ::grpcio::CallOption::default())
    }

    pub fn heartbeat_async_opt(&self, req: &super::coordinator::HeartbeartRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::coordinator::HeartbeartResponse>> {
        self.client.unary_call_async(&METHOD_COORDINATOR_HEARTBEAT, req, opt)
    }

    pub fn heartbeat_async(&self, req: &super::coordinator::HeartbeartRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::coordinator::HeartbeartResponse>> {
        self.heartbeat_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Coordinator {
    fn heartbeat(&mut self, ctx: ::grpcio::RpcContext, req: super::coordinator::HeartbeartRequest, sink: ::grpcio::UnarySink<super::coordinator::HeartbeartResponse>);
}

pub fn create_coordinator<S: Coordinator + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_COORDINATOR_HEARTBEAT, move |ctx, req, resp| {
        instance.heartbeat(ctx, req, resp)
    });
    builder.build()
}
