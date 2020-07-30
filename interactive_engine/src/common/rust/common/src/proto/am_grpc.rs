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

const METHOD_APP_MASTER_API_RESTART_WORKER: ::grpcio::Method<super::am::RestartWorkerRequest, super::common::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/AppMasterApi/restartWorker",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_APP_MASTER_API_KILL_WORKER: ::grpcio::Method<super::am::KillWorkerRequest, super::common::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/AppMasterApi/killWorker",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct AppMasterApiClient {
    client: ::grpcio::Client,
}

impl AppMasterApiClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        AppMasterApiClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn restart_worker_opt(&self, req: &super::am::RestartWorkerRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::Response> {
        self.client.unary_call(&METHOD_APP_MASTER_API_RESTART_WORKER, req, opt)
    }

    pub fn restart_worker(&self, req: &super::am::RestartWorkerRequest) -> ::grpcio::Result<super::common::Response> {
        self.restart_worker_opt(req, ::grpcio::CallOption::default())
    }

    pub fn restart_worker_async_opt(&self, req: &super::am::RestartWorkerRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.client.unary_call_async(&METHOD_APP_MASTER_API_RESTART_WORKER, req, opt)
    }

    pub fn restart_worker_async(&self, req: &super::am::RestartWorkerRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.restart_worker_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kill_worker_opt(&self, req: &super::am::KillWorkerRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::Response> {
        self.client.unary_call(&METHOD_APP_MASTER_API_KILL_WORKER, req, opt)
    }

    pub fn kill_worker(&self, req: &super::am::KillWorkerRequest) -> ::grpcio::Result<super::common::Response> {
        self.kill_worker_opt(req, ::grpcio::CallOption::default())
    }

    pub fn kill_worker_async_opt(&self, req: &super::am::KillWorkerRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.client.unary_call_async(&METHOD_APP_MASTER_API_KILL_WORKER, req, opt)
    }

    pub fn kill_worker_async(&self, req: &super::am::KillWorkerRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.kill_worker_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait AppMasterApi {
    fn restart_worker(&mut self, ctx: ::grpcio::RpcContext, req: super::am::RestartWorkerRequest, sink: ::grpcio::UnarySink<super::common::Response>);
    fn kill_worker(&mut self, ctx: ::grpcio::RpcContext, req: super::am::KillWorkerRequest, sink: ::grpcio::UnarySink<super::common::Response>);
}

pub fn create_app_master_api<S: AppMasterApi + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_APP_MASTER_API_RESTART_WORKER, move |ctx, req, resp| {
        instance.restart_worker(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_APP_MASTER_API_KILL_WORKER, move |ctx, req, resp| {
        instance.kill_worker(ctx, req, resp)
    });
    builder.build()
}
