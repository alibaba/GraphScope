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

const METHOD_SCHEDULER_API_RESTART_WORKER_MANUALLY: ::grpcio::Method<super::scheduler_monitor::RestartWorkerReq, super::common::Response> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/SchedulerApi/restartWorkerManually",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SCHEDULER_API_GET_SCHEDULER_EVENT: ::grpcio::Method<super::scheduler_monitor::SchedulerEventReq, super::scheduler_monitor::SchedulerEventResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/SchedulerApi/getSchedulerEvent",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct SchedulerApiClient {
    client: ::grpcio::Client,
}

impl SchedulerApiClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        SchedulerApiClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn restart_worker_manually_opt(&self, req: &super::scheduler_monitor::RestartWorkerReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::Response> {
        self.client.unary_call(&METHOD_SCHEDULER_API_RESTART_WORKER_MANUALLY, req, opt)
    }

    pub fn restart_worker_manually(&self, req: &super::scheduler_monitor::RestartWorkerReq) -> ::grpcio::Result<super::common::Response> {
        self.restart_worker_manually_opt(req, ::grpcio::CallOption::default())
    }

    pub fn restart_worker_manually_async_opt(&self, req: &super::scheduler_monitor::RestartWorkerReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.client.unary_call_async(&METHOD_SCHEDULER_API_RESTART_WORKER_MANUALLY, req, opt)
    }

    pub fn restart_worker_manually_async(&self, req: &super::scheduler_monitor::RestartWorkerReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Response>> {
        self.restart_worker_manually_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_scheduler_event_opt(&self, req: &super::scheduler_monitor::SchedulerEventReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::scheduler_monitor::SchedulerEventResp> {
        self.client.unary_call(&METHOD_SCHEDULER_API_GET_SCHEDULER_EVENT, req, opt)
    }

    pub fn get_scheduler_event(&self, req: &super::scheduler_monitor::SchedulerEventReq) -> ::grpcio::Result<super::scheduler_monitor::SchedulerEventResp> {
        self.get_scheduler_event_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_scheduler_event_async_opt(&self, req: &super::scheduler_monitor::SchedulerEventReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::scheduler_monitor::SchedulerEventResp>> {
        self.client.unary_call_async(&METHOD_SCHEDULER_API_GET_SCHEDULER_EVENT, req, opt)
    }

    pub fn get_scheduler_event_async(&self, req: &super::scheduler_monitor::SchedulerEventReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::scheduler_monitor::SchedulerEventResp>> {
        self.get_scheduler_event_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait SchedulerApi {
    fn restart_worker_manually(&mut self, ctx: ::grpcio::RpcContext, req: super::scheduler_monitor::RestartWorkerReq, sink: ::grpcio::UnarySink<super::common::Response>);
    fn get_scheduler_event(&mut self, ctx: ::grpcio::RpcContext, req: super::scheduler_monitor::SchedulerEventReq, sink: ::grpcio::UnarySink<super::scheduler_monitor::SchedulerEventResp>);
}

pub fn create_scheduler_api<S: SchedulerApi + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SCHEDULER_API_RESTART_WORKER_MANUALLY, move |ctx, req, resp| {
        instance.restart_worker_manually(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SCHEDULER_API_GET_SCHEDULER_EVENT, move |ctx, req, resp| {
        instance.get_scheduler_event(ctx, req, resp)
    });
    builder.build()
}
