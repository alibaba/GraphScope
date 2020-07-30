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

const METHOD_LAMBDA_SERVICE_PREPARE: ::grpcio::Method<super::lambda_service::LambdaBase, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.LambdaService/prepare",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LAMBDA_SERVICE_REMOVE: ::grpcio::Method<super::lambda_service::LambdaBase, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.LambdaService/remove",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LAMBDA_SERVICE_FILTER: ::grpcio::Method<super::lambda_service::LambdaData, super::lambda_service::LambdaResult> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.LambdaService/filter",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LAMBDA_SERVICE_MAP: ::grpcio::Method<super::lambda_service::LambdaData, super::lambda_service::LambdaResult> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.LambdaService/map",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_LAMBDA_SERVICE_FLATMAP: ::grpcio::Method<super::lambda_service::LambdaData, super::lambda_service::LambdaResult> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.LambdaService/flatmap",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct LambdaServiceClient {
    client: ::grpcio::Client,
}

impl LambdaServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        LambdaServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn prepare_opt(&self, req: &super::lambda_service::LambdaBase, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_LAMBDA_SERVICE_PREPARE, req, opt)
    }

    pub fn prepare(&self, req: &super::lambda_service::LambdaBase) -> ::grpcio::Result<super::message::OperationResponse> {
        self.prepare_opt(req, ::grpcio::CallOption::default())
    }

    pub fn prepare_async_opt(&self, req: &super::lambda_service::LambdaBase, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_LAMBDA_SERVICE_PREPARE, req, opt)
    }

    pub fn prepare_async(&self, req: &super::lambda_service::LambdaBase) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.prepare_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn remove_opt(&self, req: &super::lambda_service::LambdaBase, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_LAMBDA_SERVICE_REMOVE, req, opt)
    }

    pub fn remove(&self, req: &super::lambda_service::LambdaBase) -> ::grpcio::Result<super::message::OperationResponse> {
        self.remove_opt(req, ::grpcio::CallOption::default())
    }

    pub fn remove_async_opt(&self, req: &super::lambda_service::LambdaBase, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_LAMBDA_SERVICE_REMOVE, req, opt)
    }

    pub fn remove_async(&self, req: &super::lambda_service::LambdaBase) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.remove_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn filter_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.client.unary_call(&METHOD_LAMBDA_SERVICE_FILTER, req, opt)
    }

    pub fn filter(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.filter_opt(req, ::grpcio::CallOption::default())
    }

    pub fn filter_async_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.client.unary_call_async(&METHOD_LAMBDA_SERVICE_FILTER, req, opt)
    }

    pub fn filter_async(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.filter_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn map_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.client.unary_call(&METHOD_LAMBDA_SERVICE_MAP, req, opt)
    }

    pub fn map(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.map_opt(req, ::grpcio::CallOption::default())
    }

    pub fn map_async_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.client.unary_call_async(&METHOD_LAMBDA_SERVICE_MAP, req, opt)
    }

    pub fn map_async(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.map_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn flatmap_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.client.unary_call(&METHOD_LAMBDA_SERVICE_FLATMAP, req, opt)
    }

    pub fn flatmap(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<super::lambda_service::LambdaResult> {
        self.flatmap_opt(req, ::grpcio::CallOption::default())
    }

    pub fn flatmap_async_opt(&self, req: &super::lambda_service::LambdaData, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.client.unary_call_async(&METHOD_LAMBDA_SERVICE_FLATMAP, req, opt)
    }

    pub fn flatmap_async(&self, req: &super::lambda_service::LambdaData) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::lambda_service::LambdaResult>> {
        self.flatmap_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait LambdaService {
    fn prepare(&mut self, ctx: ::grpcio::RpcContext, req: super::lambda_service::LambdaBase, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
    fn remove(&mut self, ctx: ::grpcio::RpcContext, req: super::lambda_service::LambdaBase, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
    fn filter(&mut self, ctx: ::grpcio::RpcContext, req: super::lambda_service::LambdaData, sink: ::grpcio::UnarySink<super::lambda_service::LambdaResult>);
    fn map(&mut self, ctx: ::grpcio::RpcContext, req: super::lambda_service::LambdaData, sink: ::grpcio::UnarySink<super::lambda_service::LambdaResult>);
    fn flatmap(&mut self, ctx: ::grpcio::RpcContext, req: super::lambda_service::LambdaData, sink: ::grpcio::UnarySink<super::lambda_service::LambdaResult>);
}

pub fn create_lambda_service<S: LambdaService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LAMBDA_SERVICE_PREPARE, move |ctx, req, resp| {
        instance.prepare(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LAMBDA_SERVICE_REMOVE, move |ctx, req, resp| {
        instance.remove(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LAMBDA_SERVICE_FILTER, move |ctx, req, resp| {
        instance.filter(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LAMBDA_SERVICE_MAP, move |ctx, req, resp| {
        instance.map(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_LAMBDA_SERVICE_FLATMAP, move |ctx, req, resp| {
        instance.flatmap(ctx, req, resp)
    });
    builder.build()
}
