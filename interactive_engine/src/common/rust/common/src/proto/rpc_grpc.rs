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

const METHOD_TRANSFER_SERVICE_SEND: ::grpcio::Method<super::rpc::TransferRequest, super::rpc::TransferResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tinkerpop.TransferService/send",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct TransferServiceClient {
    client: ::grpcio::Client,
}

impl TransferServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        TransferServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn send_opt(&self, req: &super::rpc::TransferRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::rpc::TransferResponse> {
        self.client.unary_call(&METHOD_TRANSFER_SERVICE_SEND, req, opt)
    }

    pub fn send(&self, req: &super::rpc::TransferRequest) -> ::grpcio::Result<super::rpc::TransferResponse> {
        self.send_opt(req, ::grpcio::CallOption::default())
    }

    pub fn send_async_opt(&self, req: &super::rpc::TransferRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::rpc::TransferResponse>> {
        self.client.unary_call_async(&METHOD_TRANSFER_SERVICE_SEND, req, opt)
    }

    pub fn send_async(&self, req: &super::rpc::TransferRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::rpc::TransferResponse>> {
        self.send_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait TransferService {
    fn send(&mut self, ctx: ::grpcio::RpcContext, req: super::rpc::TransferRequest, sink: ::grpcio::UnarySink<super::rpc::TransferResponse>);
}

pub fn create_transfer_service<S: TransferService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_TRANSFER_SERVICE_SEND, move |ctx, req, resp| {
        instance.send(ctx, req, resp)
    });
    builder.build()
}
