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

const METHOD_META_SERVICE_GET_STORE_LIST: ::grpcio::Method<super::meta_service::GetStoreListRequest, super::meta_service::GetStoreListResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph_store.MetaService/get_store_list",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct MetaServiceClient {
    client: ::grpcio::Client,
}

impl MetaServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        MetaServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_store_list_opt(&self, req: &super::meta_service::GetStoreListRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::meta_service::GetStoreListResponse> {
        self.client.unary_call(&METHOD_META_SERVICE_GET_STORE_LIST, req, opt)
    }

    pub fn get_store_list(&self, req: &super::meta_service::GetStoreListRequest) -> ::grpcio::Result<super::meta_service::GetStoreListResponse> {
        self.get_store_list_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_store_list_async_opt(&self, req: &super::meta_service::GetStoreListRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::meta_service::GetStoreListResponse>> {
        self.client.unary_call_async(&METHOD_META_SERVICE_GET_STORE_LIST, req, opt)
    }

    pub fn get_store_list_async(&self, req: &super::meta_service::GetStoreListRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::meta_service::GetStoreListResponse>> {
        self.get_store_list_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait MetaService {
    fn get_store_list(&mut self, ctx: ::grpcio::RpcContext, req: super::meta_service::GetStoreListRequest, sink: ::grpcio::UnarySink<super::meta_service::GetStoreListResponse>);
}

pub fn create_meta_service<S: MetaService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_META_SERVICE_GET_STORE_LIST, move |ctx, req, resp| {
        instance.get_store_list(ctx, req, resp)
    });
    builder.build()
}
