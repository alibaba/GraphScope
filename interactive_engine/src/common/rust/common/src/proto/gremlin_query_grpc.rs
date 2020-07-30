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

const METHOD_GREMLIN_SERVICE_GET_EDGES: ::grpcio::Method<super::gremlin_query::EdgesRequest, super::gremlin_query::EdgesReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.GremlinService/getEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_GREMLIN_SERVICE_GET_VERTEXS: ::grpcio::Method<super::gremlin_query::VertexRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.GremlinService/getVertexs",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_GREMLIN_SERVICE_GET_LIMIT_EDGES: ::grpcio::Method<super::gremlin_query::LimitEdgeRequest, super::gremlin_query::LimitEdgesReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.GremlinService/getLimitEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_GREMLIN_SERVICE_SCAN: ::grpcio::Method<super::gremlin_query::VertexScanRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.GremlinService/scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct GremlinServiceClient {
    client: ::grpcio::Client,
}

impl GremlinServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        GremlinServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_edges_opt(&self, req: &super::gremlin_query::EdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::EdgesReponse>> {
        self.client.server_streaming(&METHOD_GREMLIN_SERVICE_GET_EDGES, req, opt)
    }

    pub fn get_edges(&self, req: &super::gremlin_query::EdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::EdgesReponse>> {
        self.get_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertexs_opt(&self, req: &super::gremlin_query::VertexRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_GREMLIN_SERVICE_GET_VERTEXS, req, opt)
    }

    pub fn get_vertexs(&self, req: &super::gremlin_query::VertexRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.get_vertexs_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_limit_edges_opt(&self, req: &super::gremlin_query::LimitEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::LimitEdgesReponse>> {
        self.client.server_streaming(&METHOD_GREMLIN_SERVICE_GET_LIMIT_EDGES, req, opt)
    }

    pub fn get_limit_edges(&self, req: &super::gremlin_query::LimitEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::LimitEdgesReponse>> {
        self.get_limit_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::gremlin_query::VertexScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_GREMLIN_SERVICE_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::gremlin_query::VertexScanRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait GremlinService {
    fn get_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_query::EdgesRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::EdgesReponse>);
    fn get_vertexs(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_query::VertexRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
    fn get_limit_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_query::LimitEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::LimitEdgesReponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_query::VertexScanRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
}

pub fn create_gremlin_service<S: GremlinService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_GREMLIN_SERVICE_GET_EDGES, move |ctx, req, resp| {
        instance.get_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_GREMLIN_SERVICE_GET_VERTEXS, move |ctx, req, resp| {
        instance.get_vertexs(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_GREMLIN_SERVICE_GET_LIMIT_EDGES, move |ctx, req, resp| {
        instance.get_limit_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_GREMLIN_SERVICE_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
