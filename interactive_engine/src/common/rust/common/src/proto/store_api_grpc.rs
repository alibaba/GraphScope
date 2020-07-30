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

const METHOD_STORE_SERVICE_GET_OUT_EDGES: ::grpcio::Method<super::store_api::GetOutEdgesRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getOutEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_BATCH_OUT_TARGET: ::grpcio::Method<super::store_api::BatchVertexEdgeRequest, super::store_api::BatchVertexEdgeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getBatchOutTarget",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_BATCH_OUT_COUNT: ::grpcio::Method<super::store_api::BatchVertexEdgeRequest, super::store_api::BatchVertexCountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getBatchOutCount",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_IN_EDGES: ::grpcio::Method<super::store_api::GetInEdgesRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getInEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_BATCH_IN_TARGET: ::grpcio::Method<super::store_api::BatchVertexEdgeRequest, super::store_api::BatchVertexEdgeResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getBatchInTarget",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_BATCH_IN_COUNT: ::grpcio::Method<super::store_api::BatchVertexEdgeRequest, super::store_api::BatchVertexCountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getBatchInCount",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_VERTEXS: ::grpcio::Method<super::store_api::GetVertexsRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getVertexs",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_GET_EDGES: ::grpcio::Method<super::store_api::GetEdgesRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/getEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_SCAN_EDGES: ::grpcio::Method<super::store_api::ScanEdgeRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/scanEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_SERVICE_SCAN: ::grpcio::Method<super::store_api::ScanRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.StoreService/scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct StoreServiceClient {
    client: ::grpcio::Client,
}

impl StoreServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        StoreServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_out_edges_opt(&self, req: &super::store_api::GetOutEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_OUT_EDGES, req, opt)
    }

    pub fn get_out_edges(&self, req: &super::store_api::GetOutEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.get_out_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_out_target_opt(&self, req: &super::store_api::BatchVertexEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexEdgeResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_BATCH_OUT_TARGET, req, opt)
    }

    pub fn get_batch_out_target(&self, req: &super::store_api::BatchVertexEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexEdgeResponse>> {
        self.get_batch_out_target_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_out_count_opt(&self, req: &super::store_api::BatchVertexEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexCountResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_BATCH_OUT_COUNT, req, opt)
    }

    pub fn get_batch_out_count(&self, req: &super::store_api::BatchVertexEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexCountResponse>> {
        self.get_batch_out_count_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_in_edges_opt(&self, req: &super::store_api::GetInEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_IN_EDGES, req, opt)
    }

    pub fn get_in_edges(&self, req: &super::store_api::GetInEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.get_in_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_in_target_opt(&self, req: &super::store_api::BatchVertexEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexEdgeResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_BATCH_IN_TARGET, req, opt)
    }

    pub fn get_batch_in_target(&self, req: &super::store_api::BatchVertexEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexEdgeResponse>> {
        self.get_batch_in_target_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_in_count_opt(&self, req: &super::store_api::BatchVertexEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexCountResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_BATCH_IN_COUNT, req, opt)
    }

    pub fn get_batch_in_count(&self, req: &super::store_api::BatchVertexEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::BatchVertexCountResponse>> {
        self.get_batch_in_count_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertexs_opt(&self, req: &super::store_api::GetVertexsRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_VERTEXS, req, opt)
    }

    pub fn get_vertexs(&self, req: &super::store_api::GetVertexsRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.get_vertexs_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_edges_opt(&self, req: &super::store_api::GetEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_GET_EDGES, req, opt)
    }

    pub fn get_edges(&self, req: &super::store_api::GetEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.get_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_edges_opt(&self, req: &super::store_api::ScanEdgeRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_SCAN_EDGES, req, opt)
    }

    pub fn scan_edges(&self, req: &super::store_api::ScanEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.scan_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::store_api::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_STORE_SERVICE_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::store_api::ScanRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait StoreService {
    fn get_out_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::GetOutEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn get_batch_out_target(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::BatchVertexEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::BatchVertexEdgeResponse>);
    fn get_batch_out_count(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::BatchVertexEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::BatchVertexCountResponse>);
    fn get_in_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::GetInEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn get_batch_in_target(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::BatchVertexEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::BatchVertexEdgeResponse>);
    fn get_batch_in_count(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::BatchVertexEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::BatchVertexCountResponse>);
    fn get_vertexs(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::GetVertexsRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
    fn get_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::GetEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn scan_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::ScanEdgeRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::store_api::ScanRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
}

pub fn create_store_service<S: StoreService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_OUT_EDGES, move |ctx, req, resp| {
        instance.get_out_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_BATCH_OUT_TARGET, move |ctx, req, resp| {
        instance.get_batch_out_target(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_BATCH_OUT_COUNT, move |ctx, req, resp| {
        instance.get_batch_out_count(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_IN_EDGES, move |ctx, req, resp| {
        instance.get_in_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_BATCH_IN_TARGET, move |ctx, req, resp| {
        instance.get_batch_in_target(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_BATCH_IN_COUNT, move |ctx, req, resp| {
        instance.get_batch_in_count(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_VERTEXS, move |ctx, req, resp| {
        instance.get_vertexs(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_GET_EDGES, move |ctx, req, resp| {
        instance.get_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_SCAN_EDGES, move |ctx, req, resp| {
        instance.scan_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_STORE_SERVICE_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
