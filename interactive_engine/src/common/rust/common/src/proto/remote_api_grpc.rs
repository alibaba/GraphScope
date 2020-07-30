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

const METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT: ::grpcio::Method<super::remote_api::BatchVerticesEdgesRequest, super::remote_api::BatchVerticesEdgesResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getBatchOut",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN: ::grpcio::Method<super::remote_api::BatchVerticesEdgesRequest, super::remote_api::BatchVerticesEdgesResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getBatchIn",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT_CNT: ::grpcio::Method<super::remote_api::BatchVerticesEdgesRequest, super::remote_api::BatchVerticesCountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getBatchOutCnt",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN_CNT: ::grpcio::Method<super::remote_api::BatchVerticesEdgesRequest, super::remote_api::BatchVerticesCountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getBatchInCnt",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_GET_VERTICES: ::grpcio::Method<super::remote_api::VerticesRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getVertices",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_GET_GRAPH_EDGES: ::grpcio::Method<super::remote_api::GraphEdgesRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/getGraphEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_QUERY_VERTICES: ::grpcio::Method<super::remote_api::QueryRequest, super::gremlin_query::VertexResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/query_vertices",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_QUERY_EDGES: ::grpcio::Method<super::remote_api::QueryRequest, super::store_api::GraphEdgeReponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/tinkerpop.RemoteStoreService/query_edges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_VERTEX_COUNT: ::grpcio::Method<super::remote_api::QueryCountRequest, super::remote_api::CountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tinkerpop.RemoteStoreService/vertex_count",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_REMOTE_STORE_SERVICE_EDGE_COUNT: ::grpcio::Method<super::remote_api::QueryCountRequest, super::remote_api::CountResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/tinkerpop.RemoteStoreService/edge_count",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct RemoteStoreServiceClient {
    client: ::grpcio::Client,
}

impl RemoteStoreServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        RemoteStoreServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_batch_out_opt(&self, req: &super::remote_api::BatchVerticesEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesEdgesResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT, req, opt)
    }

    pub fn get_batch_out(&self, req: &super::remote_api::BatchVerticesEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesEdgesResponse>> {
        self.get_batch_out_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_in_opt(&self, req: &super::remote_api::BatchVerticesEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesEdgesResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN, req, opt)
    }

    pub fn get_batch_in(&self, req: &super::remote_api::BatchVerticesEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesEdgesResponse>> {
        self.get_batch_in_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_out_cnt_opt(&self, req: &super::remote_api::BatchVerticesEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesCountResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT_CNT, req, opt)
    }

    pub fn get_batch_out_cnt(&self, req: &super::remote_api::BatchVerticesEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesCountResponse>> {
        self.get_batch_out_cnt_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_batch_in_cnt_opt(&self, req: &super::remote_api::BatchVerticesEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesCountResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN_CNT, req, opt)
    }

    pub fn get_batch_in_cnt(&self, req: &super::remote_api::BatchVerticesEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::remote_api::BatchVerticesCountResponse>> {
        self.get_batch_in_cnt_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertices_opt(&self, req: &super::remote_api::VerticesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_VERTICES, req, opt)
    }

    pub fn get_vertices(&self, req: &super::remote_api::VerticesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.get_vertices_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_graph_edges_opt(&self, req: &super::remote_api::GraphEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_GET_GRAPH_EDGES, req, opt)
    }

    pub fn get_graph_edges(&self, req: &super::remote_api::GraphEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.get_graph_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query_vertices_opt(&self, req: &super::remote_api::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_QUERY_VERTICES, req, opt)
    }

    pub fn query_vertices(&self, req: &super::remote_api::QueryRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::gremlin_query::VertexResponse>> {
        self.query_vertices_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query_edges_opt(&self, req: &super::remote_api::QueryRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.client.server_streaming(&METHOD_REMOTE_STORE_SERVICE_QUERY_EDGES, req, opt)
    }

    pub fn query_edges(&self, req: &super::remote_api::QueryRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::store_api::GraphEdgeReponse>> {
        self.query_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn vertex_count_opt(&self, req: &super::remote_api::QueryCountRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::remote_api::CountResponse> {
        self.client.unary_call(&METHOD_REMOTE_STORE_SERVICE_VERTEX_COUNT, req, opt)
    }

    pub fn vertex_count(&self, req: &super::remote_api::QueryCountRequest) -> ::grpcio::Result<super::remote_api::CountResponse> {
        self.vertex_count_opt(req, ::grpcio::CallOption::default())
    }

    pub fn vertex_count_async_opt(&self, req: &super::remote_api::QueryCountRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::remote_api::CountResponse>> {
        self.client.unary_call_async(&METHOD_REMOTE_STORE_SERVICE_VERTEX_COUNT, req, opt)
    }

    pub fn vertex_count_async(&self, req: &super::remote_api::QueryCountRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::remote_api::CountResponse>> {
        self.vertex_count_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn edge_count_opt(&self, req: &super::remote_api::QueryCountRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::remote_api::CountResponse> {
        self.client.unary_call(&METHOD_REMOTE_STORE_SERVICE_EDGE_COUNT, req, opt)
    }

    pub fn edge_count(&self, req: &super::remote_api::QueryCountRequest) -> ::grpcio::Result<super::remote_api::CountResponse> {
        self.edge_count_opt(req, ::grpcio::CallOption::default())
    }

    pub fn edge_count_async_opt(&self, req: &super::remote_api::QueryCountRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::remote_api::CountResponse>> {
        self.client.unary_call_async(&METHOD_REMOTE_STORE_SERVICE_EDGE_COUNT, req, opt)
    }

    pub fn edge_count_async(&self, req: &super::remote_api::QueryCountRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::remote_api::CountResponse>> {
        self.edge_count_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait RemoteStoreService {
    fn get_batch_out(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::BatchVerticesEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::remote_api::BatchVerticesEdgesResponse>);
    fn get_batch_in(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::BatchVerticesEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::remote_api::BatchVerticesEdgesResponse>);
    fn get_batch_out_cnt(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::BatchVerticesEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::remote_api::BatchVerticesCountResponse>);
    fn get_batch_in_cnt(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::BatchVerticesEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::remote_api::BatchVerticesCountResponse>);
    fn get_vertices(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::VerticesRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
    fn get_graph_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::GraphEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn query_vertices(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::QueryRequest, sink: ::grpcio::ServerStreamingSink<super::gremlin_query::VertexResponse>);
    fn query_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::QueryRequest, sink: ::grpcio::ServerStreamingSink<super::store_api::GraphEdgeReponse>);
    fn vertex_count(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::QueryCountRequest, sink: ::grpcio::UnarySink<super::remote_api::CountResponse>);
    fn edge_count(&mut self, ctx: ::grpcio::RpcContext, req: super::remote_api::QueryCountRequest, sink: ::grpcio::UnarySink<super::remote_api::CountResponse>);
}

pub fn create_remote_store_service<S: RemoteStoreService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT, move |ctx, req, resp| {
        instance.get_batch_out(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN, move |ctx, req, resp| {
        instance.get_batch_in(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_OUT_CNT, move |ctx, req, resp| {
        instance.get_batch_out_cnt(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_BATCH_IN_CNT, move |ctx, req, resp| {
        instance.get_batch_in_cnt(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_VERTICES, move |ctx, req, resp| {
        instance.get_vertices(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_GET_GRAPH_EDGES, move |ctx, req, resp| {
        instance.get_graph_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_QUERY_VERTICES, move |ctx, req, resp| {
        instance.query_vertices(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_REMOTE_STORE_SERVICE_QUERY_EDGES, move |ctx, req, resp| {
        instance.query_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_REMOTE_STORE_SERVICE_VERTEX_COUNT, move |ctx, req, resp| {
        instance.vertex_count(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_REMOTE_STORE_SERVICE_EDGE_COUNT, move |ctx, req, resp| {
        instance.edge_count(ctx, req, resp)
    });
    builder.build()
}
