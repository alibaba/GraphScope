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

const METHOD_DEBUG_SERVICE_API_GET_SERVER_INFO: ::grpcio::Method<super::common::Empty, super::debug::ServerInfo> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/DebugServiceApi/getServerInfo",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_GET_GRAPH_INFO: ::grpcio::Method<super::common::Empty, super::debug::GraphInfo> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/DebugServiceApi/getGraphInfo",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_GET_VERTEX: ::grpcio::Method<super::debug::GetVertexRequest, super::debug::VertexProto> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/DebugServiceApi/getVertex",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_SCAN_VERTEX: ::grpcio::Method<super::debug::ScanVertexRequest, super::debug::VertexProto> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/DebugServiceApi/scanVertex",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_GET_OUT_EDGES: ::grpcio::Method<super::debug::GetOutEdgesRequest, super::debug::EdgeProto> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/DebugServiceApi/getOutEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_GET_IN_EDGES: ::grpcio::Method<super::debug::GetInEdgesRequest, super::debug::EdgeProto> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/DebugServiceApi/getInEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_DEBUG_SERVICE_API_GET_SCHEMA: ::grpcio::Method<super::debug::GetSchemaRequest, super::schema::SchemaProto> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/DebugServiceApi/getSchema",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct DebugServiceApiClient {
    client: ::grpcio::Client,
}

impl DebugServiceApiClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        DebugServiceApiClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_server_info_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debug::ServerInfo> {
        self.client.unary_call(&METHOD_DEBUG_SERVICE_API_GET_SERVER_INFO, req, opt)
    }

    pub fn get_server_info(&self, req: &super::common::Empty) -> ::grpcio::Result<super::debug::ServerInfo> {
        self.get_server_info_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_server_info_async_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::ServerInfo>> {
        self.client.unary_call_async(&METHOD_DEBUG_SERVICE_API_GET_SERVER_INFO, req, opt)
    }

    pub fn get_server_info_async(&self, req: &super::common::Empty) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::ServerInfo>> {
        self.get_server_info_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_graph_info_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debug::GraphInfo> {
        self.client.unary_call(&METHOD_DEBUG_SERVICE_API_GET_GRAPH_INFO, req, opt)
    }

    pub fn get_graph_info(&self, req: &super::common::Empty) -> ::grpcio::Result<super::debug::GraphInfo> {
        self.get_graph_info_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_graph_info_async_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::GraphInfo>> {
        self.client.unary_call_async(&METHOD_DEBUG_SERVICE_API_GET_GRAPH_INFO, req, opt)
    }

    pub fn get_graph_info_async(&self, req: &super::common::Empty) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::GraphInfo>> {
        self.get_graph_info_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertex_opt(&self, req: &super::debug::GetVertexRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::VertexProto>> {
        self.client.server_streaming(&METHOD_DEBUG_SERVICE_API_GET_VERTEX, req, opt)
    }

    pub fn get_vertex(&self, req: &super::debug::GetVertexRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::VertexProto>> {
        self.get_vertex_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_vertex_opt(&self, req: &super::debug::ScanVertexRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::VertexProto>> {
        self.client.server_streaming(&METHOD_DEBUG_SERVICE_API_SCAN_VERTEX, req, opt)
    }

    pub fn scan_vertex(&self, req: &super::debug::ScanVertexRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::VertexProto>> {
        self.scan_vertex_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_out_edges_opt(&self, req: &super::debug::GetOutEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::EdgeProto>> {
        self.client.server_streaming(&METHOD_DEBUG_SERVICE_API_GET_OUT_EDGES, req, opt)
    }

    pub fn get_out_edges(&self, req: &super::debug::GetOutEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::EdgeProto>> {
        self.get_out_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_in_edges_opt(&self, req: &super::debug::GetInEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::EdgeProto>> {
        self.client.server_streaming(&METHOD_DEBUG_SERVICE_API_GET_IN_EDGES, req, opt)
    }

    pub fn get_in_edges(&self, req: &super::debug::GetInEdgesRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::debug::EdgeProto>> {
        self.get_in_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_schema_opt(&self, req: &super::debug::GetSchemaRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::schema::SchemaProto> {
        self.client.unary_call(&METHOD_DEBUG_SERVICE_API_GET_SCHEMA, req, opt)
    }

    pub fn get_schema(&self, req: &super::debug::GetSchemaRequest) -> ::grpcio::Result<super::schema::SchemaProto> {
        self.get_schema_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_schema_async_opt(&self, req: &super::debug::GetSchemaRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::schema::SchemaProto>> {
        self.client.unary_call_async(&METHOD_DEBUG_SERVICE_API_GET_SCHEMA, req, opt)
    }

    pub fn get_schema_async(&self, req: &super::debug::GetSchemaRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::schema::SchemaProto>> {
        self.get_schema_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait DebugServiceApi {
    fn get_server_info(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Empty, sink: ::grpcio::UnarySink<super::debug::ServerInfo>);
    fn get_graph_info(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Empty, sink: ::grpcio::UnarySink<super::debug::GraphInfo>);
    fn get_vertex(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetVertexRequest, sink: ::grpcio::ServerStreamingSink<super::debug::VertexProto>);
    fn scan_vertex(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::ScanVertexRequest, sink: ::grpcio::ServerStreamingSink<super::debug::VertexProto>);
    fn get_out_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetOutEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::debug::EdgeProto>);
    fn get_in_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetInEdgesRequest, sink: ::grpcio::ServerStreamingSink<super::debug::EdgeProto>);
    fn get_schema(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetSchemaRequest, sink: ::grpcio::UnarySink<super::schema::SchemaProto>);
}

pub fn create_debug_service_api<S: DebugServiceApi + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_SERVICE_API_GET_SERVER_INFO, move |ctx, req, resp| {
        instance.get_server_info(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_SERVICE_API_GET_GRAPH_INFO, move |ctx, req, resp| {
        instance.get_graph_info(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_DEBUG_SERVICE_API_GET_VERTEX, move |ctx, req, resp| {
        instance.get_vertex(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_DEBUG_SERVICE_API_SCAN_VERTEX, move |ctx, req, resp| {
        instance.scan_vertex(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_DEBUG_SERVICE_API_GET_OUT_EDGES, move |ctx, req, resp| {
        instance.get_out_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_DEBUG_SERVICE_API_GET_IN_EDGES, move |ctx, req, resp| {
        instance.get_in_edges(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_DEBUG_SERVICE_API_GET_SCHEMA, move |ctx, req, resp| {
        instance.get_schema(ctx, req, resp)
    });
    builder.build()
}

const METHOD_STORE_TEST_SERVICE_GET_SERVER_INFO: ::grpcio::Method<super::common::Empty, super::debug::ServerInfo> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/StoreTestService/getServerInfo",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_TEST_SERVICE_GET_VERTEX: ::grpcio::Method<super::debug::GetVertexRequest, super::debug::StoreTestResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/StoreTestService/getVertex",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_STORE_TEST_SERVICE_GET_OUT_EDGES: ::grpcio::Method<super::debug::GetOutEdgesRequest, super::debug::StoreTestResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/StoreTestService/getOutEdges",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct StoreTestServiceClient {
    client: ::grpcio::Client,
}

impl StoreTestServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        StoreTestServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_server_info_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debug::ServerInfo> {
        self.client.unary_call(&METHOD_STORE_TEST_SERVICE_GET_SERVER_INFO, req, opt)
    }

    pub fn get_server_info(&self, req: &super::common::Empty) -> ::grpcio::Result<super::debug::ServerInfo> {
        self.get_server_info_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_server_info_async_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::ServerInfo>> {
        self.client.unary_call_async(&METHOD_STORE_TEST_SERVICE_GET_SERVER_INFO, req, opt)
    }

    pub fn get_server_info_async(&self, req: &super::common::Empty) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::ServerInfo>> {
        self.get_server_info_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertex_opt(&self, req: &super::debug::GetVertexRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debug::StoreTestResponse> {
        self.client.unary_call(&METHOD_STORE_TEST_SERVICE_GET_VERTEX, req, opt)
    }

    pub fn get_vertex(&self, req: &super::debug::GetVertexRequest) -> ::grpcio::Result<super::debug::StoreTestResponse> {
        self.get_vertex_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_vertex_async_opt(&self, req: &super::debug::GetVertexRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::StoreTestResponse>> {
        self.client.unary_call_async(&METHOD_STORE_TEST_SERVICE_GET_VERTEX, req, opt)
    }

    pub fn get_vertex_async(&self, req: &super::debug::GetVertexRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::StoreTestResponse>> {
        self.get_vertex_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_out_edges_opt(&self, req: &super::debug::GetOutEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::debug::StoreTestResponse> {
        self.client.unary_call(&METHOD_STORE_TEST_SERVICE_GET_OUT_EDGES, req, opt)
    }

    pub fn get_out_edges(&self, req: &super::debug::GetOutEdgesRequest) -> ::grpcio::Result<super::debug::StoreTestResponse> {
        self.get_out_edges_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_out_edges_async_opt(&self, req: &super::debug::GetOutEdgesRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::StoreTestResponse>> {
        self.client.unary_call_async(&METHOD_STORE_TEST_SERVICE_GET_OUT_EDGES, req, opt)
    }

    pub fn get_out_edges_async(&self, req: &super::debug::GetOutEdgesRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::debug::StoreTestResponse>> {
        self.get_out_edges_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait StoreTestService {
    fn get_server_info(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Empty, sink: ::grpcio::UnarySink<super::debug::ServerInfo>);
    fn get_vertex(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetVertexRequest, sink: ::grpcio::UnarySink<super::debug::StoreTestResponse>);
    fn get_out_edges(&mut self, ctx: ::grpcio::RpcContext, req: super::debug::GetOutEdgesRequest, sink: ::grpcio::UnarySink<super::debug::StoreTestResponse>);
}

pub fn create_store_test_service<S: StoreTestService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_STORE_TEST_SERVICE_GET_SERVER_INFO, move |ctx, req, resp| {
        instance.get_server_info(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_STORE_TEST_SERVICE_GET_VERTEX, move |ctx, req, resp| {
        instance.get_vertex(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_STORE_TEST_SERVICE_GET_OUT_EDGES, move |ctx, req, resp| {
        instance.get_out_edges(ctx, req, resp)
    });
    builder.build()
}
