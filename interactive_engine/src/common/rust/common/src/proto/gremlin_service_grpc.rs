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

const METHOD_MAX_GRAPH_SERVICE_QUERY: ::grpcio::Method<super::query_flow::QueryFlow, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.MaxGraphService/query",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_SERVICE_PREPARE: ::grpcio::Method<super::query_flow::QueryFlow, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.MaxGraphService/prepare",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_SERVICE_QUERY2: ::grpcio::Method<super::query_flow::Query, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.MaxGraphService/query2",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_SERVICE_EXECUTE: ::grpcio::Method<super::query_flow::QueryFlow, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.MaxGraphService/execute",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_SERVICE_REMOVE: ::grpcio::Method<super::message::RemoveDataflowRequest, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.MaxGraphService/remove",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct MaxGraphServiceClient {
    client: ::grpcio::Client,
}

impl MaxGraphServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        MaxGraphServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn query_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_MAX_GRAPH_SERVICE_QUERY, req, opt)
    }

    pub fn query(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.query_opt(req, ::grpcio::CallOption::default())
    }

    pub fn prepare_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_MAX_GRAPH_SERVICE_PREPARE, req, opt)
    }

    pub fn prepare(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<super::message::OperationResponse> {
        self.prepare_opt(req, ::grpcio::CallOption::default())
    }

    pub fn prepare_async_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_MAX_GRAPH_SERVICE_PREPARE, req, opt)
    }

    pub fn prepare_async(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.prepare_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn query2_opt(&self, req: &super::query_flow::Query, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_MAX_GRAPH_SERVICE_QUERY2, req, opt)
    }

    pub fn query2(&self, req: &super::query_flow::Query) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.query2_opt(req, ::grpcio::CallOption::default())
    }

    pub fn execute_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_MAX_GRAPH_SERVICE_EXECUTE, req, opt)
    }

    pub fn execute(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.execute_opt(req, ::grpcio::CallOption::default())
    }

    pub fn remove_opt(&self, req: &super::message::RemoveDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_MAX_GRAPH_SERVICE_REMOVE, req, opt)
    }

    pub fn remove(&self, req: &super::message::RemoveDataflowRequest) -> ::grpcio::Result<super::message::OperationResponse> {
        self.remove_opt(req, ::grpcio::CallOption::default())
    }

    pub fn remove_async_opt(&self, req: &super::message::RemoveDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_MAX_GRAPH_SERVICE_REMOVE, req, opt)
    }

    pub fn remove_async(&self, req: &super::message::RemoveDataflowRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.remove_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait MaxGraphService {
    fn query(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn prepare(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
    fn query2(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::Query, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn execute(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn remove(&mut self, ctx: ::grpcio::RpcContext, req: super::message::RemoveDataflowRequest, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
}

pub fn create_max_graph_service<S: MaxGraphService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_MAX_GRAPH_SERVICE_QUERY, move |ctx, req, resp| {
        instance.query(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_MAX_GRAPH_SERVICE_PREPARE, move |ctx, req, resp| {
        instance.prepare(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_MAX_GRAPH_SERVICE_QUERY2, move |ctx, req, resp| {
        instance.query2(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_MAX_GRAPH_SERVICE_EXECUTE, move |ctx, req, resp| {
        instance.execute(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_MAX_GRAPH_SERVICE_REMOVE, move |ctx, req, resp| {
        instance.remove(ctx, req, resp)
    });
    builder.build()
}

const METHOD_MAX_GRAPH_CTRL_SERVICE_SHOW_PROCESS_LIST: ::grpcio::Method<super::gremlin_service::ShowProcessListRequest, super::gremlin_service::ShowProcessListResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.MaxGraphCtrlService/showProcessList",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW: ::grpcio::Method<super::gremlin_service::CancelDataflowRequest, super::gremlin_service::CancelDataflowResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.MaxGraphCtrlService/cancelDataflow",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW_BY_FRONT: ::grpcio::Method<super::gremlin_service::CancelDataflowByFrontRequest, super::gremlin_service::CancelDataflowResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.MaxGraphCtrlService/cancelDataflowByFront",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct MaxGraphCtrlServiceClient {
    client: ::grpcio::Client,
}

impl MaxGraphCtrlServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        MaxGraphCtrlServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn show_process_list_opt(&self, req: &super::gremlin_service::ShowProcessListRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::gremlin_service::ShowProcessListResponse> {
        self.client.unary_call(&METHOD_MAX_GRAPH_CTRL_SERVICE_SHOW_PROCESS_LIST, req, opt)
    }

    pub fn show_process_list(&self, req: &super::gremlin_service::ShowProcessListRequest) -> ::grpcio::Result<super::gremlin_service::ShowProcessListResponse> {
        self.show_process_list_opt(req, ::grpcio::CallOption::default())
    }

    pub fn show_process_list_async_opt(&self, req: &super::gremlin_service::ShowProcessListRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::ShowProcessListResponse>> {
        self.client.unary_call_async(&METHOD_MAX_GRAPH_CTRL_SERVICE_SHOW_PROCESS_LIST, req, opt)
    }

    pub fn show_process_list_async(&self, req: &super::gremlin_service::ShowProcessListRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::ShowProcessListResponse>> {
        self.show_process_list_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cancel_dataflow_opt(&self, req: &super::gremlin_service::CancelDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::gremlin_service::CancelDataflowResponse> {
        self.client.unary_call(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW, req, opt)
    }

    pub fn cancel_dataflow(&self, req: &super::gremlin_service::CancelDataflowRequest) -> ::grpcio::Result<super::gremlin_service::CancelDataflowResponse> {
        self.cancel_dataflow_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cancel_dataflow_async_opt(&self, req: &super::gremlin_service::CancelDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::CancelDataflowResponse>> {
        self.client.unary_call_async(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW, req, opt)
    }

    pub fn cancel_dataflow_async(&self, req: &super::gremlin_service::CancelDataflowRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::CancelDataflowResponse>> {
        self.cancel_dataflow_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cancel_dataflow_by_front_opt(&self, req: &super::gremlin_service::CancelDataflowByFrontRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::gremlin_service::CancelDataflowResponse> {
        self.client.unary_call(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW_BY_FRONT, req, opt)
    }

    pub fn cancel_dataflow_by_front(&self, req: &super::gremlin_service::CancelDataflowByFrontRequest) -> ::grpcio::Result<super::gremlin_service::CancelDataflowResponse> {
        self.cancel_dataflow_by_front_opt(req, ::grpcio::CallOption::default())
    }

    pub fn cancel_dataflow_by_front_async_opt(&self, req: &super::gremlin_service::CancelDataflowByFrontRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::CancelDataflowResponse>> {
        self.client.unary_call_async(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW_BY_FRONT, req, opt)
    }

    pub fn cancel_dataflow_by_front_async(&self, req: &super::gremlin_service::CancelDataflowByFrontRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::gremlin_service::CancelDataflowResponse>> {
        self.cancel_dataflow_by_front_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait MaxGraphCtrlService {
    fn show_process_list(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_service::ShowProcessListRequest, sink: ::grpcio::UnarySink<super::gremlin_service::ShowProcessListResponse>);
    fn cancel_dataflow(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_service::CancelDataflowRequest, sink: ::grpcio::UnarySink<super::gremlin_service::CancelDataflowResponse>);
    fn cancel_dataflow_by_front(&mut self, ctx: ::grpcio::RpcContext, req: super::gremlin_service::CancelDataflowByFrontRequest, sink: ::grpcio::UnarySink<super::gremlin_service::CancelDataflowResponse>);
}

pub fn create_max_graph_ctrl_service<S: MaxGraphCtrlService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_MAX_GRAPH_CTRL_SERVICE_SHOW_PROCESS_LIST, move |ctx, req, resp| {
        instance.show_process_list(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW, move |ctx, req, resp| {
        instance.cancel_dataflow(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_MAX_GRAPH_CTRL_SERVICE_CANCEL_DATAFLOW_BY_FRONT, move |ctx, req, resp| {
        instance.cancel_dataflow_by_front(ctx, req, resp)
    });
    builder.build()
}

const METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY: ::grpcio::Method<super::query_flow::QueryFlow, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.AsyncMaxGraphService/asyncQuery",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_PREPARE: ::grpcio::Method<super::query_flow::QueryFlow, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.AsyncMaxGraphService/asyncPrepare",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY2: ::grpcio::Method<super::query_flow::Query, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.AsyncMaxGraphService/asyncQuery2",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_EXECUTE: ::grpcio::Method<super::query_flow::QueryFlow, super::message::QueryResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::ServerStreaming,
    name: "/maxgraph.AsyncMaxGraphService/asyncExecute",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_REMOVE: ::grpcio::Method<super::message::RemoveDataflowRequest, super::message::OperationResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/maxgraph.AsyncMaxGraphService/asyncRemove",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct AsyncMaxGraphServiceClient {
    client: ::grpcio::Client,
}

impl AsyncMaxGraphServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        AsyncMaxGraphServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn async_query_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY, req, opt)
    }

    pub fn async_query(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.async_query_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_prepare_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_PREPARE, req, opt)
    }

    pub fn async_prepare(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<super::message::OperationResponse> {
        self.async_prepare_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_prepare_async_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_PREPARE, req, opt)
    }

    pub fn async_prepare_async(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.async_prepare_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_query2_opt(&self, req: &super::query_flow::Query, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY2, req, opt)
    }

    pub fn async_query2(&self, req: &super::query_flow::Query) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.async_query2_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_execute_opt(&self, req: &super::query_flow::QueryFlow, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.client.server_streaming(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_EXECUTE, req, opt)
    }

    pub fn async_execute(&self, req: &super::query_flow::QueryFlow) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<super::message::QueryResponse>> {
        self.async_execute_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_remove_opt(&self, req: &super::message::RemoveDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::message::OperationResponse> {
        self.client.unary_call(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_REMOVE, req, opt)
    }

    pub fn async_remove(&self, req: &super::message::RemoveDataflowRequest) -> ::grpcio::Result<super::message::OperationResponse> {
        self.async_remove_opt(req, ::grpcio::CallOption::default())
    }

    pub fn async_remove_async_opt(&self, req: &super::message::RemoveDataflowRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.client.unary_call_async(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_REMOVE, req, opt)
    }

    pub fn async_remove_async(&self, req: &super::message::RemoveDataflowRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::message::OperationResponse>> {
        self.async_remove_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait AsyncMaxGraphService {
    fn async_query(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn async_prepare(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
    fn async_query2(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::Query, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn async_execute(&mut self, ctx: ::grpcio::RpcContext, req: super::query_flow::QueryFlow, sink: ::grpcio::ServerStreamingSink<super::message::QueryResponse>);
    fn async_remove(&mut self, ctx: ::grpcio::RpcContext, req: super::message::RemoveDataflowRequest, sink: ::grpcio::UnarySink<super::message::OperationResponse>);
}

pub fn create_async_max_graph_service<S: AsyncMaxGraphService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY, move |ctx, req, resp| {
        instance.async_query(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_PREPARE, move |ctx, req, resp| {
        instance.async_prepare(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_QUERY2, move |ctx, req, resp| {
        instance.async_query2(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_server_streaming_handler(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_EXECUTE, move |ctx, req, resp| {
        instance.async_execute(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_ASYNC_MAX_GRAPH_SERVICE_ASYNC_REMOVE, move |ctx, req, resp| {
        instance.async_remove(ctx, req, resp)
    });
    builder.build()
}
