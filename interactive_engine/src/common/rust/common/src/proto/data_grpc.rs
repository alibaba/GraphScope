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

const METHOD_SERVER_DATA_API_GET_INSTANCE_INFO: ::grpcio::Method<super::common::Request, super::data::InstanceInfoResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getInstanceInfo",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_HEARTBEAT: ::grpcio::Method<super::hb::ServerHBReq, super::hb::ServerHBResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/heartbeat",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_UPDATE_RUNTIME_ENV: ::grpcio::Method<super::cluster::RuntimeEnv, super::cluster::RuntimeEnvList> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/updateRuntimeEnv",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_RESET_RUNTIME_ENV: ::grpcio::Method<super::common::Empty, super::common::Empty> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/resetRuntimeEnv",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_SIMPLE_HEARTBEAT: ::grpcio::Method<super::hb::SimpleServerHBReq, super::hb::SimpleServerHBResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/simpleHeartbeat",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_RUNTIME_GROUP_STATUS: ::grpcio::Method<super::common::Empty, super::data::RuntimeGroupStatusResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getRuntimeGroupStatus",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_IS_DATA_PATH_IN_USE: ::grpcio::Method<super::cluster::ServerIdAliveIdProto, super::data::DataPathStatusResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/isDataPathInUse",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST: ::grpcio::Method<super::common::Request, super::hb::RoutingServerInfoResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getWorkerInfoAndRoutingServerList",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_REAL_TIME_METRIC: ::grpcio::Method<super::common::MetricInfoRequest, super::common::MetricInfoResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getRealTimeMetric",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_ALL_REAL_TIME_METRICS: ::grpcio::Method<super::common::Request, super::common::AllMetricsInfoResp> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getAllRealTimeMetrics",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_EXECUTOR_ALIVE_ID: ::grpcio::Method<super::data::GetExecutorAliveIdRequest, super::data::GetExecutorAliveIdResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getExecutorAliveId",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_SERVER_DATA_API_GET_PARTITION_ASSIGNMENT: ::grpcio::Method<super::data::GetPartitionAssignmentRequest, super::data::GetPartitionAssignmentResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/ServerDataApi/getPartitionAssignment",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct ServerDataApiClient {
    client: ::grpcio::Client,
}

impl ServerDataApiClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ServerDataApiClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_instance_info_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::data::InstanceInfoResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_INSTANCE_INFO, req, opt)
    }

    pub fn get_instance_info(&self, req: &super::common::Request) -> ::grpcio::Result<super::data::InstanceInfoResp> {
        self.get_instance_info_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_instance_info_async_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::InstanceInfoResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_INSTANCE_INFO, req, opt)
    }

    pub fn get_instance_info_async(&self, req: &super::common::Request) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::InstanceInfoResp>> {
        self.get_instance_info_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn heartbeat_opt(&self, req: &super::hb::ServerHBReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::hb::ServerHBResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_HEARTBEAT, req, opt)
    }

    pub fn heartbeat(&self, req: &super::hb::ServerHBReq) -> ::grpcio::Result<super::hb::ServerHBResp> {
        self.heartbeat_opt(req, ::grpcio::CallOption::default())
    }

    pub fn heartbeat_async_opt(&self, req: &super::hb::ServerHBReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::ServerHBResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_HEARTBEAT, req, opt)
    }

    pub fn heartbeat_async(&self, req: &super::hb::ServerHBReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::ServerHBResp>> {
        self.heartbeat_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn update_runtime_env_opt(&self, req: &super::cluster::RuntimeEnv, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::cluster::RuntimeEnvList> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_UPDATE_RUNTIME_ENV, req, opt)
    }

    pub fn update_runtime_env(&self, req: &super::cluster::RuntimeEnv) -> ::grpcio::Result<super::cluster::RuntimeEnvList> {
        self.update_runtime_env_opt(req, ::grpcio::CallOption::default())
    }

    pub fn update_runtime_env_async_opt(&self, req: &super::cluster::RuntimeEnv, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::cluster::RuntimeEnvList>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_UPDATE_RUNTIME_ENV, req, opt)
    }

    pub fn update_runtime_env_async(&self, req: &super::cluster::RuntimeEnv) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::cluster::RuntimeEnvList>> {
        self.update_runtime_env_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn reset_runtime_env_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::Empty> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_RESET_RUNTIME_ENV, req, opt)
    }

    pub fn reset_runtime_env(&self, req: &super::common::Empty) -> ::grpcio::Result<super::common::Empty> {
        self.reset_runtime_env_opt(req, ::grpcio::CallOption::default())
    }

    pub fn reset_runtime_env_async_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Empty>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_RESET_RUNTIME_ENV, req, opt)
    }

    pub fn reset_runtime_env_async(&self, req: &super::common::Empty) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::Empty>> {
        self.reset_runtime_env_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn simple_heartbeat_opt(&self, req: &super::hb::SimpleServerHBReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::hb::SimpleServerHBResponse> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_SIMPLE_HEARTBEAT, req, opt)
    }

    pub fn simple_heartbeat(&self, req: &super::hb::SimpleServerHBReq) -> ::grpcio::Result<super::hb::SimpleServerHBResponse> {
        self.simple_heartbeat_opt(req, ::grpcio::CallOption::default())
    }

    pub fn simple_heartbeat_async_opt(&self, req: &super::hb::SimpleServerHBReq, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::SimpleServerHBResponse>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_SIMPLE_HEARTBEAT, req, opt)
    }

    pub fn simple_heartbeat_async(&self, req: &super::hb::SimpleServerHBReq) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::SimpleServerHBResponse>> {
        self.simple_heartbeat_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_runtime_group_status_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::data::RuntimeGroupStatusResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_RUNTIME_GROUP_STATUS, req, opt)
    }

    pub fn get_runtime_group_status(&self, req: &super::common::Empty) -> ::grpcio::Result<super::data::RuntimeGroupStatusResp> {
        self.get_runtime_group_status_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_runtime_group_status_async_opt(&self, req: &super::common::Empty, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::RuntimeGroupStatusResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_RUNTIME_GROUP_STATUS, req, opt)
    }

    pub fn get_runtime_group_status_async(&self, req: &super::common::Empty) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::RuntimeGroupStatusResp>> {
        self.get_runtime_group_status_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn is_data_path_in_use_opt(&self, req: &super::cluster::ServerIdAliveIdProto, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::data::DataPathStatusResponse> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_IS_DATA_PATH_IN_USE, req, opt)
    }

    pub fn is_data_path_in_use(&self, req: &super::cluster::ServerIdAliveIdProto) -> ::grpcio::Result<super::data::DataPathStatusResponse> {
        self.is_data_path_in_use_opt(req, ::grpcio::CallOption::default())
    }

    pub fn is_data_path_in_use_async_opt(&self, req: &super::cluster::ServerIdAliveIdProto, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::DataPathStatusResponse>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_IS_DATA_PATH_IN_USE, req, opt)
    }

    pub fn is_data_path_in_use_async(&self, req: &super::cluster::ServerIdAliveIdProto) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::DataPathStatusResponse>> {
        self.is_data_path_in_use_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_worker_info_and_routing_server_list_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::hb::RoutingServerInfoResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, req, opt)
    }

    pub fn get_worker_info_and_routing_server_list(&self, req: &super::common::Request) -> ::grpcio::Result<super::hb::RoutingServerInfoResp> {
        self.get_worker_info_and_routing_server_list_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_worker_info_and_routing_server_list_async_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::RoutingServerInfoResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, req, opt)
    }

    pub fn get_worker_info_and_routing_server_list_async(&self, req: &super::common::Request) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::hb::RoutingServerInfoResp>> {
        self.get_worker_info_and_routing_server_list_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_real_time_metric_opt(&self, req: &super::common::MetricInfoRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::MetricInfoResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_REAL_TIME_METRIC, req, opt)
    }

    pub fn get_real_time_metric(&self, req: &super::common::MetricInfoRequest) -> ::grpcio::Result<super::common::MetricInfoResp> {
        self.get_real_time_metric_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_real_time_metric_async_opt(&self, req: &super::common::MetricInfoRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::MetricInfoResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_REAL_TIME_METRIC, req, opt)
    }

    pub fn get_real_time_metric_async(&self, req: &super::common::MetricInfoRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::MetricInfoResp>> {
        self.get_real_time_metric_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_all_real_time_metrics_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::common::AllMetricsInfoResp> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_ALL_REAL_TIME_METRICS, req, opt)
    }

    pub fn get_all_real_time_metrics(&self, req: &super::common::Request) -> ::grpcio::Result<super::common::AllMetricsInfoResp> {
        self.get_all_real_time_metrics_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_all_real_time_metrics_async_opt(&self, req: &super::common::Request, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::AllMetricsInfoResp>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_ALL_REAL_TIME_METRICS, req, opt)
    }

    pub fn get_all_real_time_metrics_async(&self, req: &super::common::Request) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::common::AllMetricsInfoResp>> {
        self.get_all_real_time_metrics_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_executor_alive_id_opt(&self, req: &super::data::GetExecutorAliveIdRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::data::GetExecutorAliveIdResponse> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_EXECUTOR_ALIVE_ID, req, opt)
    }

    pub fn get_executor_alive_id(&self, req: &super::data::GetExecutorAliveIdRequest) -> ::grpcio::Result<super::data::GetExecutorAliveIdResponse> {
        self.get_executor_alive_id_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_executor_alive_id_async_opt(&self, req: &super::data::GetExecutorAliveIdRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::GetExecutorAliveIdResponse>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_EXECUTOR_ALIVE_ID, req, opt)
    }

    pub fn get_executor_alive_id_async(&self, req: &super::data::GetExecutorAliveIdRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::GetExecutorAliveIdResponse>> {
        self.get_executor_alive_id_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_partition_assignment_opt(&self, req: &super::data::GetPartitionAssignmentRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::data::GetPartitionAssignmentResponse> {
        self.client.unary_call(&METHOD_SERVER_DATA_API_GET_PARTITION_ASSIGNMENT, req, opt)
    }

    pub fn get_partition_assignment(&self, req: &super::data::GetPartitionAssignmentRequest) -> ::grpcio::Result<super::data::GetPartitionAssignmentResponse> {
        self.get_partition_assignment_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_partition_assignment_async_opt(&self, req: &super::data::GetPartitionAssignmentRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::GetPartitionAssignmentResponse>> {
        self.client.unary_call_async(&METHOD_SERVER_DATA_API_GET_PARTITION_ASSIGNMENT, req, opt)
    }

    pub fn get_partition_assignment_async(&self, req: &super::data::GetPartitionAssignmentRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::data::GetPartitionAssignmentResponse>> {
        self.get_partition_assignment_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait ServerDataApi {
    fn get_instance_info(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Request, sink: ::grpcio::UnarySink<super::data::InstanceInfoResp>);
    fn heartbeat(&mut self, ctx: ::grpcio::RpcContext, req: super::hb::ServerHBReq, sink: ::grpcio::UnarySink<super::hb::ServerHBResp>);
    fn update_runtime_env(&mut self, ctx: ::grpcio::RpcContext, req: super::cluster::RuntimeEnv, sink: ::grpcio::UnarySink<super::cluster::RuntimeEnvList>);
    fn reset_runtime_env(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Empty, sink: ::grpcio::UnarySink<super::common::Empty>);
    fn simple_heartbeat(&mut self, ctx: ::grpcio::RpcContext, req: super::hb::SimpleServerHBReq, sink: ::grpcio::UnarySink<super::hb::SimpleServerHBResponse>);
    fn get_runtime_group_status(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Empty, sink: ::grpcio::UnarySink<super::data::RuntimeGroupStatusResp>);
    fn is_data_path_in_use(&mut self, ctx: ::grpcio::RpcContext, req: super::cluster::ServerIdAliveIdProto, sink: ::grpcio::UnarySink<super::data::DataPathStatusResponse>);
    fn get_worker_info_and_routing_server_list(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Request, sink: ::grpcio::UnarySink<super::hb::RoutingServerInfoResp>);
    fn get_real_time_metric(&mut self, ctx: ::grpcio::RpcContext, req: super::common::MetricInfoRequest, sink: ::grpcio::UnarySink<super::common::MetricInfoResp>);
    fn get_all_real_time_metrics(&mut self, ctx: ::grpcio::RpcContext, req: super::common::Request, sink: ::grpcio::UnarySink<super::common::AllMetricsInfoResp>);
    fn get_executor_alive_id(&mut self, ctx: ::grpcio::RpcContext, req: super::data::GetExecutorAliveIdRequest, sink: ::grpcio::UnarySink<super::data::GetExecutorAliveIdResponse>);
    fn get_partition_assignment(&mut self, ctx: ::grpcio::RpcContext, req: super::data::GetPartitionAssignmentRequest, sink: ::grpcio::UnarySink<super::data::GetPartitionAssignmentResponse>);
}

pub fn create_server_data_api<S: ServerDataApi + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_INSTANCE_INFO, move |ctx, req, resp| {
        instance.get_instance_info(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_HEARTBEAT, move |ctx, req, resp| {
        instance.heartbeat(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_UPDATE_RUNTIME_ENV, move |ctx, req, resp| {
        instance.update_runtime_env(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_RESET_RUNTIME_ENV, move |ctx, req, resp| {
        instance.reset_runtime_env(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_SIMPLE_HEARTBEAT, move |ctx, req, resp| {
        instance.simple_heartbeat(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_RUNTIME_GROUP_STATUS, move |ctx, req, resp| {
        instance.get_runtime_group_status(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_IS_DATA_PATH_IN_USE, move |ctx, req, resp| {
        instance.is_data_path_in_use(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_WORKER_INFO_AND_ROUTING_SERVER_LIST, move |ctx, req, resp| {
        instance.get_worker_info_and_routing_server_list(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_REAL_TIME_METRIC, move |ctx, req, resp| {
        instance.get_real_time_metric(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_ALL_REAL_TIME_METRICS, move |ctx, req, resp| {
        instance.get_all_real_time_metrics(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_EXECUTOR_ALIVE_ID, move |ctx, req, resp| {
        instance.get_executor_alive_id(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_SERVER_DATA_API_GET_PARTITION_ASSIGNMENT, move |ctx, req, resp| {
        instance.get_partition_assignment(ctx, req, resp)
    });
    builder.build()
}
