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

use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::env;
use std::{ thread};
use core::time;

use grpcio::{RpcContext, UnarySink, ServerStreamingSink};
use protobuf::Message;

use maxgraph_common::proto::gremlin_service_grpc::*;
use maxgraph_common::proto::query_flow::*;
use maxgraph_common::proto::message::OperationResponse;
use maxgraph_common::proto::message::QueryResponse;
use maxgraph_common::proto::message::RemoveDataflowRequest;
use maxgraph_common::util::time::*;
use maxgraph_common::util::log::log_query;
use maxgraph_common::util::log::QueryEvent;
use maxgraph_common::util::log::QueryType;
use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::prelude::*;

use dataflow::io::remove_odps_id_cache;

use server::query_manager::{QueryManager, DataflowInfo};
use server::future_set::ClientFuture;

use pegasus::Pegasus;
use pegasus::allocate::ParallelConfig;
use pegasus::worker::Worker;
use pegasus::operator::source::IntoStream;
use pegasus::operator::advanced::map;

use rpc::FromErrorMessage;
use rpc::{error_response, input_batch_level_to_num, to_response_new};
use rpc::SinkError;
use rpc::SinkAll;

use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::{Future, Sink, Stream};

use futures_cpupool::{Builder, CpuPool};
use tokio_sync::semaphore::Semaphore;
use pegasus::operator::advanced::inspect::Inspect;
use pegasus::operator::advanced::sink::Output;
use execution::{build_route_fn, build_worker_partition_ids, build_partition_router, build_process_router, build_empty_router};
use dataflow::manager::context::{RuntimeContext, BuilderContext};
use dataflow::plan::tiny_builder::TinyDataflowBuilder;
use dataflow::plan::query_plan::QueryFlowPlan;
use dataflow::message::RawMessage;
use maxgraph_common::proto::message::ErrorCode;
use pegasus::operator::advanced::map::Map;
use pegasus::worker::DefaultStrategy;
use server::DataflowId;
use rpc::rpc_pegasus::generate_task_id;
use std::fs::File;
use std::io::{BufReader, BufRead};
use utils::get_lambda_service_client;
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
use dataflow::manager::lambda::LambdaManager;
use store::store_delegate::StoreDelegate;
use store::remote_store_service::RemoteStoreServiceManager;
use maxgraph_store::api::graph_partition::{FixedStorePartitionManager, GraphPartitionManager};
use store::global_schema::LocalGraphSchema;
use store::ffi::{GlobalVertex, FFIEdge, GlobalVertexIter, GlobalEdgeIter};
use maxgraph_store::api::graph_schema::Schema;
use store::task_partition_manager::TaskPartitionManager;
use std::collections::HashMap;


pub struct AsyncMaxGraphServiceImpl<V, VI, E, EI>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    store_config: Arc<StoreConfig>,
    pegasus_server: Arc<Option<Pegasus>>,
    query_manager: QueryManager,
    cpu_pool: CpuPool,
    client_semaphore: Arc<Semaphore>,
    remote_store_service_manager: Arc<RwLock<Option<RemoteStoreServiceManager>>>,
    remote_store_service_manager_core: Arc<RemoteStoreServiceManager>,
    lambda_service_client: Option<Arc<LambdaServiceClient>>,
    signal: Arc<AtomicBool>,
    graph: Arc<GlobalGraphQuery<V=V, E=E, VI=VI, EI=EI>>,
    partition_manager: Arc<GraphPartitionManager>,
    task_partition_manager: Arc<RwLock<Option<TaskPartitionManager>>>,
}

impl<V, VI, E, EI> AsyncMaxGraphServiceImpl<V, VI, E, EI>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    pub fn new_service(
        store_config: Arc<StoreConfig>,
        pegasus_server: Arc<Option<Pegasus>>,
        query_manager: QueryManager,
        remote_store_service_manager: Arc<RwLock<Option<RemoteStoreServiceManager>>>,
        lambda_service_client: Option<Arc<LambdaServiceClient>>,
        signal: Arc<AtomicBool>,
        graph: Arc<GlobalGraphQuery<V=V, E=E, VI=VI, EI=EI>>,
        partition_manager: Arc<GraphPartitionManager>,
        task_partition_manager: Arc<RwLock<Option<TaskPartitionManager>>>,
    ) -> ::grpcio::Service {
        let cpu_pool = Builder::new()
            .name_prefix("gremlin-grpc-pool-".to_owned())
            .pool_size(store_config.rpc_thread_count as usize)
            .create();
        let client_semaphore = Semaphore::new(store_config.rpc_thread_count as usize);
        let service = AsyncMaxGraphServiceImpl {
            store_config,
            pegasus_server,
            query_manager,
            cpu_pool,
            client_semaphore: Arc::new(client_semaphore),
            remote_store_service_manager,
            remote_store_service_manager_core: Arc::new(RemoteStoreServiceManager::empty()),
            lambda_service_client,
            signal,
            graph,
            partition_manager,
            task_partition_manager,
        };
        create_async_max_graph_service(service)
    }

    fn create_workers(store_config: Arc<StoreConfig>,
                      remote_store_service_manager: Arc<RemoteStoreServiceManager>,
                      lambda_service_client: Option<Arc<LambdaServiceClient>>,
                      pegasus: Pegasus,
                      req: QueryFlow,
                      sender: UnboundedSender<Vec<Vec<u8>>>,
                      timeout_ms: Arc<AtomicUsize>,
                      graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                      partition_manager: Arc<GraphPartitionManager>,
                      task_partition_manager: Arc<RwLock<Option<TaskPartitionManager>>>) -> Result<Vec<Worker>, String> {
        let task_id = generate_task_id(req.query_id.clone());

        let thread_count = store_config.timely_worker_per_process as usize;
        let process_count = store_config.worker_num as usize;
        let peers = (thread_count * process_count) as u64;

        let si = req.get_snapshot();
        let debug_log_flag = req.get_debug_log_flag();
        if debug_log_flag {
            info!("query flow: {:?}", &req);
        }
        let exec_local_flag = req.get_exec_local_flag();
        let schema = graph.as_ref().get_schema(si).unwrap();

        let mut workers = pegasus.create_workers(task_id, thread_count, process_count).unwrap();

        let initialized_task_partition_manager;
        loop {
            match task_partition_manager.try_read() {
                Ok(n) => { if n.is_some() {
                    initialized_task_partition_manager = n.clone().unwrap();
                    break;
                } else {
                    continue;
                }
            },
                Err(_) => continue,
            }
        }
        let task_partition_manager = initialized_task_partition_manager;

        for worker in workers.iter_mut() {
            worker.set_schedule_strategy(DefaultStrategy::new(10240, 9216, Some(1024)));
            worker.set_timeout(timeout_ms.clone());
            let index = worker.id.1;

            let mut req_clone = req.clone();
            let query_id = req_clone.get_query_id().to_owned();
            let script = req_clone.take_script();
            let bytecode = req_clone.take_bytecode();
            let sender_clone = sender.clone();
            let partition_task_list = task_partition_manager.get_partition_task_list();
            let route = build_partition_router(partition_manager.clone(),
                                               partition_task_list.clone(),
                                               debug_log_flag);

            let lambda_manager = LambdaManager::new(&query_id, lambda_service_client.clone());
            if req_clone.get_lambda_existed() {
                if script.len() != 0 {
                    lambda_manager.send_lambda_base_with_script(&script, req_clone.timeout_ms * 2, schema.clone());
                } else {
                    lambda_manager.send_lambda_base_with_bytecode(bytecode, req_clone.timeout_ms * 2, schema.clone());
                }
            }

            let worker_partition_ids = task_partition_manager.get_task_partition_list(&(index as u32));
            info!("worker {:?} manager partition ids {:?}", index, &worker_partition_ids);
            let context = RuntimeContext::new(req_clone.get_query_id().to_owned(),
                                              schema.clone(),
                                              Arc::new(route),
                                              si,
                                              debug_log_flag,
                                              index as u64,
                                              peers,
                                              exec_local_flag,
                                              Arc::new(lambda_manager),
                                              worker_partition_ids,
                                              remote_store_service_manager.clone(),
                                              partition_manager.clone(),
                                              graph.clone());

            let process_router = Arc::new(build_process_router(partition_manager.clone(),
                                                               partition_task_list.clone(),
                                                               thread_count as i32));
            let result = worker.dataflow(req.query_id.clone().as_str(), move |builder| {
                let mut flow_builder = TinyDataflowBuilder::new(builder);

                let query_plan = req_clone.take_query_plan();
                let query_id = req_clone.get_query_id();
                let script = req_clone.get_script();
                let last_op_id = query_plan.get_operator_id_list().to_vec()[query_plan.get_operator_id_list().len() - 1];
                let flow_plan = QueryFlowPlan::new(query_plan);
                flow_plan.build(&mut flow_builder, query_id, &script, &context)?;

                let current_process_router = process_router.clone();
                let stream = if let Ok(result_stream) = flow_builder.get_stream(last_op_id, 0) {
                    result_stream.map(move |v| {
                        let result_process_router = current_process_router.clone();
                        v.to_proto(Some(result_process_router.as_ref())).write_to_bytes().unwrap()
                    })
                } else {
                    let err_msg = format!("cant found result operator for id {:?}", last_op_id);
                    vec![RawMessage::from_error(ErrorCode::DATAFLOW_ERROR, err_msg)].into_stream(builder).map(|x: RawMessage| {
                        let empty_fn = build_empty_router();
                        x.to_proto(Some(&empty_fn)).write_to_bytes().unwrap()
                    })
                };

                stream.sink(move |id, data| { sender_clone.unbounded_send(data).expect("send data failed."); });
                Ok(())
            });

            if let Err(err) = result {
                return Err(format!("{:?}.", err));
            }
        }

        return Ok(workers);
    }
}

impl<V, VI, E, EI> Clone for AsyncMaxGraphServiceImpl<V, VI, E, EI>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    fn clone(&self) -> Self {
        AsyncMaxGraphServiceImpl {
            store_config: self.store_config.clone(),
            pegasus_server: self.pegasus_server.clone(),
            query_manager: self.query_manager.clone(),
            cpu_pool: self.cpu_pool.clone(),
            client_semaphore: self.client_semaphore.clone(),
            remote_store_service_manager: self.remote_store_service_manager.clone(),
            remote_store_service_manager_core: self.remote_store_service_manager_core.clone(),
            lambda_service_client: self.lambda_service_client.clone(),
            signal: self.signal.clone(),
            graph: self.graph.clone(),
            partition_manager: self.partition_manager.clone(),
            task_partition_manager: self.task_partition_manager.clone(),
        }
    }
}

impl<V, VI, E, EI> AsyncMaxGraphService for AsyncMaxGraphServiceImpl<V, VI, E, EI>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    fn async_query(&mut self, ctx: RpcContext, _req: QueryFlow, sink: ServerStreamingSink<QueryResponse>) {
        let resp = error_response("unimplemented".to_owned());
        sink.sink_all(&ctx, resp);
    }

    fn async_prepare(&mut self, ctx: RpcContext, req: QueryFlow, sink: UnarySink<OperationResponse>) {
        sink.sink_error(&ctx, "unimplemented".to_owned());
    }

    fn async_query2(&mut self, ctx: RpcContext, req: Query, sink: ServerStreamingSink<QueryResponse>) {
        let resp = error_response("unimplemented".to_owned());
        sink.sink_all(&ctx, resp);
    }

    fn async_execute(&mut self, ctx: RpcContext, req: QueryFlow, sink: ServerStreamingSink<QueryResponse>) {
        let start = Instant::now();
        let query_id = req.query_id.clone();
        let query_id_clone = req.query_id.clone();

        log_query(&self.store_config.graph_name, self.store_config.worker_id,
                  &query_id, QueryType::Execute, QueryEvent::ExecutorReceived);
        let query_guard = self.query_manager.new_query(query_id.clone(), req.get_front_id(), req.get_script().to_string());
        let query_guard = Arc::new(query_guard);
        let query_guard_clone = query_guard.clone();


        let deadline = req.start_timestamp_ms + req.timeout_ms;
        let now = current_time_millis();
        if now >= deadline {
            log_query(&self.store_config.graph_name, self.store_config.worker_id,
                      &query_id, QueryType::Execute,
                      QueryEvent::ExecutorFinish {
                          latency_nano: 0,
                          result_num: 0,
                          success: false,
                      });
            error!("query: {} request timeout upon arrival, start: {}, timeout: {}, now: {}", query_id, req.start_timestamp_ms, req.timeout_ms, now);
            sink.sink_all(&ctx, error_response("timeout upon arrival".to_owned()));
            return;
        }

        // check and update store_service_manager
        let signal = match self.signal.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(x) => x,
            Err(x) => x,
        };
        if signal {
            let mut remote_store_service_manager_clone = (*self.remote_store_service_manager.read().unwrap()).clone();
            self.remote_store_service_manager_core = Arc::new(remote_store_service_manager_clone.take().unwrap());
        }

        // check pegasus engine
        if self.pegasus_server.is_none() {
            sink.sink_all(&ctx, error_response("Executor engine is not running, please retry.".to_owned()));
            return;
        }

        let timeout_ms = Arc::new(AtomicUsize::new((deadline - now) as usize));

        let pegasus_clone = self.pegasus_server.as_ref().as_ref().expect("pegasus is not running.").clone();
        let store_config_clone = self.store_config.clone();
        let remote_store_service_manager = self.remote_store_service_manager_core.clone();
        let lambda_service_client = self.lambda_service_client.clone();

        let graph_name = self.store_config.graph_name.clone();
        let worker_id = self.store_config.worker_id;

        let is_execute_success = Arc::new(AtomicBool::new(true));
        let is_execute_success_clone_1 = is_execute_success.clone();
        let is_execute_success_clone_2 = is_execute_success.clone();

        let (sender, receiver) = unbounded();
        let worker_result = AsyncMaxGraphServiceImpl::create_workers(store_config_clone,
                                                                     remote_store_service_manager,
                                                                     lambda_service_client,
                                                                     pegasus_clone.clone(),
                                                                     req,
                                                                     sender,
                                                                     timeout_ms.clone(),
                                                                     self.graph.clone(),
                                                                     self.partition_manager.clone(),
                                                                     self.task_partition_manager.clone());
        if let Err(err_msg) = worker_result {
            sink.sink_all(&ctx, error_response(err_msg));
            return;
        }

        let spawn_result = pegasus_clone.run_workers(worker_result.unwrap());
        if let Err(err) = spawn_result {
            let err_msg = format!("Pegasus spawn workers failed, caused by {:?}", err);
            sink.sink_all(&ctx, error_response(err_msg));
            return;
        }

        let future = futures::done(Ok(())).and_then(move |_| {
            query_guard_clone.set_dataflow_id(query_id_clone.as_str(), DataflowInfo::new_timeout(timeout_ms));
            let stream = receiver.then(move |data| {
                match data {
                    Ok(data) => {
                        Ok(to_response_new(data))
                    }
                    Err(err) => {
                        let err_msg = format!("Run receiver future error: {:?}.", err);
                        error!("{}", err_msg);
                        is_execute_success_clone_1.store(false, Ordering::Relaxed);
                        Ok((QueryResponse::from_str(err_msg), Default::default()))
                    }
                }
            }).map_err(|err_msg: grpcio::Error| { err_msg });

            sink.send_all(stream)
                .then(move |result| {
                    if let Err(error) = result {
                        let err_msg = format!("Sink send query result error: {}", error);
                        error!("{}", err_msg);
                        Err(())
                    } else {
                        Ok(())
                    }
                })
        }).then(move |result| {
            ::std::mem::drop(query_guard);
            let execute_result = result.is_ok() && is_execute_success.load(Ordering::Relaxed);
            let total_cost = start.elapsed();
            log_query(&graph_name, worker_id,
                      &query_id, QueryType::Execute,
                      QueryEvent::ExecutorFinish {
                          latency_nano: duration_to_nanos(&total_cost),
                          result_num: 1,
                          success: execute_result,
                      });
            Ok(())
        });

        let f = self.cpu_pool.spawn(future);
        ctx.spawn(f);
    }

    fn async_remove(&mut self, ctx: RpcContext, _req: RemoveDataflowRequest, sink: UnarySink<OperationResponse>) {
        sink.sink_error(&ctx, "unimplemented".to_owned());
    }
}



