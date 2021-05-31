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

use maxgraph_store::api::prelude::*;
use dataflow::message::subgraph::SubGraph;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;
use store::store_delegate::StoreDelegate;
use store::remote_store_service::RemoteStoreServiceManager;
use store::global_store::GlobalStore;
use store::task_partition_manager::TaskPartitionManager;
use dataflow::store::cache::CacheStore;
use dataflow::manager::lambda::LambdaManager;
use itertools::Itertools;
use maxgraph_store::api::GlobalGraphQuery;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
use store::ffi::{GlobalVertex, GlobalVertexIter, FFIEdge, GlobalEdgeIter};
use maxgraph_store::api::graph_schema::Schema;
use maxgraph_common::util::partition::{assign_single_partition, assign_empty_partition, assign_all_partition, assign_vertex_label_partition};

pub struct EarlyStopState {
    global_stop_flag: AtomicBool,
}

impl EarlyStopState {
    pub fn new() -> Self {
        EarlyStopState {
            global_stop_flag: AtomicBool::new(false),
        }
    }

    pub fn enable_global_stop(&self) {
        self.global_stop_flag.store(true, Ordering::Relaxed);
    }

    pub fn check_global_stop(&self) -> bool {
        self.global_stop_flag.load(Ordering::Relaxed)
    }
}

pub struct RuntimeContext<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + Send + Sync + 'static {
    query_id: String,
    partition_ids: Arc<Vec<PartitionId>>,
    schema: Arc<dyn Schema>,
    route: Arc<F>,
    snapshot_id: SnapshotId,
    early_stop_state: Arc<EarlyStopState>,
    debug_flag: bool,
    index: u64,
    peers: u64,
    subgraph: Arc<SubGraph>,
    cache_store: Arc<CacheStore>,
    remote_store_service: Arc<RemoteStoreServiceManager>,
    exec_local_flag: bool,
    lambda_manager: Arc<LambdaManager>,
    partition_manager: Arc<GraphPartitionManager>,
    // vineyard graph id
    graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI, F> RuntimeContext<V, VI, E, EI, F>
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static,
          F: Fn(&i64) -> u64 + Send + Sync + 'static {
    pub fn new(query_id: String,
               schema: Arc<dyn Schema>,
               route: Arc<F>,
               snapshot_id: SnapshotId,
               debug_flag: bool,
               index: u64,
               peers: u64,
               exec_local_flag: bool,
               lambda_manager: Arc<LambdaManager>,
               partition_ids: Vec<PartitionId>,
               remote_store_service: Arc<RemoteStoreServiceManager>,
               partition_manager: Arc<GraphPartitionManager>,
               // vineyard graph
               graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
    ) -> Self {
        RuntimeContext {
            query_id,
            partition_ids: Arc::new(partition_ids.to_vec()),
            schema,
            route,
            snapshot_id,
            early_stop_state: Arc::new(EarlyStopState::new()),
            debug_flag,
            index,
            peers,
            subgraph: Arc::new(SubGraph::new()),
            cache_store: Arc::new(CacheStore::new(partition_ids.as_ref())),
            remote_store_service,
            exec_local_flag,
            lambda_manager,
            partition_manager,
            graph,
        }
    }

    pub fn get_query_id(&self) -> &String {
        &self.query_id
    }

    pub fn get_partition_ids(&self) -> &Arc<Vec<PartitionId>> {
        &self.partition_ids
    }

    pub fn get_schema(&self) -> &Arc<dyn Schema> {
        &self.schema
    }

    pub fn get_route(&self) -> &Arc<F> {
        &self.route
    }

    pub fn get_snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub fn get_early_stop_state(&self) -> &Arc<EarlyStopState> {
        &self.early_stop_state
    }

    pub fn get_debug_flag(&self) -> bool {
        self.debug_flag
    }

    pub fn get_index(&self) -> u64 {
        self.index
    }

    pub fn get_peers(&self) -> u64 {
        self.peers
    }

    pub fn get_subgraph(&self) -> &Arc<SubGraph> {
        &self.subgraph
    }

    pub fn get_cache_store(&self) -> &Arc<CacheStore> {
        &self.cache_store
    }

    pub fn get_exec_local_flag(&self) -> bool {
        self.exec_local_flag
    }

    pub fn get_lambda_manager(&self) -> Arc<LambdaManager> {
        self.lambda_manager.clone()
    }

    pub fn get_store(&self) -> &Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>> {
        &self.graph
    }

    pub fn get_graph_partition_manager(&self) -> &Arc<GraphPartitionManager> {
        &self.partition_manager
    }
}

pub struct TaskContext {
    index: u32,
    si: SnapshotId,
    partition_manager: Arc<GraphPartitionManager>,
    task_partition_list: Vec<PartitionId>,
    // access remote graph
    remote_graph_flag: bool,
    // print debug log
    debug_flag: bool,
}

impl TaskContext {
    pub fn new(worker_index: u32,
               si: SnapshotId,
               partition_manager: Arc<GraphPartitionManager>,
               task_partition_list: Vec<PartitionId>,
               remote_graph_flag: bool,
               debug_flag: bool) -> Self {
        TaskContext {
            index: worker_index,
            si,
            partition_manager,
            task_partition_list,
            remote_graph_flag,
            debug_flag,
        }
    }

    pub fn get_si(&self) -> SnapshotId {
        self.si
    }

    pub fn get_debug_flag(&self) -> bool {
        self.debug_flag
    }

    pub fn get_remote_graph_flag(&self) -> bool {
        self.remote_graph_flag
    }

    pub fn get_partition_id(&self, vid: VertexId) -> Option<PartitionId> {
        let parid = self.partition_manager.as_ref().get_partition_id(vid);
        if self.debug_flag {
            info!("Get partition id {:?} for vertex id {:?}", parid, vid);
        }
        if parid < 0 {
            None
        } else {
            Some(parid as u32)
        }
    }

    pub fn get_partition_list(&self) -> &Vec<PartitionId> {
        &self.task_partition_list
    }

    pub fn assign_out_vertex_partition(&self, vid: VertexId, partition_vertex_list: &mut Vec<(PartitionId, Vec<VertexId>)>) {
        if let Some(partition_id) = self.get_partition_id(vid) {
            let flag = !(self.remote_graph_flag || self.task_partition_list.contains(&partition_id));
            if flag {
                if self.debug_flag {
                    info!("Assign out vertex failed for remote graph flag {:?} task partition list {:?} partition id {:?} for vid {:?}",
                          self.remote_graph_flag,
                          &self.task_partition_list,
                          partition_id,
                          vid);
                }
                return;
            }

            assign_single_partition(vid, partition_id, partition_vertex_list);
        }
    }

    pub fn assign_empty_vertex_partition(&self, partition_vertex_list: &mut Vec<(PartitionId, Vec<VertexId>)>) {
        assign_empty_partition(&self.task_partition_list, partition_vertex_list);
    }

    pub fn assign_in_vertex_partition(&self, vid: VertexId, partition_vertex_list: &mut Vec<(PartitionId, Vec<VertexId>)>) {
        assign_all_partition(vid, partition_vertex_list);
    }

    pub fn assign_prop_vertex_partition(&self,
                                        label_id: Option<LabelId>,
                                        vid: VertexId,
                                        partition_vertex_list: &mut Vec<(PartitionId, Vec<(Option<LabelId>, Vec<VertexId>)>)>) {
        if let Some(partition_id) = self.get_partition_id(vid) {
            let flag = !(self.remote_graph_flag || self.task_partition_list.contains(&partition_id));
            if flag {
                info!("Assign vertex property failed for remote graph flag {:?} task partition list {:?} partition id {:?} for vid {:?}",
                      self.remote_graph_flag,
                      &self.task_partition_list,
                      partition_id,
                      vid);
                return;
            }

            assign_vertex_label_partition(label_id, vid, partition_id, partition_vertex_list);
        }
    }

    pub fn get_worker_index(&self) -> u32 {
        self.index
    }
}

pub struct BuilderContext {
    remote_store_service_manager: Arc<RemoteStoreServiceManager>,
    task_partition_manager: Arc<TaskPartitionManager>,
    lambda_service_client: Option<Arc<LambdaServiceClient>>,
    partition_manager: Arc<GraphPartitionManager>,
}

impl BuilderContext {
    pub fn new(
        remote_store_service_manager: Arc<RemoteStoreServiceManager>,
        task_partition_manager: Arc<TaskPartitionManager>,
        partition_manager: Arc<GraphPartitionManager>,
        lambda_service_client: Option<Arc<LambdaServiceClient>>) -> Self {
        BuilderContext {
            remote_store_service_manager,
            task_partition_manager,
            partition_manager,
            lambda_service_client,
        }
    }

    pub fn get_remote_store_service_manager(&self) -> &Arc<RemoteStoreServiceManager> {
        &self.remote_store_service_manager
    }

    pub fn get_task_partition_manager(&self) -> &Arc<TaskPartitionManager> {
        &self.task_partition_manager
    }

    pub fn get_partition_manager(&self) -> &Arc<GraphPartitionManager> {
        &self.partition_manager
    }

    pub fn get_lambda_service_client(&self) -> Option<&Arc<LambdaServiceClient>> {
        self.lambda_service_client.as_ref()
    }
}
