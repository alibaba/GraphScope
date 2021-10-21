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

//! query implementation
extern crate maxgraph_common;

use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::prelude::{MVGraph, Vertex, Edge};
use maxgraph_common::proto::query_flow::QueryFlow;
use maxgraph_common::proto::message::ErrorCode;
use maxgraph_common::proto::lambda_service::{LambdaData, LambdaBase};


use dataflow::plan::query_plan::QueryFlowPlan;
use dataflow::message::RawMessage;

use std::path::{PathBuf, Path};
use std::sync::{Arc, RwLock};
use server::ServerTimestamp;
use protobuf::parse_from_bytes;
use protobuf::{Message, CodedInputStream};
use dataflow::manager::context::{RuntimeContext, BuilderContext};
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;

use std::env;

//mod chain;

#[inline]
pub fn build_route_fn(store_config: &StoreConfig) -> impl Fn(&i64) -> u64 + 'static {
    let worker_per_process = store_config.timely_worker_per_process as u64;
    let process_num = store_config.worker_num as u64;
    let partition_num = store_config.partition_num as u64;
    let partition_per_process = partition_num / process_num;

    move |vid| {
        let mut m = vid % partition_num as i64;
        if m < 0 { m += partition_num as i64; }
        let process_index = m as u64 / partition_per_process;
        let worker_index = m as u64 % worker_per_process;
        process_index * worker_per_process + worker_index
    }
}

#[inline]
pub fn build_process_router(partition_manager: Arc<GraphPartitionManager>,
                            partition_task_list: HashMap<u32, u32>,
                            worker_num_per_process: i32) -> impl Fn(&i64) -> i32 + 'static {
    move |vid| {
        let partition_id = partition_manager.as_ref().get_partition_id(*vid);
        if partition_id < 0 {
            panic!("Cant found partition id for vertex {:?}", vid);
        }
        if let Some(task_index) = partition_task_list.get(&(partition_id as u32)) {
            let store_id = *task_index as i32 / worker_num_per_process;
//            info!("get store id {:?} for task index {:?} worker num per process {:?} partition id {:?} partition_task_list {:?}",
//                  store_id,
//                  task_index,
//                  worker_num_per_process,
//                  partition_id,
//                  &partition_task_list);
            store_id
        } else {
            panic!("Cant found task index for partition {:?} in {:?}", partition_id, &partition_task_list);
        }
    }
}

#[inline]
pub fn build_empty_router() -> impl Fn(&i64) -> i32 + 'static {
    move |_| {
        // info!("generate store id with 0");
        0
    }
}

#[inline]
pub fn build_partition_router(partition_manager: Arc<GraphPartitionManager>,
                              partition_task_list: HashMap<u32, u32>,
                              debug_log: bool) -> impl Fn(&i64) -> u64 + 'static {
    move |vid| {
        let partition_id = partition_manager.as_ref().get_partition_id(*vid);
        if partition_id < 0 {
            if debug_log {
                info!("Router get partition id {:?} for id {:?}", &partition_id, vid);
            }
            *vid as u64
        } else {
            if let Some(task_index) = partition_task_list.get(&(partition_id as u32)) {
                if debug_log {
                    info!("Router result task index {:?} partition id {:?} for vid {:?} partition task list {:?}",
                          task_index,
                          partition_id,
                          vid,
                          &partition_task_list);
                }
                *task_index as u64
            } else {
                if debug_log {
                    info!("Router result task index is 0 partition id {:?} for vid {:?} partition task list {:?}",
                          partition_id,
                          vid,
                          &partition_task_list);
                }
                0
            }
        }
    }
}

#[inline]
pub fn build_worker_partition_ids<F>(partition_id_list: &Vec<PartitionId>,
                                     route: &F,
                                     index: u64) -> Vec<PartitionId>
    where F: Fn(&i64) -> u64 {
    return partition_id_list.iter()
        .filter(|p| route(&(**p as i64)) == index)
        .map(|p| *p).collect();
}


use maxgraph_common::proto::query_flow::QueryInput;
use store::store_service::StoreServiceManager;
use std::rc::Rc;
use store::store_delegate::StoreDelegate;
use dataflow::manager::lambda;
use std::fs::File;
use std::collections::HashMap;
use std::io::{BufReader, BufRead};
use maxgraph_common::proto::schema::SchemaProto;
use dataflow::manager::lambda::LambdaManager;
use maxgraph_store::api::{PartitionId, GlobalGraphQuery};
use store::remote_store_service::RemoteStoreServiceManager;
use maxgraph_store::api::graph_partition::{ConstantPartitionManager, FixedStorePartitionManager, GraphPartitionManager};
use store::task_partition_manager::TaskPartitionManager;
use store::ffi::{GlobalVertex, GlobalVertexIter, FFIEdge, GlobalEdgeIter};
use store::global_schema::LocalGraphSchema;
use maxgraph_store::api::graph_schema::Schema;


#[test]
fn test_route() {
    let store_config = StoreConfig {
        worker_id: 1,
        alive_id: 0,
        worker_num: 2,
        zk_url: "".to_string(),
        graph_name: "".to_string(),
        partition_num: 4,
        zk_timeout_ms: 0,
        zk_auth_enable: false,
        zk_auth_user: "test".to_string(),
        zk_auth_password: "test".to_string(),
        hb_interval_ms: 0,
        insert_thread_count: 0,
        download_thread_count: 0,
        hadoop_home: "".to_string(),
        local_data_root: "".to_string(),
        load_thread_count: 0,
        rpc_thread_count: 0,
        rpc_port: 0,
        timely_worker_per_process: 3,
        monitor_interval_ms: 0,
        total_memory_mb: 0,
        hdfs_default_fs: "".to_string(),
        timely_prepare_dir: "".to_string(),
        replica_count: 0,
        realtime_write_buffer_size: 0,
        realtime_write_ingest_count: 0,
        realtime_write_buffer_mb: 0,
        realtime_write_queue_count: 0,
        realtime_precommit_buffer_size: 0,
        instance_id: "INSTANCE_ID".to_owned(),
        engine_name: "timely".to_owned(),
        pegasus_thread_pool_size: 4_u32,
        graph_type: "".to_string(),
        vineyard_graph_id: 0,
        lambda_enabled: false,
    };
    let route = build_route_fn(&store_config);
    let id0 = route(&0);
    let id1 = route(&1);
    let id2 = route(&2);
    let id3 = route(&3);
    let id4 = route(&4);
    let id5 = route(&5);
    println!("{}, {}, {}, {}, {}, {}", id0, id1, id2, id3, id4, id5);
    assert_eq!(0, id0);
    assert_eq!(1, id1);
    assert_eq!(5, id2);
    assert_eq!(3, id3);
    assert_eq!(0, id4);
    assert_eq!(1, id5);
}
