//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//! http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use structopt::StructOpt;
use std::env;
use serde_json::Value;
use serde_json;
use std::collections::HashMap;

pub const VINEYARD_GRAPH: &str = "vineyard";

#[derive(Debug, StructOpt, Default)]
pub struct StoreConfig {
    /// worker id
    #[structopt(short = "w", long = "worker-id")]
    pub worker_id: u32,

    /// alive id
    #[structopt(long = "alive-id", default_value = "1313145512")]
    pub alive_id: u64,

    #[structopt(short = "n", long = "worker-num", default_value = "4")]
    pub worker_num: u32,

    /// zookeeper uRL, e.g.
    /// 127.0.0.1:2181/test or 127.0.0.1:2181/test,xx.xx.xx.xx:2181/xx/xx
    #[structopt(short = "z", long = "zk", default_value = "")]
    pub zk_url: String,

    /// graph name
    #[structopt(short = "g", long = "graph-name")]
    pub graph_name: String,

    /// partition num
    #[structopt(short = "p", long = "partition-num")]
    pub partition_num: u32,

    /// timeout of zk connection
    #[structopt(long = "zk-timeout-ms", default_value = "6000")]
    pub zk_timeout_ms: u64,

    /// zookeeper auth enable
    #[structopt(long = "zk-auth-enable")]
    pub zk_auth_enable: bool,

    /// zookeeper auth user
    #[structopt(long = "zk-auth-user", default_value = "test")]
    pub zk_auth_user: String,

    /// zookeeper auth password
    #[structopt(long = "zk-auth-password", default_value = "test")]
    pub zk_auth_password: String,

    /// the interval of heartbeat
    #[structopt(long = "hb-interval-ms", default_value = "5000")]
    pub hb_interval_ms: u64,


    /// thread count of realtime insert
    #[structopt(long = "insert-thread-count", default_value = "1")]
    pub insert_thread_count: u32,

    /// thread count of download graph data from HDFS or some other data source
    #[structopt(long = "download-thread-count", default_value = "4")]
    pub download_thread_count: u32,

    /// hadoop home
    #[structopt(long = "hadoop-home", default_value = "./")]
    pub hadoop_home: String,

    /// all local data will store at $local.data.root/$graph.name/label/partition_id/
    #[structopt(long = "local-data-root", default_value = "./")]
    pub local_data_root: String,

    /// thread count of bulk load
    #[structopt(long = "load-thread-count", default_value = "4")]
    pub load_thread_count: u32,

    #[structopt(long = "grpc-thread-count", default_value = "16")]
    pub rpc_thread_count: u32,

    #[structopt(long = "port", default_value = "0")]
    pub rpc_port: u32,

    #[structopt(long = "graph-port", default_value = "0")]
    pub graph_port: u32,

    #[structopt(long = "query-port", default_value = "0")]
    pub query_port: u32,

    #[structopt(long = "engine-port", default_value = "0")]
    pub engine_port: u32,

    #[structopt(long = "gaia-engine-port", default_value = "0")]
    pub gaia_engine_port: u32,

    #[structopt(long = "timely-worker-per-process", default_value = "2")]
    pub timely_worker_per_process: u32,

    #[structopt(long = "monitor-interval-ms", default_value = "2000")]
    pub monitor_interval_ms: u64,

    #[structopt(long = "total-memory-mb", default_value = "4096")]
    pub total_memory_mb: u64,

    #[structopt(long = "hdfs-default-fs", default_value = "hdfs://")]
    pub hdfs_default_fs: String,

    #[structopt(long = "timely-prepare-dir", default_value="prepare_query_info")]
    pub timely_prepare_dir : String,

    #[structopt(long = "replica-count", default_value="1")]
    pub replica_count : u32,

    #[structopt(long = "realtime-write-buffer-size", default_value="1024")]
    pub realtime_write_buffer_size : u32,

    #[structopt(long = "realtime-write-ingest-count", default_value="1")]
    pub realtime_write_ingest_count : u32,

    #[structopt(long = "realtime-write-buffer-mb", default_value="16")]
    pub realtime_write_buffer_mb : u64,

    #[structopt(long = "realtime-write-queue-count", default_value="128")]
    pub realtime_write_queue_count : u32,

    #[structopt(long = "realtime-precommit-buffer-size", default_value="8388608")]
    pub realtime_precommit_buffer_size : u32,

    #[structopt(long = "instance.unique.id", default_value="INSTANCE_ID")]
    pub instance_id : String,

    #[structopt(long = "engine-name", default_value = "timely")]
    pub engine_name: String,

    #[structopt(long = "pegasus-thread-pool-size", default_value = "24")]
    pub pegasus_thread_pool_size: u32,

    #[structopt(long = "graph-type", default_value="vineyard")]
    pub graph_type: String,

    #[structopt(long = "graph-vineyard-object-id", default_value = "0")]
    pub vineyard_graph_id: i64,

    #[structopt(long = "lambda-enabled", parse(try_from_str), default_value = "false")]
    pub lambda_enabled: bool,
}

impl StoreConfig {
    pub fn init() -> Self {
        Self::init_with_args(vec![])
    }

    pub fn init_with_args(args: Vec<&str>) -> Self {
        let mut store_config = if args.len() > 0 {
            StoreConfig::from_iter(args.into_iter())
        } else {
            StoreConfig::from_args()
        };
        if let Ok(env) = env::var("YARN_WORKER_ENV") {
            info!("$YARN_WORKER_ENV={}", env);
            let json: Value = serde_json::from_str(env.as_str())
                .expect("parse $YARN_WORKER_ENV error");
            if let Some(x) = json["resource.executor.heapmem.mb"].as_str() {
                store_config.total_memory_mb = x.parse().unwrap();
            } else {
                warn!("resource.executor.heapmem.mb not found in YARN_WORKER_ENV");
            }

            if let Some(fs) = json["hdfs.default.fs"].as_str() {
                store_config.hdfs_default_fs = fs.parse().unwrap();
            } else {
                error!("hdfs.default.fs not found in YARN_WORKER_ENV");
            }

            if let Some(dir) = json["timely.prepare.persist.directory"].as_str() {
                store_config.timely_prepare_dir = dir.parse().unwrap();
            } else {
                warn!("prepare directory not found. use default. ")
            }

            if let Some(x) = json["worker.aliveid"].as_str() {
                store_config.alive_id = x.parse().unwrap();
            } else {
                warn!("worker.aliveid not found in YARN_WORKER_ENV");
            }

            if let Some(replica) = json["replica.count"].as_str() {
                store_config.replica_count = replica.parse().unwrap();
                store_config.worker_num = store_config.worker_num / store_config.replica_count;
            } else {
                warn!("replica.count not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["store.write.buffer.size"].as_str() {
                store_config.realtime_write_buffer_size = x.parse().unwrap();
            } else {
                warn!("store.write.buffer.size not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["store.write.buffer.mb"].as_str() {
                store_config.realtime_write_buffer_mb = x.parse().unwrap();
            } else {
                warn!("store.write.buffer.mb not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["store.precommit.buffer.size"].as_str() {
                store_config.realtime_precommit_buffer_size = x.parse().unwrap();
            } else {
                warn!("store.precommit.buffer.size not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["store.insert.thread.count"].as_str() {
                store_config.insert_thread_count = x.parse().unwrap();
            } else {
                warn!("store.insert.thread.count not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["resource.ingestnode.count"].as_str() {
                store_config.realtime_write_ingest_count = x.parse().unwrap();
                store_config.realtime_write_queue_count = store_config.realtime_write_ingest_count / store_config.replica_count;
            } else {
                warn!("resource.ingestnode.count not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["instance.unique.id"].as_str() {
                store_config.instance_id = x.to_owned();
            } else {
                warn!("instance.unique.id not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["zookeeper.auth.enable"].as_str() {
                if x == "true" {
                    store_config.zk_auth_enable = true;
                }
            } else {
                warn!("zookeeper.auth.enable not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["zookeeper.auth.user"].as_str() {
                store_config.zk_auth_user = x.to_owned();
            } else {
                warn!("zookeeper.auth.user not found in YARN_WORKER_ENV");
            }

            if let Some(x) = json["zookeeper.auth.password"].as_str() {
                store_config.zk_auth_password = x.to_owned();
            } else {
                warn!("zookeeper.auth.password not found in YARN_WORKER_ENV");
            }

        } else {
            warn!("environment variable $YARN_WORKER_ENV not found");
        }
        store_config
    }

    pub fn init_from_file(path: &str, server_id: &str) -> Self {
        let ini = ini::Ini::load_from_file(path).unwrap();
        let mut args = Vec::new();
        args.push("executor".to_owned());
        for (_sec, properties) in ini.iter() {
            for (k, v) in properties {
                let option = format!("--{}", k.replace(".", "-"));
                if option == "--worker-id" {
                    args.push(option);
                    args.push(server_id.to_string())
                } else {
                    args.push(option);
                    args.push(v.to_owned());
                }
            }
        }
        StoreConfig::from_iter_safe(args.into_iter()).unwrap()
    }

    pub fn init_from_config(store_options: &HashMap<String, String>) -> Self {
        let mut args = Vec::new();
        args.push("executor".to_owned());
        args.push("--worker-id".to_owned());
        args.push(store_options.get("node.idx").unwrap().to_owned());
        args.push("--graph-name".to_owned());
        args.push(store_options.get("graph.name").unwrap().to_owned());
        args.push("--partition-num".to_owned());
        args.push(store_options.get("partition.count").unwrap().to_owned());
        args.push("--graph-port".to_owned());
        args.push(store_options.get("graph.port").unwrap().to_owned());
        args.push("--query-port".to_owned());
        args.push(store_options.get("query.port").unwrap().to_owned());
        args.push("--engine-port".to_owned());
        args.push(store_options.get("engine.port").unwrap().to_owned());
        args.push("--timely-worker-per-process".to_owned());
        args.push(store_options.get("worker.per.process").unwrap().to_owned());
        args.push("--worker-num".to_owned());
        args.push(store_options.get("worker.num").unwrap().to_owned());
        StoreConfig::from_iter(args.into_iter())
    }

    pub fn update_alive_id(&mut self, alive_id: u64) {
        self.alive_id = alive_id;
        let mut new_path = String::new();
        {
            let disks: Vec<&str> = self.local_data_root.split(",").collect();
            let mut flag = false;
            for disk in disks {
                if flag {
                    new_path += ",";
                } else {
                    flag = true;
                }
                let p = format!("{}/{}_{}/{}_{}", disk, self.graph_name, self.instance_id, self.worker_id, self.alive_id);
                new_path += &p;
            }
        }
        self.local_data_root = new_path;
    }

    pub fn get_zk_auth(&self) -> Option<(String, String)> {
        if self.zk_auth_enable {
            Some((self.zk_auth_user.clone(), self.zk_auth_password.clone()))
        } else {
            None
        }
    }
}
