//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::{path::PathBuf, sync::Arc};

use graph_proxy::{apis::PegasusClusterInfo, create_exp_store, SimplePartition};
use log::info;
use runtime::initialize_job_assembly;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(config.config_dir).unwrap();

    let num_servers = server_config.servers_size();
    let cluster_info = Arc::new(PegasusClusterInfo::default());
    let exp_store = create_exp_store(cluster_info.clone());
    let partition_info = Arc::new(SimplePartition { num_servers });
    let job_assembly = initialize_job_assembly::<_, SimplePartition, PegasusClusterInfo>(
        exp_store,
        partition_info,
        cluster_info,
    );
    info!("try to start rpc server;");

    pegasus_server::cluster::standalone::start(rpc_config, server_config, job_assembly).await?;

    Ok(())
}
