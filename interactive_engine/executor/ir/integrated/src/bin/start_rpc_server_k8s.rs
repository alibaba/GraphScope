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

use std::{env, sync::Arc};

use graph_proxy::{apis::PegasusClusterInfo, create_exp_store, SimplePartition};
use log::info;
use runtime::initialize_job_assembly;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let id = get_id();
    let servers_size = get_servers_size();
    let dns_name = get_dns_name();
    let engine_port = get_engine_port();
    let rpc_port = get_rpc_port();
    let server_config_string = generate_server_config_string(id, servers_size, &dns_name, engine_port);
    let rpc_config_string = generate_rpc_config_string(id, &dns_name, rpc_port);
    let server_config = pegasus::Configuration::parse(&server_config_string)?;
    let rpc_config = pegasus_server::rpc::RPCServerConfig::parse(&rpc_config_string)?;

    let cluster_info = Arc::new(PegasusClusterInfo::default());
    let exp_store = create_exp_store(cluster_info.clone());
    let partition_info = Arc::new(SimplePartition { num_servers: server_config.servers_size() });
    let job_assembly = initialize_job_assembly::<_, SimplePartition, PegasusClusterInfo>(
        exp_store,
        partition_info,
        cluster_info,
    );
    info!("try to start rpc server;");

    pegasus_server::cluster::standalone::start(rpc_config, server_config, job_assembly).await?;

    Ok(())
}

fn get_id() -> u64 {
    env::var("HOSTNAME")
        .expect("k8s cluster should set HOSTNAME env variable")
        .split("-")
        .last()
        .expect("invalid HOSTNAME format")
        .parse()
        .expect("server id should be a number")
}

fn get_servers_size() -> usize {
    env::var("SERVERSSIZE")
        .expect("k8s cluster should set SERVERSSIZE env variable")
        .parse()
        .expect("servers size should be a number")
}

fn get_dns_name() -> String {
    env::var("DNS_NAME_PREFIX_STORE").expect("k8s cluster should set DNS_NAME_PREFIX_STORE env variable")
}

fn get_rpc_port() -> usize {
    env::var("GAIA_RPC_PORT")
        .expect("k8s cluster should set GAIA_RPC_PORT env variable")
        .parse()
        .expect("port should be a number")
}

fn get_engine_port() -> usize {
    env::var("GAIA_ENGINE_PORT")
        .expect("k8s cluster should set GAIA_ENGINE_PORT env variable")
        .parse()
        .expect("port should be a number")
}

fn generate_server_config_string(
    id: u64, servers_size: usize, dns_name: &str, engine_port: usize,
) -> String {
    let mut server_config = format!(
        "[network]\n\
        server_id = {}\n\
        servers_size = {}\n",
        id, servers_size
    );
    for i in 0..servers_size {
        server_config.push_str(&format!(
            "[[network.servers]]\n\
            hostname = '{}'\n\
            port = {}\n",
            dns_name.replace("{}", &i.to_string()),
            engine_port
        ))
    }
    server_config
}

fn generate_rpc_config_string(id: u64, dns_name: &str, rpc_port: usize) -> String {
    format!(
        "rpc_host = '{}'\n\
        rpc_port = {}",
        dns_name.replace("{}", &id.to_string()),
        rpc_port
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_generate_server_config_string() {
        let id = 0;
        let servers_size = 3;
        let dns_name = "gaia-ir-rpc-{}.gaia-ir-rpc-hs.default.svc.cluster.local".to_string();
        let engine_port = 11234;
        let server_config_string = generate_server_config_string(id, servers_size, &dns_name, engine_port);
        let server_config = pegasus::Configuration::parse(&server_config_string).unwrap();
        let network_config = server_config.network.unwrap();
        assert_eq!(network_config.server_id, id);
        assert_eq!(network_config.servers_size, servers_size);
    }

    #[test]
    fn test_generate_rpc_config_string() {
        let id = 0;
        let dns_name = "gaia-ir-rpc-{}.gaia-ir-rpc-hs.default.svc.cluster.local".to_string();
        let rpc_port = 1234;
        let rpc_config_string = generate_rpc_config_string(id, &dns_name, rpc_port);
        let rpc_config = pegasus_server::rpc::RPCServerConfig::parse(&rpc_config_string).unwrap();
        let rpc_host = rpc_config.rpc_host.unwrap();
        let rpc_port = rpc_config.rpc_port.unwrap();
        assert_eq!(rpc_host, "gaia-ir-rpc-0.gaia-ir-rpc-hs.default.svc.cluster.local".to_string());
        assert_eq!(rpc_port, 1234)
    }
}
