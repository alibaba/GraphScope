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

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use std::fmt::Debug;
use std::path::Path;

use pegasus::{Configuration, StartupError};
use pegasus_network::config::{NetworkConfig, ServerAddr};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct HostsConfig {
    pub peers: Vec<ServerAddr>,
}

impl HostsConfig {
    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(&content)
    }

    pub fn read_from<P: AsRef<Path>>(path: P) -> Result<HostsConfig, StartupError> {
        let config_str = std::fs::read_to_string(path)?;
        Ok(HostsConfig::parse(&config_str)?)
    }
}

#[derive(Debug, Deserialize)]
pub struct CommonConfig {
    pub max_pool_size: Option<u32>,
    pub nonblocking: Option<bool>,
    pub read_timeout_ms: Option<u32>,
    pub write_timeout_ms: Option<u32>,
    pub read_slab_size: Option<u32>,
    pub no_delay: Option<bool>,
    pub send_buffer: Option<u32>,
    pub heartbeat_sec: Option<u32>,
}

impl CommonConfig {
    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(&content)
    }

    pub fn read_from<P: AsRef<Path>>(path: P) -> Result<CommonConfig, StartupError> {
        let config_str = std::fs::read_to_string(path)?;
        Ok(CommonConfig::parse(&config_str)?)
    }
}

pub fn combine_config(
    server_id: u64, host_config: Option<HostsConfig>, common_config: Option<CommonConfig>,
) -> Option<Configuration> {
    if let Some(host_config) = host_config {
        let local_host = &host_config.peers[server_id as usize];
        let ip = local_host.get_ip().to_owned();
        let port = local_host.get_port();
        let config = if let Some(common_config) = common_config {
            let mut network_config = NetworkConfig::with(server_id, host_config.peers);
            network_config.nonblocking(common_config.nonblocking)
                .read_timeout_ms(common_config.read_timeout_ms)
                .write_timeout_ms(common_config.write_timeout_ms)
                .read_slab_size(common_config.read_slab_size)
                .no_delay(common_config.no_delay)
                .send_buffer(common_config.send_buffer)
                .heartbeat_sec(common_config.heartbeat_sec);
            Configuration { network: Some(network_config), max_pool_size: common_config.max_pool_size }
        } else {
            let network_config = NetworkConfig::with(server_id, host_config.peers);
            Configuration { network: Some(network_config), max_pool_size: None }
        };
        Some(config)
    } else {
        if let Some(common_config) = common_config {
            Some(Configuration { network: None, max_pool_size: common_config.max_pool_size })
        } else {
            None
        }
    }
}
