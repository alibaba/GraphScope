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

use std::net::SocketAddr;

use pegasus::Data;

use crate::job::JobAssembly;
use crate::rpc::{RPCServerConfig, ServiceStartListener};

struct StandaloneServiceListener;

impl ServiceStartListener for StandaloneServiceListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("RPC server of server[{}] start on {}", server_id, addr);
        Ok(())
    }

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("compute server[{}] start on {}", server_id, addr);
        Ok(())
    }
}

pub async fn start<I: Data, P>(
    rpc_config: RPCServerConfig, server_config: pegasus::Configuration, assemble: P,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: JobAssembly<I>,
{
    let detect = if let Some(net_conf) = server_config.network_config() {
        net_conf.get_server_addrs()?
    } else {
        vec![]
    };

    crate::rpc::start_all(rpc_config, server_config, assemble, detect, StandaloneServiceListener).await?;
    Ok(())
}
