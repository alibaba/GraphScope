use std::fmt::Debug;
use std::net::SocketAddr;

use pegasus::Data;
use prost::Message;

use crate::job::JobParser;
use crate::rpc::{RPCServerConfig, ServiceStartListener};
use crate::service::Service;

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

pub async fn start<I, O, P>(
    rpc_config: RPCServerConfig, server_config: pegasus::Configuration, job_parser: P,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: Data,
    O: Send + Debug + Message + 'static,
    P: JobParser<I, O>,
{
    let service = Service::new(job_parser);
    let detect = if let Some(net_conf) = server_config.network_config() {
        net_conf.get_servers()?.unwrap_or(vec![])
    } else {
        vec![]
    };

    crate::rpc::start_rpc_server(rpc_config, server_config, service, detect, StandaloneServiceListener)
        .await?;
    Ok(())
}
