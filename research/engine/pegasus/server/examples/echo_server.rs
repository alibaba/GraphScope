use std::path::PathBuf;

use log::info;
use pegasus::api::{Sink, Source};
use pegasus::result::ResultSink;
use pegasus::{BuildJobError, JobConf, ServerConf};
use pegasus_server::service::{JobDesc, JobParser};
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
    #[structopt(long = "server")]
    server: bool,
    #[structopt(long = "client")]
    client: bool,
}

struct EchoJobParser;

impl JobParser<Vec<u8>, Vec<u8>> for EchoJobParser {
    fn parse(
        &self, job: &JobDesc, input: &mut Source<Vec<u8>>, output: ResultSink<Vec<u8>>,
    ) -> Result<(), BuildJobError> {
        input
            .input_from(Some(job.input.clone()))?
            .sink_into(output)
    }
}

#[tokio::main]
async fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(config.config_dir).unwrap();
    if config.server {
        pegasus_server::cluster::standalone::start(rpc_config, server_config, EchoJobParser)
            .await
            .expect("start server failure;")
    } else if config.client {
        let mut client = pegasus_server::client::RPCJobClient::new();
        let servers = server_config
            .network_config()
            .map(|n| n.servers_size)
            .unwrap_or(1);
        if servers == 1 {
            let mut host = "0.0.0.0".to_owned();
            if let Some(addr) = rpc_config.rpc_host {
                host = addr;
            } else if let Some(net_conf) = server_config.network_config() {
                if let Some(addr) = net_conf
                    .get_server_addr(0)
                    .map(|s| s.get_ip().to_owned())
                {
                    host = addr;
                }
            }
            let port = rpc_config
                .rpc_port
                .as_ref()
                .copied()
                .expect("rpc port not found");
            info!("connect to server[0]({}:{})", host, port);
            let url = format!("http://{}:{}", host, port);
            client.connect(0, url).await.unwrap();
        } else {
            let net_conf = server_config
                .network_config()
                .expect("network config not found");
            let port = rpc_config
                .rpc_port
                .as_ref()
                .copied()
                .expect("rpc port not found");
            for i in 0..servers {
                let addr = net_conf
                    .get_server_addr(i as u64)
                    .expect("server not found");
                let host = addr.get_ip().to_owned();
                info!("connect to server[{}]({}:{})", i, host, port);
                client
                    .connect(i as u64, format!("http://{}:{}", host, port))
                    .await
                    .unwrap();
            }
        }
        for i in 0..100 {
            let mut conf = JobConf::with_id(i + 1, "Echo example", 1);
            conf.reset_servers(ServerConf::All);
            let mut job_desc = JobDesc::default();
            job_desc.set_input(vec![8u8; 8]);
            let result = client.submit(conf, job_desc).await.unwrap();
            let result_set: Result<Vec<Option<Vec<u8>>>, tonic::Status> = result.collect().await;
            let result_set = result_set.unwrap();
            assert_eq!(result_set.len(), servers);
            for r in result_set {
                assert!(r.is_some());
                assert_eq!(r.unwrap(), vec![8u8; 8]);
            }
        }
    } else {
        println!("--server or --client");
    }
}
