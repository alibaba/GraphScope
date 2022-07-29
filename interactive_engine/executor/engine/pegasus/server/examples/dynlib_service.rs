use std::path::PathBuf;

use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_server::job::{DynLibraryAssembly, JobDesc};
use pegasus_server::rpc::RPCServerConfig;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[derive(Debug, StructOpt)]
#[structopt(name = "dynamic library service", about = "example of rpc service")]
struct Config {
    #[structopt(long = "server")]
    server: bool,
    #[structopt(long = "client")]
    client: bool,
}

#[tokio::main]
async fn main() {
    pegasus_common::logs::init_log();
    let server_config = Configuration::parse(
        r#"
        max_pool_size = 8
        [network]
        server_id = 0
        servers_size = 1
        [[network.servers]]
        ip = '127.0.0.1'
        port = 8090
    "#,
    )
    .unwrap();

    let rpc_config = RPCServerConfig::parse(
        r#"
        rpc_host = "127.0.0.1"
        rpc_port = 5007
    "#,
    )
    .unwrap();

    let config: Config = Config::from_args();
    if config.server {
        pegasus_server::cluster::standalone::start(rpc_config, server_config, DynLibraryAssembly)
            .await
            .expect("start server failure;")
    } else if config.client {
        let mut client = pegasus_server::client::RPCJobClient::new();

        let port = rpc_config
            .rpc_port
            .as_ref()
            .copied()
            .expect("rpc port not found");
        let url = format!("http://{}:{}", "127.0.0.1", port);
        client.connect(0, url).await.unwrap();

        let mut path = PathBuf::from("./dynlib_exp");
        path.push("target");
        path.push("debug");
        path.push("libdynexp.so");

        client
            .add_library("libdynexp", path.as_path())
            .await
            .expect("add library failure");

        let mut conf = JobConf::with_id(99, "dynlibexp", 1);
        conf.reset_servers(ServerConf::All);
        let mut job_desc = JobDesc::default();
        job_desc
            .set_input(vec![8u8; 8])
            .set_plan(b"build_job".to_vec())
            .set_resource(b"libdynexp".to_vec());
        let result = client.submit(conf, job_desc).await.unwrap();
        let result_set: Result<Vec<Vec<u8>>, tonic::Status> = result.collect().await;
        let result_set = result_set.unwrap();
        for res in result_set {
            assert_eq!(res, vec![9u8; 8]);
        }
    } else {
        println!("--server or --client");
    }
}
