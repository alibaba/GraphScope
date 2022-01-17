use std::path::PathBuf;
use pegasus::api::{Sink, Source};
use pegasus::BuildJobError;
use pegasus::result::ResultSink;
use pegasus_server::service::{JobDesc, JobParser};

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
}


struct EchoJobParser;

impl JobParser<Vec<u8>, Vec<u8>> for EchoJobParser {
    fn parse(&self, job: &JobDesc, input: &mut Source<Vec<u8>>, output: ResultSink<Vec<u8>>) -> Result<(), BuildJobError> {
        input.input_from(Some(job.input.clone()))?
            .sink_into(output)
    }
}

#[tokio::main]
async fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(config.config_dir).unwrap();
    pegasus_server::cluster::standalone::start(rpc_config, server_config, EchoJobParser).await.expect("start server failure;")
}