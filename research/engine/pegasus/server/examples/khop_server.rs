use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "khop_server", about = "Search khop neighbors on parallel dataflow")]
struct Config {
    #[structopt(short, long, default_value = "0.0.0.0")]
    host: String,
    #[structopt(short, long, default_value = "443")]
    port: u64,
    #[structopt(parse(from_os_str))]
    graph: PathBuf,
    #[structopt(parse(from_os_str), long = "servers")]
    server_conf: Option<PathBuf>,
}

fn main() {
    let config: Config = Config::from_args();
}
