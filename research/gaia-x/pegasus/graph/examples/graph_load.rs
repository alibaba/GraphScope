use std::path::PathBuf;
use std::time::{Duration, Instant};

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "graph_load", about = "load graph topology")]
struct Config {
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
}

fn main() {
    let config: Config = Config::from_args();
    let graph = pegasus_graph::load(&config.data_path).unwrap();
    let samples = graph.sample_vertices(1000_000);
    let start = Instant::now();
    let mut count = 0;
    for id in samples {
        count += graph.count_neighbors(id);
    }
    println!(
        "searched 1 million vertices' neighbors, total touched {} edges, use: {:?};",
        count,
        start.elapsed()
    );
}
