use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::Instant;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "graph_load", about = "load graph topology")]
struct Config {
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    #[structopt(short = "s", default_value = "10")]
    start: u32,
    #[structopt(short = "k", default_value = "1")]
    hop: u8,
}

fn main() {
    let config: Config = Config::from_args();
    let path = if config.data_path.extension() != Some(OsStr::new("bin")) {
        pegasus_graph::encode(&config.data_path, |_| true).expect("failed");
        config.data_path.with_extension("bin")
    } else {
        config.data_path.clone()
    };
    let graph = pegasus_graph::load(path).expect("load fail");
    let mut vertices = graph.sample_vertices(config.start as usize, 0.1);
    vertices.sort();
    let mut total = Vec::with_capacity(config.start as usize);
    let start = Instant::now();
    for v in vertices {
        let res = graph.get_k_hop_neighbors(v, config.hop).count();
        total.push(res);
    }
    let elapsed = start.elapsed();
    let mut cnt = 0;
    for (i, r) in total.into_iter().enumerate() {
        cnt += r;
        if i < 10 || i > 99990 {
            print!("{}, ", r);
        } else {
            print!(".")
        }
        if i % 10 == 0 {
            println!()
        }
    }

    println!("total found {} neighbors for {} vertices, used {:?}", cnt, config.start, elapsed)
}
