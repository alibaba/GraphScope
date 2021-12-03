use std::ffi::OsStr;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use byteorder::ReadBytesExt;
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
    let path = if config.data_path.extension() != Some(OsStr::new("bin")) {
        pegasus_graph::encode(&config.data_path, |_| true).expect("failed");
        config.data_path.with_extension("bin")
    } else {
        config.data_path.clone()
    };
    let graph = pegasus_graph::load(path).expect("load fail");
    let mut buf = String::new();
    println!("########################################");
    let mut vec = Vec::with_capacity(1024);
    loop {
        print!("give vertex id: ");
        std::io::stdout().flush().unwrap();
        let read = std::io::stdin()
            .read_line(&mut buf)
            .expect("read error");
        if read > 0 {
            if let Ok(id) = buf
                .trim_matches(|c| c == ' ' || c == '\t' || c == '\n')
                .parse::<u64>()
            {
                let n = graph.count_neighbors(id);
                println!("  find {} neighbors for {}", n, id);
                buf.clear();
            } else if buf.starts_with("edge[") {
                let (_, id) = buf.split_at(5);
                if let Ok(id) = id
                    .trim_matches(|c| c == ' ' || c == '\t' || c == '\n' || c == ']')
                    .parse::<u64>()
                {
                    let start = Instant::now();
                    for n in graph.get_neighbors(id) {
                        vec.push(n);
                    }
                    println!(
                        "  find {} neighbors {:?} for {}, use {:?}",
                        vec.len(),
                        vec,
                        id,
                        start.elapsed()
                    );
                    vec.clear();
                }
                buf.clear()
            } else {
                println!("unknown command {}", buf);
            }
        }
    }
}
