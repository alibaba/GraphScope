use std::path::PathBuf;
use pegasus::{JobConf, Configuration, ServerConf};
use structopt::StructOpt;
use std::sync::Arc;
use pegasus::api::{Exchange, Map, Count, Range, Sink, ResultSet};
use pegasus::preclude::Pipeline;
use std::time::Instant;


#[derive(Debug, StructOpt)]
#[structopt(name = "k-hop", about = "Search k-hop neighbors on parallel dataflow")]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "k", default_value = "3")]
    k           : u32,
    #[structopt(short = "t", default_value = "100")]
    times       : u32,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path   : PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions : u32,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
}

fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    pegasus::startup(server_conf).unwrap();
    let graph = Arc::new(pegasus_graph::load(&config.data_path).unwrap());
    let mut conf = JobConf::new("k-hop");
    conf.set_workers(config.partitions);
    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let src = graph.sample_vertices(config.times as usize);
    let mut guards = Vec::new();
    let k_hop = config.k;
    for (i, id) in src.into_iter().enumerate() {
        let mut conf = conf.clone();
        conf.job_id = i as u64;
        let start = Instant::now();
        let g = pegasus::run(conf.clone(), |worker| {
            let index = worker.id.index;
            let graph = graph.clone();
            worker.dataflow(move |dfb| {
                let mut stream = if index == 0 {
                    dfb.input_from_iter(vec![id].into_iter())
                } else {
                    dfb.input_from_iter(vec![].into_iter())
                }?;
                for _i  in 0..k_hop {
                    let graph = graph.clone();
                    stream = stream.exchange_with_fn(|id| *id)?
                        .flat_map_with_fn(Pipeline, move |id| {
                            Ok(graph.get_neighbors(id).map(|item| Ok(item)))
                        })?;
                }
                stream.count(Range::Global)?
                    .sink_by(|info| {
                        let job_id = info.worker_id.job_id;
                        move |_, result| {
                            match result {
                                ResultSet::Data(cnt) => {
                                    println!("job[{}]: {} has k-hop {} neighbors, use {:?}", job_id, id, cnt[0], start.elapsed());
                                }
                                ResultSet::End => {}
                            }
                        }
                    })?;
                Ok(())
            })
        }).unwrap().unwrap();
        guards.push(g);
    }

    pegasus::shutdown_all();
}