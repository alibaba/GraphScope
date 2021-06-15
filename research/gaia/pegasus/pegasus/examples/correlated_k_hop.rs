use pegasus::api::Range::Global;
use pegasus::api::{Count, Exchange, Iteration, Map, ResultSet, Sink, SubTask};
use pegasus::preclude::Pipeline;
use pegasus::{Configuration, JobConf, ServerConf};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

/// Search and count k-hop neighbors for each vertex in a vertices list use only one job;
/// A correlated subtask will be created for each vertex;
#[derive(Debug, StructOpt)]
#[structopt(name = "correlated k-hop ", about = "Search k-hop neighbors on parallel dataflow")]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "k", default_value = "3")]
    k: u32,
    #[structopt(short = "t", default_value = "100")]
    times: u32,
    #[structopt(short = "f")]
    use_loop: bool,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
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
    let k_hop = config.k;
    assert!(k_hop > 0);
    let use_loop = config.use_loop;
    let conf = conf.clone();
    let start = Instant::now();
    let mut g = pegasus::run(conf.clone(), |worker| {
        let index = worker.id.index;
        let src = if index == 0 { src.clone() } else { vec![] };
        let graph = graph.clone();
        worker.dataflow(move |dfb| {
            let stream = dfb.input_from_iter(src.into_iter())?;
            let subtasks = stream.fork_subtask(|sub| {
                let stream = if use_loop {
                    sub.iterate(k_hop, |start| {
                        let graph = graph.clone();
                        start.exchange_with_fn(|id| *id)?.flat_map_with_fn(Pipeline, move |id| {
                            Ok(graph.get_neighbors(id).map(|item| Ok(item)))
                        })
                    })?
                    .count(Global)?
                } else {
                    let g = graph.clone();
                    let mut stream =
                        sub.exchange_with_fn(|id| *id)?.flat_map_with_fn(Pipeline, move |id| {
                            Ok(g.get_neighbors(id).map(|item| Ok(item)))
                        })?;

                    for _i in 1..k_hop {
                        let g = graph.clone();
                        stream = stream
                            .exchange_with_fn(|id| *id)?
                            .flat_map_with_fn(Pipeline, move |id| {
                                Ok(g.get_neighbors(id).map(|item| Ok(item)))
                            })?;
                    }
                    stream.count(Global)?
                };

                Ok(stream)
            })?;

            stream.join_subtask(subtasks, |p, s| Some((*p, s)))?.sink_by(|info| {
                let index = info.worker_id.index;
                let mut n = 10;
                let mut cnt_list = vec![];
                let mut elp_list = vec![];
                move |_, result| match result {
                    ResultSet::Data(result) => {
                        for (src, cnt) in result {
                            let elp = start.elapsed();
                            if n > 0 {
                                println!("{}\tfind\t{}\tk-hop neighbors, use\t{:?}", src, cnt, elp);
                                n -= 1;
                            }
                            cnt_list.push(cnt);
                            elp_list.push(elp.as_millis() as u64);
                        }
                    }
                    ResultSet::End => {
                        if index == 0 {
                            println!("...");
                            println!("==========================================================");
                            cnt_list.sort();
                            let len = cnt_list.len();
                            println!(
                                "{} k-hop counts range from: [{} .. {}]",
                                len,
                                cnt_list[0],
                                cnt_list[len - 1]
                            );
                            let len = len as f64;
                            let mut i = (len * 0.99) as usize;
                            println!("99% k-hop count <= {}", cnt_list[i]);
                            i = (len * 0.90) as usize;
                            println!("90% k-hop count <= {}", cnt_list[i]);
                            i = (len * 0.5) as usize;
                            println!("50% k-hop count <= {}", cnt_list[i]);
                            let total: u64 = cnt_list.iter().sum();
                            println!("avg k-hop count {}", total as f64 / len);

                            println!("==========================================================");
                            elp_list.sort();
                            let len = elp_list.len();
                            println!(
                                "{} k-hop elapses range from: [{} .. {}]",
                                len,
                                elp_list[0],
                                elp_list[len - 1]
                            );
                            let len = len as f64;
                            let mut i = (len * 0.99) as usize;
                            println!("99% elapse <= {} ms", elp_list[i]);
                            i = (len * 0.90) as usize;
                            println!("90% elapse <= {} ms", elp_list[i]);
                            i = (len * 0.5) as usize;
                            println!("50% elapse <= {} ms", elp_list[i]);
                            let total: u64 = elp_list.iter().sum();
                            println!("avg elapse {} ms", total as f64 / len);
                        }
                    }
                }
            })?;
            Ok(())
        })
    })
    .unwrap()
    .unwrap();

    g.join().unwrap();
    pegasus::shutdown_all();
}
