use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use pegasus::api::{CorrelatedSubTask, Count, Map, Sink};
use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

/// This query begin from a set of sampling vertices (number specified by `source`)
/// and do i-hop search before enter subtask (specified by `i`),
/// and then count j-hop neighbors for each vertex using apply method (specified by `j`).
/// A correlated subtask will be applied for each vertex;
#[derive(Debug, StructOpt)]
#[structopt(name = "correlated khop ", about = "Search khop neighbors on parallel dataflow")]
struct Config {
    /// the number of hops before subtask
    #[structopt(short = "i", default_value = "2")]
    outer_hop: u32,
    /// the number of hops in subtask;
    #[structopt(short = "j", default_value = "2")]
    inner_hop: u32,
    /// the number of start vertices sampling from graph
    #[structopt(short = "s", default_value = "100")]
    source: u32,
    /// the path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    #[structopt(short = "c", default_value = "32")]
    concurrent: u32,
    #[structopt(short = "b", default_value = "64")]
    batch_width: u32,
    #[structopt(short = "m", long = "servers")]
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
    // config job;
    let mut conf = JobConf::new("correlated_k_hop");
    conf.set_workers(config.partitions);
    conf.batch_capacity = config.batch_width;

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let src = graph.sample_vertices(config.source as usize);
    let inner_hop = config.inner_hop;
    let outer_hop = config.outer_hop;
    let conf = conf.clone();

    pegasus::wait_servers_ready(conf.servers());

    let start = Instant::now();
    let parallel = config.concurrent;
    let res = pegasus::run(conf.clone(), || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        let graph = graph.clone();
        move |input, output| {
            let mut stream = input.input_from(src)?;
            for _i in 0..outer_hop {
                let graph_clone = graph.clone();
                stream = stream
                    .repartition(|id| Ok(*id))
                    .flat_map(move |id| Ok(graph_clone.get_neighbors(id)))?;
            }
            stream
                .repartition(|id| Ok(*id))
                .apply_parallel(parallel, |sub| {
                    let mut stream = sub;

                    for _j in 0..inner_hop {
                        let g = graph.clone();
                        stream = stream
                            .repartition(|id| Ok(*id))
                            .flat_map(move |id| Ok(g.get_neighbors(id)))?;
                    }
                    stream.count()
                })?
                .map(move |(id, cnt)| Ok((id, cnt, start.elapsed().as_millis() as u64)))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut cnt_list = Vec::new();
    let mut elp_list = Vec::new();
    let mut n = 10;

    for x in res {
        let (id, cnt, elp) = x.unwrap();
        if n > 0 {
            println!("{}\tfind khop\t{}\tneighbors, use {:?}us;", id, cnt, elp);
        }
        n -= 1;
        cnt_list.push(cnt);
        elp_list.push(elp);
    }

    let elp = start.elapsed();
    println!("...");
    println!("==========================================================");

    cnt_list.sort();
    let len = cnt_list.len();
    println!("{} khop counts range from: [{} .. {}]", len, cnt_list[0], cnt_list[len - 1]);
    let len = len as f64;
    let mut i = (len * 0.99) as usize;
    println!("99% khop count <= {}", cnt_list[i]);
    i = (len * 0.90) as usize;
    println!("90% khop count <= {}", cnt_list[i]);
    i = (len * 0.5) as usize;
    println!("50% khop count <= {}", cnt_list[i]);
    let total: u64 = cnt_list.iter().sum();
    println!("avg khop count {}/{} {}", total, len, total as f64 / len);

    println!("==========================================================");
    elp_list.sort();
    let len = elp_list.len();
    println!("{} khop elapses range from: [{} .. {}]", len, elp_list[0], elp_list[len - 1]);
    let len = len as f64;
    let mut i = (len * 0.99) as usize;
    println!("99% elapse <= {} ms", elp_list[i]);
    i = (len * 0.90) as usize;
    println!("90% elapse <= {} ms", elp_list[i]);
    i = (len * 0.5) as usize;
    println!("50% elapse <= {} ms", elp_list[i]);
    let total: u64 = elp_list.iter().sum();
    println!("avg elapse {} us", total as f64 / len);

    println!("==========================================================");
    let millis = elp.as_millis() as f64;
    println!("total use {}ms, qps: {:.1}", millis, config.source as f64 * 1000.0 / millis);

    pegasus::shutdown_all();
}
