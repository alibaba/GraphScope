use pegasus::api::{Dedup, Fold, Join, KeyBy, Map, Pair, Sink};
use pegasus::{Configuration, JobConf, ServerConf};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

/// This query begin from a set of sampling vertices (number specified by `source`)
/// and do i-hop search before enter subtask (specified by `i`),
/// and then count j-hop neighbors for each vertex using join method (specified by `j`).
/// A correlated subtask will be applied for each vertex;
#[derive(Debug, StructOpt)]
#[structopt(
name = "k-hop subtask using join",
about = "Search k-hop neighbors on parallel dataflow using join method"
)]
struct Config {
    /// the number of hops before subtask
    #[structopt(short = "i", default_value = "2")]
    outer_hop: u32,
    /// the number of hops in subtask
    #[structopt(short = "j", default_value = "2")]
    inner_hop: u32,
    /// the number of start vertices sampling from graph
    #[structopt(short = "s", default_value = "100")]
    source: u32,
    /// the file contains start vertices
    #[structopt(long = "source_path", parse(from_os_str))]
    source_path: Option<PathBuf>,
    /// the user-defined flat_map ratio
    #[structopt(short = "n")]
    degree: Option<u32>,
    /// the path of the origin graph data
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    #[structopt(short = "c", default_value = "32")]
    concurrent: u32,
    #[structopt(short = "b", default_value = "64")]
    batch_width: u32,
    #[structopt(default_value = "1024")]
    batch_size: u32,
    #[structopt(short = "m", long = "servers")]
    servers: Option<PathBuf>,
}

/// for generate source vertices from graph store, you can specify your own vertices using file.
fn get_source(path: &PathBuf) -> Vec<u64> {
    let mut sources = vec![];
    let f = std::fs::File::open(path).expect("source file not found;");
    let reader = std::io::BufReader::new(f);
    let mut lines = reader.lines();
    while let Some(Ok(id)) = lines.next() {
        let id = id.parse::<u64>().unwrap();
        sources.push(id);
    }
    sources
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
    // conf.scope_capacity = config.concurrent;
    conf.batch_capacity = config.batch_width;
    conf.batch_size = config.batch_size;

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    // let src = if let Some(ref source_file) = config.source_path {
    //     get_source(source_file)
    // } else {
    //     graph.sample_vertices(config.source as usize, 0.5)
    // };

    let mut src = Vec::new();
    src.push(1);

    let inner_hop = config.inner_hop;
    let outer_hop = config.outer_hop;
    let degree = config.degree;
    let conf = conf.clone();

    pegasus::wait_servers_ready(conf.servers());

    let start = Instant::now();
    let res = pegasus::run(conf.clone(), || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        let graph = graph.clone();
        move |input, output| {
            let mut stream = input.input_from(src)?;
            for _i in 0..outer_hop {
                let graph_clone = graph.clone();
                stream = stream.repartition(|id| Ok(*id));
                // if let Some(degree) = degree {
                //     stream = stream.flat_map(move |_id| {
                //         Ok(graph_clone
                //             .sample_neighbors(degree as usize)
                //             .into_iter())
                //     })?;
                // } else {
                //     stream = stream.flat_map(move |id| Ok(graph_clone.get_neighbors(id)))?;
                // }
                stream = stream.flat_map(move |id| Ok(graph_clone.get_neighbors(id)))?;
            }
            let (main, sub) = stream.repartition(|id| Ok(*id)).copied()?;

            let mut sub_stream = sub.dedup()?.key_by(|x| Ok((x, x)))?;
            for _j in 0..inner_hop {
                let g = graph.clone();
                sub_stream = sub_stream.repartition(|pair| Ok(pair.value));
                // if let Some(degree) = degree {
                //     sub_stream = sub_stream.flat_map(move |pair| {
                //         Ok(g.sample_neighbors(degree as usize)
                //             .into_iter()
                //             .map(move |n| Pair { key: pair.key, value: n }))
                //     })?;
                // } else {
                //     sub_stream = sub_stream.flat_map(move |pair| {
                //         Ok(g.get_neighbors(pair.value)
                //             .map(move |n| Pair { key: pair.key, value: n }))
                //     })?;
                // }
                sub_stream = sub_stream.flat_map(move |pair| {
                    Ok(g.get_neighbors(pair.value)
                        .map(move |n| Pair { key: pair.key, value: n }))
                })?;
            }
            let sub_count = sub_stream
                .fold_partition(HashMap::new(), || {
                    |mut count, pair| {
                        let cnt = count.entry(pair.key).or_insert(0u64);
                        *cnt += 1;
                        Ok(count)
                    }
                })?
                .unfold(|map| Ok(map.into_iter()))?
                .repartition(|(src, _)| Ok(*src))
                .fold_partition(HashMap::new(), || {
                    |mut count, (src, cnt)| {
                        let sum = count.entry(src).or_insert(0u64);
                        *sum += cnt;
                        Ok(count)
                    }
                })?
                .unfold(move |map| {
                    Ok(map
                        .into_iter()
                        .map(|(p, c)| Pair { key: p, value: c }))
                })?;

            main.key_by(|x| Ok((x, x)))?
                .inner_join(sub_count.repartition(|pair| Ok(pair.key)))?
                .map(|d| Ok((d.0.key, d.1.value)))?
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
            println!("{}\tfind k-hop\t{}\tneighbors, use {:?} ms;", id, cnt, elp);
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
    println!("{} k-hop counts range from: [{} .. {}]", len, cnt_list[0], cnt_list[len - 1]);
    let len = len as f64;
    let mut i = (len * 0.99) as usize;
    println!("99% k-hop count <= {}", cnt_list[i]);
    i = (len * 0.90) as usize;
    println!("90% k-hop count <= {}", cnt_list[i]);
    i = (len * 0.5) as usize;
    println!("50% k-hop count <= {}", cnt_list[i]);
    let total: u64 = cnt_list.iter().sum();
    println!("avg k-hop count {}/{} {}", total, len, total as f64 / len);

    println!("==========================================================");
    elp_list.sort();
    let len = elp_list.len();
    println!("{} k-hop elapses range from: [{} .. {}]", len, elp_list[0], elp_list[len - 1]);
    let len = len as f64;
    let mut i = (len * 0.99) as usize;
    println!("99% elapse <= {} ms", elp_list[i]);
    i = (len * 0.90) as usize;
    println!("90% elapse <= {} ms", elp_list[i]);
    i = (len * 0.5) as usize;
    println!("50% elapse <= {} ms", elp_list[i]);
    let total: u64 = elp_list.iter().sum();
    println!("avg elapse {} ms", total as f64 / len);

    println!("==========================================================");
    let millis = elp.as_millis() as f64;
    println!("total use {} ms, qps: {:.1}", millis, config.source as f64 * 1000.0 / millis);

    pegasus::shutdown_all();
}