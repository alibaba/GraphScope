use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use pegasus::api::{Binary, Fold, Iteration, Map, Sink};
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

/// Search and count khop neighbors for each vertex in a vertices list use only one job;
/// A correlated subtask will be created for each vertex;
#[derive(Debug, StructOpt)]
#[structopt(name = "de-correlated khop ", about = "Search khop neighbors on parallel dataflow")]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "k", default_value = "3")]
    k: u32,
    #[structopt(short = "t", default_value = "100")]
    starts: u32,
    #[structopt(short = "f")]
    use_loop: bool,
    /// specify the ids of start vertices;
    #[structopt(short = "i")]
    id_from_std: bool,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    #[structopt(short = "b", default_value = "64")]
    batch_width: u32,
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
    // config job;
    let mut conf = JobConf::new("de_correlated_k_hop");
    conf.set_workers(config.partitions);
    conf.batch_capacity = config.batch_width;
    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let src = if config.id_from_std {
        let mut buf = String::new();
        let mut ids = vec![];
        loop {
            let line = std::io::stdin().read_line(&mut buf).unwrap();
            if line == 0 {
                break;
            } else {
                buf.split(',')
                    .map(|id_str| {
                        let id = id_str.trim_end_matches(|c| c == '\n' || c == '\t' || c == ' ');
                        id.parse::<u64>().unwrap()
                    })
                    .for_each(|x| ids.push(x));
            }
            buf.clear();
        }
        ids
    } else {
        graph.sample_vertices(config.starts as usize)
    };

    let src = src
        .into_iter()
        .enumerate()
        .map(|(i, id)| (i as u64, id))
        .collect::<Vec<_>>();

    pegasus::wait_servers_ready(conf.servers());

    let k_hop = config.k;
    let nums = src.len();
    if nums == 0 {
        return;
    }

    println!("start search {}-hop neighbors for {} vertices;", k_hop, nums);

    assert!(k_hop > 0);
    let use_loop = config.use_loop;
    let start = Instant::now();
    let res = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        let graph = graph.clone();
        move |input, output| {
            let stream = input.input_from(src)?;
            let (left, mut right) = stream.copied()?;
            if use_loop {
                right = right.iterate(k_hop, |start| {
                    let graph = graph.clone();
                    start
                        .repartition(|(_, id)| Ok(*id))
                        .flat_map(move |(src, id)| Ok(graph.get_neighbors(id).map(move |id| (src, id))))
                })?
            } else {
                for _i in 0..k_hop {
                    let graph = graph.clone();
                    right = right
                        .repartition(|(_, id)| Ok(*id))
                        .flat_map(move |(src, id)| Ok(graph.get_neighbors(id).map(move |id| (src, id))))?;
                }
            };

            let group_cnt = right
                .fold_partition(HashMap::new(), || {
                    |mut count, (src, _)| {
                        let cnt = count.entry(src).or_insert(0u64);
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
                .unfold(move |map| Ok(map.into_iter()))?;

            left.repartition(|(src, _)| Ok(*src))
                .binary("join", group_cnt, |info| {
                    let mut map = TidyTagMap::<HashMap<u64, u64>>::new(info.scope_level);
                    move |input_left, input_right, output| {
                        input_left.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            let id_map = map.get_mut_or_insert(&dataset.tag);
                            for (k, v) in dataset.drain() {
                                if let Some(right) = id_map.remove(&k) {
                                    session.give((v, right, start.elapsed().as_micros() as u64))?;
                                } else {
                                    id_map.insert(k, v);
                                }
                            }
                            Ok(())
                        })?;

                        input_right.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            let id_map = map.get_mut_or_insert(&dataset.tag);
                            for (k, v) in dataset.drain() {
                                if let Some(left) = id_map.remove(&k) {
                                    session.give((left, v, start.elapsed().as_millis() as u64))?;
                                } else {
                                    id_map.insert(k, v);
                                }
                            }
                            Ok(())
                        })
                    }
                })?
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
            println!("{}\tfind khop\t{}\tneighbors, use {:?}ms;", id, cnt, elp);
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
    println!("avg khop count {}", total as f64 / len);

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
    println!("avg elapse {} ms", total as f64 / len);

    println!("==========================================================");
    let millis = elp.as_millis() as u64;
    println!("total use {}ms, qps: {:.1}", millis, nums as f64 * 1000f64 / millis as f64);

    pegasus::shutdown_all();
}
