use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use pegasus::api::{Count, Iteration, Map, Sink};
use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "khop", about = "Search khop neighbors on parallel dataflow")]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "k", default_value = "3")]
    k: u32,
    #[structopt(short = "n", default_value = "100")]
    starts: u32,
    #[structopt(short = "i")]
    id_from_std: bool,
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
    let mut conf = JobConf::new("k_hop");
    conf.set_workers(config.partitions);
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

    pegasus::wait_servers_ready(conf.servers());

    let mut results = Vec::new();
    let k_hop = config.k;
    let nums = src.len();

    if nums == 0 {
        return;
    }

    println!("start search {}-hop neighbors for {} vertices;", k_hop, nums);

    let use_loop = config.use_loop;
    let global_start = Instant::now();
    for (i, id) in src.into_iter().enumerate() {
        let mut conf = conf.clone();
        conf.job_id = i as u64;
        conf.plan_print = i <= 0;
        let start = Instant::now();
        let result = pegasus::run(conf.clone(), || {
            let index = pegasus::get_current_worker().index;
            let graph = graph.clone();
            let src = if index == 0 { vec![id] } else { vec![] };
            move |input, output| {
                let mut stream = input.input_from(src)?;
                if use_loop {
                    stream = stream.iterate(k_hop, |start| {
                        let graph = graph.clone();
                        start
                            .repartition(|id| Ok(*id))
                            .flat_map(move |id| Ok(graph.get_neighbors(id)))
                    })?;
                } else {
                    for _i in 0..k_hop {
                        let graph = graph.clone();
                        stream = stream
                            .repartition(|id| Ok(*id))
                            .flat_map(move |id| Ok(graph.get_neighbors(id)))?;
                    }
                }
                stream
                    .count()?
                    .map(move |cnt| {
                        let x = start.elapsed();
                        Ok((id, cnt, x))
                    })?
                    .sink_into(output)
            }
        })
        .expect("submit job failure;");
        results.push(result);
    }

    let mut cnt_list = Vec::new();
    let mut elp_list = Vec::new();
    let mut n = 10;

    for res in results {
        for x in res {
            let (id, cnt, elp) = x.unwrap();
            if cnt > 0 {
                if n > 0 {
                    println!("{}\tfind khop\t{}\tneighbors, use {:?};", id, cnt, elp);
                }
                n -= 1;
                cnt_list.push(cnt);
                elp_list.push(elp.as_millis() as u64);
            }
        }
    }
    let elp = global_start.elapsed();
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
    let millis = elp.as_millis() as f64;
    println!("total use {}ms, qps: {:.1}", millis, nums as f64 * 1000.0 / millis);
    pegasus::shutdown_all();
}
