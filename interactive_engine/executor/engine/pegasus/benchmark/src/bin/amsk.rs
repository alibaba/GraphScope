use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

/// Search and count k-hop neighbors for each vertex in a list using only one job;
/// Do k-hop searching for each vertex in async mode;
#[derive(Debug, StructOpt)]
#[structopt(
    name = "async multi-src k-hop ",
    about = "Search k-hop neighbors using parallel dataflow system"
)]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "k", default_value = "3")]
    k: u32,
    #[structopt(short = "n", default_value = "100")]
    starts: u32,
    #[structopt(short = "l")]
    is_limit_one: bool,
    #[structopt(short = "f")]
    use_loop: bool,
    #[structopt(short = "i")]
    id_from_std: bool,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    /// max number of searching in parallel
    #[structopt(short = "c", default_value = "32")]
    concurrent: u32,
    #[structopt(short = "b", default_value = "64")]
    batch_capacity: u32,
    /// path of config file with server addresses
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
    let mut conf = JobConf::new("correlated_k_hop");
    conf.set_workers(config.partitions);
    conf.batch_capacity = config.batch_capacity;

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
        graph.sample_vertices(config.starts as usize, 0.1)
    };

    let k_hop = config.k;
    let is_limit_one = config.is_limit_one;
    assert!(k_hop > 0);
    let nums = src.len();
    if nums == 0 {
        return;
    }
    println!("start search {}-hop neighbors for {} vertices;", k_hop, nums);

    let use_loop = config.use_loop;
    let conf = conf.clone();

    pegasus::wait_servers_ready(conf.servers());

    let start = Instant::now();
    let res = pegasus_benchmark::queries::khop::unpacked_multi_src_k_hop(
        src,
        k_hop,
        use_loop,
        is_limit_one,
        conf,
        graph,
    );

    let mut cnt_list = Vec::new();
    let mut elp_list = Vec::new();
    let mut n = 10;

    for x in res {
        let (id, cnt, elp) = x.unwrap();
        if n > 0 {
            println!("{}\tfind k-hop\t{}\tneighbors, use {:?}us;", id, cnt, elp);
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
    println!("avg k-hop count {}", total as f64 / len);

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
    println!("avg elapse {} us", total as f64 / len);

    println!("==========================================================");
    let millis = elp.as_millis() as f64;
    println!("total use {}ms, qps: {:.1}", millis, nums as f64 * 1000.0 / millis);

    pegasus::shutdown_all();
}
