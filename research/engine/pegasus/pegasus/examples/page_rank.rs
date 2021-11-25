use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use nohash_hasher::IntMap;
use pegasus::api::{Fold, IterCondition, Iteration, Map, Reduce, Sink};
use pegasus::resource::PartitionedResource;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_graph::{graph::Neighbors, Graph};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Page-Rank", about = "Distribute page-rank implementation based on Dataflow Model;")]
struct Config {
    /// Total pages in the graph;
    #[structopt(short = "v")]
    vertices: u64,
    /// The number of iterations this job will do;
    #[structopt(short = "i", default_value = "10")]
    max_iters: u32,
    #[structopt(short = "d", default_value = "0.001")]
    min_delta: f64,
    #[structopt(short = "f", default_value = "0.85")]
    damping_factor: f64,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    #[structopt(short = "o", parse(from_os_str))]
    output: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
}

struct PagesAndRanks {
    ranks: IntMap<u64, f64>,
    graph: Arc<Graph>,
    damping_factor: f64,
    damping_value: f64,
    output: PathBuf,
    update: Arc<ArrayQueue<(u64, f64)>>,
}

impl PagesAndRanks {
    fn new(total_pages: usize, graph: Graph, damping_factor: f64, output: PathBuf) -> Self {
        println!("create partitions with {} vertices", graph.total_vertices());
        let init = 1.0 / total_pages as f64;
        let mut ranks: IntMap<u64, f64> = Default::default();
        for v in graph.vertices() {
            ranks.insert(*v, init);
        }
        let update = Arc::new(ArrayQueue::new(ranks.len()));
        let damping_value = (1.0 - damping_factor) / total_pages as f64;
        PagesAndRanks { ranks, graph: Arc::new(graph), update, damping_factor, damping_value, output }
    }

    fn scatter(&self) -> ScatterItemIter {
        assert!(self.update.is_empty());
        for (p, r) in self.ranks.iter() {
            self.update
                .push((*p, *r))
                .expect("failure in scatter;");
        }
        ScatterItemIter {
            damping_factor: self.damping_factor,
            pr: self.update.clone(),
            graph: self.graph.clone(),
            iter: None,
        }
    }

    fn apply(&mut self, update: IntMap<u64, f64>) -> f64 {
        let mut change = 0.0;
        for (p, r) in update {
            let r = r + self.damping_value;
            if let Some(old) = self.ranks.get_mut(&p) {
                change += (*old - r).abs();
                *old = r;
            }
        }
        change
    }
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
    let mut conf = JobConf::new("page_rank");
    conf.set_workers(config.partitions);
    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let resources = prepare_resources(&config, &conf);
    pegasus::wait_servers_ready(conf.servers());

    let max_iters = config.max_iters;
    let delta = config.min_delta;
    let mut result = pegasus::run_with_resources(conf, resources, || {
        move |input, output| {
            let src = input.input_from(Some(1.0))?;
            let mut until = IterCondition::max_iters(max_iters);
            until.until(move |cge| Ok(*cge <= delta));
            src.iterate_until(until, |start| {
                start
                    .flat_map(|x| {
                        println!("start new iteration, current rank delta {}", x);
                        let page_rank = pegasus::resource::get_resource_mut::<PagesAndRanks>().unwrap();
                        Ok(page_rank.scatter())
                    })?
                    // gather: shuffle by target page id, sum all in-link's update rank;
                    .repartition(|x| Ok(x.0))
                    .fold_partition(IntMap::default(), || {
                        |mut map, (p, r)| {
                            let rank = map.entry(p).or_insert(0.0);
                            *rank += r;
                            Ok(map)
                        }
                    })?
                    .unfold(|map| {
                        let mut page_rank = pegasus::resource::get_resource_mut::<PagesAndRanks>().unwrap();
                        let change = page_rank.apply(map);
                        println!("after iteration rank delta {} on partition", change);
                        Ok(Some(change).into_iter())
                    })?
                    .broadcast()
                    .reduce_partition(|| |a, b| Ok(a + b))?
                    .into_stream()
            })?
            .sink_into(output)
        }
    })
    .expect("submit job failure;");
    while let Some(Ok(delta)) = result.next() {
        println!("page rank converge delta: {:.3}", delta);
    }

    pegasus::shutdown_all();
}

fn prepare_resources(config: &Config, conf: &JobConf) -> PartitionedResource<PagesAndRanks> {
    let graph = pegasus_graph::partition(&config.data_path, config.partitions as usize).unwrap();
    let total_pages = config.vertices as usize;

    let mut page_partitions = Vec::with_capacity(graph.len());
    for (i, par) in graph.into_iter().enumerate() {
        let output = config.output.join(format!("partitions-{}", i));
        page_partitions.push(PagesAndRanks::new(total_pages, par, config.damping_factor, output));
    }
    PartitionedResource::new(&conf, page_partitions)
        .ok()
        .unwrap()
}

impl Drop for PagesAndRanks {
    fn drop(&mut self) {
        if let Ok(file) = std::fs::File::create(&self.output) {
            let mut write = std::io::LineWriter::new(file);
            for (p, r) in self.ranks.iter() {
                if let Err(e) = write.write_all(format!("{} {}", p, r).as_bytes()) {
                    eprintln!("write result failure {}", e);
                    break;
                }
                write.write_all(b"\n").ok();
            }
        } else {
            eprintln!("create result failure;");
        }
    }
}

struct NeighborsRankUpdate {
    neighbors: Neighbors,
    update: f64,
}

impl Iterator for NeighborsRankUpdate {
    type Item = (u64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.neighbors.next() {
            Some((next, self.update))
        } else {
            None
        }
    }
}

struct ScatterItemIter {
    damping_factor: f64,
    pr: Arc<ArrayQueue<(u64, f64)>>,
    graph: Arc<Graph>,
    iter: Option<NeighborsRankUpdate>,
}

impl Iterator for ScatterItemIter {
    type Item = (u64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.iter {
            if let Some(next) = iter.next() {
                return Some(next);
            }
        }

        loop {
            if let Ok((p, r)) = self.pr.pop() {
                let neighbors = self.graph.get_neighbors(p);
                if neighbors.len() > 0 {
                    let update = self.damping_factor * (r / neighbors.len() as f64);
                    let iter = NeighborsRankUpdate { neighbors, update };
                    self.iter = Some(iter);
                    return self.next();
                }
            } else {
                return None;
            }
        }
    }
}
