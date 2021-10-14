//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

extern crate timely;

use std::sync::Arc;
use std::time::{Instant, Duration};
use std::convert::AsRef;
use std::path::Path;
use std::io::BufRead;

use structopt::StructOpt;
use log::*;
use pegasus::try_init_logger;
use timely::Configuration;
use timely::dataflow::operators::*;
use pegasus::graph::topology::{Vertex, GraphTopology, NeighborIter};
use pegasus::memory::MemMonitor;

use crossbeam_queue::SegQueue;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "k", long="khop")]
    pub k: u32,
    #[structopt(short = "q", long = "queries")]
    pub queries: String,
    #[structopt(short = "w", long = "worker", default_value = "1")]
    pub worker: usize,
    #[structopt(short = "f", long = "file")]
    pub file: String,
    #[structopt(short = "b", long = "binary")]
    pub binary: bool,
}

#[derive(Debug, Copy, Clone)]
pub struct KHopConfig {
    pub src: u64,
}

impl KHopConfig {
    pub fn from_str(line: String) -> Option<Self> {
        let split = line.split(',').collect::<Vec<_>>();
        if split.len() < 1 {
            None
        } else {
            let src = split[0].parse::<u64>().unwrap();
            Some(KHopConfig {
                src,
            })
        }
    }
}

fn read_queries_from_file<P: AsRef<Path>>(file: P) -> SegQueue<KHopConfig> {
    let reader = ::std::io::BufReader::new(::std::fs::File::open(file).unwrap());
    let queue = SegQueue::new();
    for query in reader.lines() {
        if let Ok(query) = query {
            if let Some(khop) = KHopConfig::from_str(query) {
                queue.push(khop);
            }
        }
    }
    queue
}

fn main() {
    try_init_logger().ok();
    let config: Config = Config::from_args();
    info!("Config: {:?}", config);
    let k = config.k;
    let workers = config.worker;
    let file = config.file;
    let binary = config.binary;

    let mut graphs = Vec::with_capacity(workers);
    let load_start = Instant::now();
    info!("start to load graph ...");
    for index in 0..workers {
        let file = file.clone();
        let join = ::std::thread::spawn(move || {
            if binary {
                GraphTopology::load_bin(index as u32, workers as u32, true, file)
            } else {
                GraphTopology::load(index as u32, workers as u32, true, ',',  file)
            }
        });
        graphs.push(join);
    }

    let mut graphs = graphs.into_iter()
        .map(|j| j.join().unwrap())
        .map(|g| Arc::new(g))
        .collect::<Vec<_>>();
    info!("finished load graph, cost {:?}", load_start.elapsed());

    let queries = Arc::new(read_queries_from_file(config.queries));
    let size = queries.len();
    let mut mon = MemMonitor::spawn("./mem_k_hop", 5);
    ::std::thread::sleep(Duration::from_millis(5));
    info!("K-hop started ...");
    let start = Instant::now();
    let graphs_copy = graphs.clone();
    let conf = Configuration::Process(workers);
    let guard = timely::execute(conf, move |worker| {
        let index = worker.index();
        for _ in 0..size {
            let src = if index == 0 {
                vec![queries.pop().unwrap().src]
            } else {
                vec![]
            };

            worker.dataflow::<usize, _, _>(|scope| {
                let (helper, cycle) = scope.feedback(1);
                let cycle = src.into_iter()
                    .map(|id| Vertex::new(id))
                    .to_stream(scope)
                    //.exchange(|v| v.id as u64)
                    .concat(&cycle);

                let graph = graphs_copy[index].clone();

                let (left, right) =
                    cycle.exchange(|v| v.id as u64)
                        .flat_map(move |v| {
                            graph.get_neighbors(&v.id)
                                .unwrap_or(NeighborIter::empty())
                        })
                        .branch_when(move |t| *t < (k - 1) as usize);
                right.connect_loop(helper);
                left.count()
                    .inspect(move |count| debug!("worker[{}] get {} neighbors;", index, count))
                    .exchange(|_| 0)
                    .accumulate(0, |sum, data| {
                        for part in data.iter() {
                            *sum += *part;
                        }
                    })
                    .inspect(move |sum| info!("worker[{}] get {} neighbors after aggregate;", index, sum));
            });
        }
    }).unwrap();

    guard.join();
    println!("{} K-hop finished, cost {:?}", size, start.elapsed());
    graphs.clear();
    mon.shutdown();
}
