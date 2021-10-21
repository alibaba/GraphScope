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

use std::time::Instant;
use std::sync::Arc;
use std::io::BufRead;

use structopt::StructOpt;
use log::*;

use pegasus::{try_init_logger, ConfigArgs};
use pegasus::graph::topology::{GraphTopology, NeighborIter, Vertex};
use pegasus::operator::source::IntoStream;
use pegasus::operator::unary::Unary;
use pegasus::communication::{Exchange, Pipeline};
use pegasus::operator::binary::Binary;
use pegasus::channel::output::TaggedOutput;
use pegasus::operator::advanced::aggregate::{Dedup, Count};
use pegasus::operator::advanced::inspect::Inspect;


#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "t", long = "thread", default_value = "0")]
    pub threads: usize,
    #[structopt(short = "b", long = "binary")]
    pub binary: bool,
    #[structopt(short = "f", long = "file")]
    pub file: String,
    /// used to configure distribute environment, default is single machine;
    #[structopt(short = "d", long = "distribute host file", default_value = "")]
    pub distribute: String,
    #[structopt(short = "i", long = "index", default_value = "0")]
    pub index: usize,
    #[structopt(short = "p", long = "process", default_value = "1")]
    pub processes: usize,
    #[structopt(short = "q", long = "queries")]
    pub queries: String,
}

fn main() {
    try_init_logger().ok();
    let config: Config = Config::from_args();
    info!("Config: {:?}", config);
    // run server service;
    let load_start = Instant::now();
    info!("start to load graph ...");
    let graph = if config.binary {
        GraphTopology::load_bin(0, 1, true, config.file)
    } else {
        GraphTopology::load(0, 1, true, ',', config.file)
    };

    let graph = Arc::new(graph);
    info!("finished load graph, cost {:?}", load_start.elapsed());
    let runtime = if config.distribute.is_empty() || config.processes <= 1 {
        ConfigArgs::singleton(config.threads).build()
    } else {
        ConfigArgs::distribute(config.index, config.threads, config.processes, config.distribute).build()
    };

    let reader = ::std::io::BufReader::new(::std::fs::File::open(config.queries).unwrap());
    let mut id = 0;
    let total_workers = 4 * config.processes as u64;
    let start = Instant::now();
    for query in reader.lines() {
        if let Ok(src) = query {
            let src = src.parse::<u64>().unwrap();
            let mut workers = runtime.create_workers(id, 4, config.processes).expect("create worker fail");
            for worker in workers.iter_mut() {
                let index = worker.id.index() as u64;
                let graph = graph.clone();
                worker.dataflow("union_dedup", move |builder| {
                    let n = graph.get_neighbors(&src).unwrap_or(NeighborIter::empty());
                    let src = if src % total_workers == index {
                        info!("start query {} on worker {}", src, index);
                        n
                    } else { NeighborIter::empty() };
                    let source = src.into_stream(builder);
                    let graph = graph.clone();
                    let right = source.unary("out", Exchange::new(|v: &Vertex| v.id), |info| {
                        info.set_expand();
                        move |input, output| {
                            input.for_each_batch(|dataset| {
                                let mut session = output.session(&dataset);
                                for v in dataset.data() {
                                    if let Some(n) = graph.get_neighbors(&v.id) {
                                        session.give_entire_iterator(n)?;
                                    }
                                }
                                Ok(session.has_capacity())
                            })?;
                            Ok(())
                        }
                    });
                    source.binary("union", &right, Pipeline, Pipeline, |info| {
                        info.set_pass();
                        |mut input, output| {
                            input.first_for_each(|dataset| {
                                output.session(&dataset).give_batch(dataset.data())
                            })?;
                            if output.has_capacity() {
                                input.second_for_each(|dataset| {
                                    output.session(&dataset).give_batch(dataset.data())
                                })?;
                            }
                            Ok(())
                        }
                    })
                        .dedup(Exchange::new(|v: &Vertex| v.id), |v| v.id)
                        .count(Pipeline)
                        .unary_state("sum", Exchange::new(|_| 0), |info| {
                            info.set_clip();
                            (
                                |input, _output, sum| {
                                    input.for_each_batch(|dataset| {
                                        let (t, d) = dataset.take();
                                        let sum = sum.entry(t).or_insert(0);
                                        for r in d {
                                            *sum += r;
                                        }
                                        Ok(true)
                                    })?;
                                    Ok(())
                                },
                                |output, sums| {
                                    for (t, s) in sums {
                                        output.session(&t).give(s)?;
                                    }
                                    Ok(())
                                }
                            )
                        })
                        .inspect(move |count| println!("get {} count;", count));
                    Ok(())
                }).expect("build plan failure");
            }
            runtime.run_workers(workers).expect("run workers failure");
            id += 1;
        }
    }

    runtime.shutdown().expect("shutdown failure");
    info!("{} union dedup finished, cost {:?}", id, start.elapsed());
}
