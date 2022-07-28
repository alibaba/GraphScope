//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.


use std::path::PathBuf;
use std::io::BufRead;
use std::sync::Arc;

use graph_proxy::adapters::exp_store::read_graph::GRAPH;
use graph_proxy::create_exp_store;
use graph_store::graph_db::{GlobalStoreTrait};
// These apis imported for your reference
use pegasus::api::{CorrelatedSubTask, Count, Map, Sink};
use pegasus::errors::JobSubmitError;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};

pub use petgraph::Direction;
use structopt::StructOpt;
use rand::Rng;



#[derive(StructOpt, Default)]
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

fn sample_vertices(graph : &GRAPH, size: usize, ratio: f64) -> Vec<u64> {
    if size > 0 {
        let mut rng = rand::thread_rng();
        let mut sample = Vec::with_capacity(size);
        for x in GRAPH.get_all_vertices(None) {
            if rng.gen_bool(ratio) {
                sample.push(x.get_id() as u64);
                if sample.len() == size {
                    return sample;
                }
            }
        }
        sample
    } else {
        vec![]
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

    let graph = Arc::new(GRAPH);
    
    pegasus::startup(server_conf).unwrap();
    let mut conf = JobConf::new("example");
    conf.set_workers(config.partitions);
    conf.batch_capacity = config.batch_width;

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let src = if let Some(ref source_file) = config.source_path {
        get_source(source_file)
    } else {
        sample_vertices(&GRAPH, config.source as usize, 0.1)
    };

    create_exp_store();

    let inner_hop = config.inner_hop;
    let outer_hop = config.outer_hop;
    let degree = config.degree;
    let conf = conf.clone();

    pegasus::wait_servers_ready(conf.servers());

    let result = pegasus::run(conf.clone(), || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 {src.clone()} else {vec![]};
        let graph = graph.clone();
        move |input, output| {
            let mut stream = input.input_from(src)?;
            for _i in 0..outer_hop {
                let graph_clone = graph.clone();
                stream = stream.repartition(|id| Ok(*id));
                if let Some(degree) = degree {
                    stream = stream.flat_map(move |_id| {
                        // Ok(GRAPH
                        Ok(graph_clone
                        .get_out_vertices(_id as usize, None)
                        .filter(move |_v| {
                            let mut rng = rand::thread_rng();
                            rng.gen_bool(degree as f64)
                        })
                        .map(|v| v.get_id() as u64)
                        .into_iter()
                    )
                    })?;
                } else {
                    stream = stream.flat_map(move |id| Ok(
                        GRAPH
                        .get_out_vertices(id as usize, None)
                        .map(|v| v.get_id() as u64)
                    ))?
                }
            }
            stream
            .repartition(|id| Ok(*id))
            .apply(|sub| {
                let mut stream = sub;
                for _j in 0..inner_hop {
                    // let g = graph.clone();
                    stream = stream.repartition(|id| Ok(*id));
                    if let Some(degree) = degree {
                        stream = stream.flat_map(move |_id| Ok(
                            sample_vertices(&GRAPH, 0, 0.1).into_iter()
                        ))?;
                    } else {
                        stream = stream.flat_map(move |id| Ok(
                            GRAPH.get_adj_vertices(id as usize, None, Direction::Outgoing)
                            .map(|v| v.get_id() as u64)
                        ))?
                    }
                }
                stream.count()
            })?
            .sink_into(output)
        }
    }).expect("Run Job Error!");

    // let mut result = _teach_example1(conf).expect("Run Job Error!");

    // while let Some(Ok(data)) = result.next() {
    //     println!("{:?}", data);
    // }

    pegasus::shutdown_all();
}

fn _teach_example1(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(
                    GRAPH
                        .get_all_vertices(None)
                        .map(|v| (v.get_id() as u64)),
                )?
                .sink_into(output)
        }
    })
}
