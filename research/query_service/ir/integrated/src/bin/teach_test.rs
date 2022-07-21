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

use core::time;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::time::Instant;
use std::io::BufRead;


use graph_proxy::adapters::exp_store::read_graph::GRAPH;
use graph_proxy::create_exp_store;
use graph_store::graph_db::GlobalStoreTrait;
// These apis imported for your reference
use pegasus::api::{Sink, Map};
use pegasus::errors::JobSubmitError;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

#[derive(StructOpt, Default)]
pub struct Config {
    // #[structopt(short = "s", long = "servers")]
    // servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    /// the path of the origin graph data
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the file contains start vertices
    #[structopt(long = "source_path", parse(from_os_str))]
    source_path: Option<PathBuf>,
    /// the number of start vertices sampling from graph
    #[structopt(short = "s", default_value = "100")]
    source: u32,
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
    // let server_conf = if let Some(ref servers) = config.servers {
    //     let servers = std::fs::read_to_string(servers).unwrap();
    //     Configuration::parse(&servers).unwrap()
    // } else {
    let server_conf = Configuration::singleton();
    pegasus::startup(server_conf).unwrap();
    let mut conf = JobConf::new("example");
    conf.set_workers(config.workers);

    // if config.servers.is_some() {
    //     conf.reset_servers(ServerConf::All);
    // }

    pegasus::wait_servers_ready(conf.servers());

    create_exp_store();

    let graph = Arc::new(pegasus_graph::load(&config.data_path).unwrap());

    let src = if let Some(ref source_file) = config.source_path {
        get_source(source_file)
    } else {
        graph.sample_vertices(config.source as usize, 0.1)
    };

    let mut result = pegasus::run(conf.clone(), || {
        // let index = pegasus::get_current_worker().index;
        let g = graph.clone();
        // let src = if index == 0 { src.clone() } else { vec![] };
        move |input, output| {
            // let stream = input.input_from(g.vertices().map(|v| *v))?;
            let mut stream = input.input_from(g.sample_vertices(1, 1.0))?;
            stream = stream.flat_map(move |id| {
                Ok(g.get_neighbors(id))
            }
        )?;
            stream.sink_into(output)
        }
    }).expect("Run Job Error");

    // let mut result = _teach_example1(conf).expect("Run Job Error!");

    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }

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
