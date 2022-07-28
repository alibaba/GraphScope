//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Li&censed under the Apache License, Version 2.0 (the "License");
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

// These apis imported for your reference
use pegasus::api::{Sink, Map, HasKey};
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
    data_path: PathBuf
}

fn main() {
    let config: Config = Config::from_args();
    let graph = Arc::new(pegasus_graph::load(&config.data_path).unwrap());
    let mut neighbors_list = Vec::new();
    let mut outer_neighbors = graph.get_neighbors(1);
    for outer in outer_neighbors {
        let mut inner_neighbors = graph.get_neighbors(outer);
        for inner in inner_neighbors {  
            neighbors_list.push(inner);
        }
        println!("find node {} for neighbors {:?}", outer, neighbors_list);
        neighbors_list.clear();
    }
}