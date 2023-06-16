//
//! Copyright 2020 Alibaba Group Holding Limited.
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
extern crate chrono;

use std::path::Path;
use std::sync::Arc;

use chrono::offset::{TimeZone, Utc};
use chrono::DateTime;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use pegasus::configure_with_default;

lazy_static! {
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref subgraph_2_3_2_in: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 3, 2, mcsr::graph::Direction::Incoming);
    pub static ref subgraph_2_3_3_in: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 3, 2, mcsr::graph::Direction::Incoming);
    pub static ref subgraph_7_14_6_in: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(6, 14, 7, mcsr::graph::Direction::Incoming);
    pub static ref subgraph_2_1_7_in: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 1, 2, mcsr::graph::Direction::Incoming);
    pub static ref subgraph_3_1_7_in: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 1, 3, mcsr::graph::Direction::Incoming);
}

fn _init_csr() -> CsrDB<usize, usize> {
    println!("Start load graph");
    CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID).unwrap()
}

pub fn get_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}
