extern crate chrono;

use std::path::Path;
use std::sync::Arc;

use chrono::offset::{TimeZone, Utc};
use chrono::DateTime;
use graph_index::GraphIndex;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use pegasus::configure_with_default;

lazy_static! {
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref TRIM_PATH: String = configure_with_default!(String, "TRIM_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref GRAPH_INDEX: GraphIndex = _init_graph_index();
}

fn _init_csr() -> CsrDB<usize, usize> {
    println!("Start load graph");
    if TRIM_PATH.is_empty() {
        CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID, None).unwrap()
    } else {
        CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID, Some(TRIM_PATH.clone())).unwrap()
    }
}

fn _init_graph_index() -> GraphIndex {
    GraphIndex::new(*PARTITION_ID)
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
