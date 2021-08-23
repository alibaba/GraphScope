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

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use lazy_static::lazy_static;
pub use structopt::StructOpt;

#[macro_export]
macro_rules! configure_with_default {
    ($ty:ty, $name:expr, $value: expr) => {{
        std::env::var($name)
            .map(|s| s.parse::<$ty>().unwrap_or($value))
            .unwrap_or($value)
    }};
}

pub static PROCESS_PEERS: &'static str = "PEGASUS_RUNTIME_PROCESS_PEERS";
pub static PROCESS_INDEX: &'static str = "PEGASUS_RUNTIME_PROCESS_INDEX";
pub static BATCH_SIZE: &'static str = "PEGASUS_BATCH_SIZE";
pub static OUTPUT_CAPACITY: &'static str = "PEGASUS_OUTPUT_CAPACITY";
pub static BATCH_REUSE: &'static str = "PEGASUS_BATCH_REUSE";

lazy_static! {
    pub static ref DEFAULT_BATCH_SIZE: usize = configure_with_default!(usize, BATCH_SIZE, 1024);
    pub static ref RUNTIME_PROCESSE_PEERS: usize = configure_with_default!(usize, PROCESS_PEERS, 1);
    pub static ref RUNTIME_PROCESSE_INDEX: usize = configure_with_default!(usize, PROCESS_INDEX, 0);
    pub static ref DEFAULT_OUTPUT_CAPACITY: usize = configure_with_default!(usize, OUTPUT_CAPACITY, 1024);
    pub static ref ENABLE_BATCH_REUSE: bool = configure_with_default!(bool, BATCH_REUSE, true);
}

pub static TRACE_SERVER_ADDR: &'static str = "PEGASUS_TRACE_SERVER";

#[inline]
pub fn set_cluster_process_peers(p: usize) {
    std::env::set_var(PROCESS_PEERS, p.to_string());
}

#[inline]
pub fn get_cluster_process_peers() -> usize {
    *RUNTIME_PROCESSE_PEERS
}

#[inline]
pub fn set_process_index(index: usize) {
    std::env::set_var(PROCESS_INDEX, index.to_string());
}

#[inline]
pub fn get_process_index() -> usize {
    *RUNTIME_PROCESSE_INDEX
}

#[inline]
pub fn set_trace_server(addr: String) {
    std::env::set_var(TRACE_SERVER_ADDR, addr);
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, StructOpt)]
pub struct JobConf {
    /// Sequence number or unique identifier attached to current job;
    #[structopt(short = "i", long = "id", default_value = "0")]
    pub job_seq: usize,
    /// Number of processes the job will include when running;
    #[structopt(short = "n", long = "processes", default_value = "1")]
    pub processes: usize,
    /// Number of parallel sub-tasks inside a process;
    #[structopt(short = "w", long = "workers", default_value = "1")]
    pub workers: usize,
    /// The most mill seconds this job is allowed to execute;
    #[structopt(short = "t", long = "timeout", default_value = "60000")]
    pub time_limit: u64,
    /// The default size of a mini-batch
    #[structopt(short = "b", long = "batch", default_value = "1024")]
    pub batch_size: usize,
    /// The default capacity for all outputs;
    #[structopt(short = "c", long = "capacity", default_value = "1024")]
    pub default_capacity: usize,
    /// Indicate if tracing is enabled;
    #[structopt(long = "trace")]
    pub trace_enable: bool,
    /// The most memory(in MB) this job can use in one process;
    /// 0 means no limit;
    #[structopt(short = "m", long = "memory", default_value = "0")]
    pub memory_limit: u32,
    /// Indicate if print dataflow graph;
    #[structopt(long)]
    pub report: bool,
    // #[structopt(short, long)]
    // pub servers         : Vec<u64>,
}

#[derive(Clone, Debug, StructOpt)]
pub struct ClusterConf {
    /// Specific how many processes consist of the distribute cluster;
    #[structopt(short = "n", long = "processes", default_value = "1")]
    pub processes: usize,
    /// Specific the unique index of the process will start at local;
    #[structopt(short = "p", default_value = "0")]
    pub process_index: usize,
    /// Specific the path of a file which contains all processes' network addresses;
    #[structopt(parse(from_os_str))]
    pub host_file: Option<PathBuf>,
}

impl Default for JobConf {
    fn default() -> Self {
        JobConf {
            job_seq: 0,
            processes: *RUNTIME_PROCESSE_PEERS,
            workers: 1,
            time_limit: !0,
            batch_size: *DEFAULT_BATCH_SIZE,
            default_capacity: *DEFAULT_OUTPUT_CAPACITY,
            trace_enable: false,
            memory_limit: 0,
            report: false,
        }
    }
}

lazy_static! {
    static ref JOB_SEQ: AtomicUsize = AtomicUsize::new(0);
}

impl JobConf {
    pub fn distribute(workers: usize) -> Self {
        let mut conf = JobConf::default();
        conf.workers = workers;
        conf
    }

    pub fn local(workers: usize) -> Self {
        let mut conf = JobConf::default();
        conf.job_seq = JOB_SEQ.fetch_add(1, Ordering::SeqCst);
        conf.workers = workers;
        conf.processes = 1;
        conf
    }

    pub fn set_time_limit(&mut self, mill_secs: u64) {
        self.time_limit = mill_secs;
    }

    pub fn total_workers(&self) -> usize {
        self.workers * self.processes
    }

    #[inline]
    pub fn is_in_local(&self, source: u32, target: u32) -> bool {
        (source as usize) / self.workers == (target as usize) / self.workers
    }
}
