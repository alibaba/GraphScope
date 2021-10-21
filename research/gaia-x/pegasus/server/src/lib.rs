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

#[macro_use]
extern crate log;

pub use config::{CommonConfig, HostsConfig};
use pegasus::Data;

#[cfg(not(feature = "gcip"))]
mod generated {
    pub mod protocol {
        tonic::include_proto!("protocol");
    }
}

#[cfg(feature = "gcip")]
mod generated {
    #[path = "protocol.rs"]
    pub mod protocol;
}

pub use generated::protocol as pb;

pub trait AnyData: Data + Eq {}

// pub mod client;
pub mod config;
pub mod rpc;
pub mod service;

pub use generated::protocol::{JobRequest, JobResponse};

#[allow(dead_code)]
pub fn report_memory(job_id: u64) -> Option<std::thread::JoinHandle<()>> {
    let g = std::thread::Builder::new()
        .name(format!("memory-reporter {}", job_id))
        .spawn(move || {
            let mut max_usage = 0;
            let mut count_zero_times = 50;
            loop {
                if let Some(usage) = pegasus_memory::alloc::check_task_memory(job_id as usize) {
                    if usage > max_usage {
                        max_usage = usage;
                    }
                } else if max_usage > 0 {
                    break;
                } else if max_usage == 0 {
                    count_zero_times -= 1;
                    if count_zero_times <= 0 {
                        break;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            info!("Job {} memory usage: {:.4} MB;", job_id, max_usage as f64 / 1_000_000.0);
        })
        .unwrap();
    Some(g)
}
