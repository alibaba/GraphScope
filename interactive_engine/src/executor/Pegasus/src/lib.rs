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

#[macro_use]
extern crate log;
#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

use std::fmt::{Debug, Display, Formatter, Error};
use std::any::Any;
use std::sync::Arc;
use std::io::Write;

use log::{SetLoggerError, Level};
use env_logger::fmt::Color;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

pub mod dataflow;
pub mod allocate;
pub mod channel;
pub mod graph;
pub mod schedule;
pub mod serialize;
pub mod network;
pub mod tag;
pub mod common;
pub mod stream;
pub mod operator;
pub mod communication;
pub mod event;
pub mod client;
pub mod execute;
pub mod worker;
pub mod strategy;
pub mod server;
pub mod memory;
pub mod network_connection;

use crate::tag::{Tag, TagMatcher};
use crate::allocate::{ParallelConfig, Runtime, RuntimeEnv, Configuration};
use crate::worker::{Worker, WorkerId};
use crate::execute::{Executor, ExecError};
use crate::network::PostOffice;

#[cfg(test)]
mod tests;

/// ChannelId type.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, Abomonation, Serialize, Deserialize)]
pub struct ChannelId(pub usize);

impl Display for ChannelId {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

pub trait AsAny {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn as_any_ref(&self) -> &dyn Any;
}

impl<T: ?Sized + AsAny> AsAny for Box<T> {
    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        (**self).as_any_mut()
    }

    #[inline]
    fn as_any_ref(&self) -> &dyn Any {
        (**self).as_any_ref()
    }
}

#[derive(Clone)]
pub struct Pegasus {
    env_index: usize,
    runtime: Arc<Runtime>,
    network: Option<Arc<PostOffice>>
}

impl Pegasus {
    pub fn create_workers(&self, task_id: usize, workers: usize, processes: usize) -> Option<Vec<Worker>> {
        if self.env_index < processes {
            let parallel = ParallelConfig::new(processes, workers);
            let mut worker_vec = Vec::new();
            for i in 0..workers {
                let worker_id = self.env_index * workers + i;
                let w = Worker::new(&self.runtime, task_id, parallel, worker_id);
                worker_vec.push(w);
            }
            Some(worker_vec)
        } else {
            None
        }
    }

    pub fn run_workers(&self, workers: Vec<Worker>) -> Result<(), ExecError> {
        self.runtime.spawn(workers)
    }

    pub fn reset_network(&self, connections: Vec<crate::network_connection::Connection>) -> ::std::io::Result<()> {
        if let Some(network) = self.network.as_ref() {
        //    let connections = crate::network::create_sockets(self.env_index, addresses, false)?;
            network.reconnect_all(connections);
        }
        Ok(())
    }

    pub fn reset_single_network(&self, connection: crate::network_connection::Connection) {
        if let Some(network) = self.network.as_ref() {
            network.re_connect(connection);
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        self.runtime.stop();
        self.runtime.await_termination()
    }
}

#[derive(Debug, Clone, StructOpt, Default)]
pub struct ConfigArgs {
    #[structopt(short = "t", long = "threads", default_value = "0")]
    pub threads: usize,
    #[structopt(short = "p", long = "process_id", default_value = "0")]
    pub process_index: usize,
    #[structopt(short = "n", long = "processes", default_value = "1")]
    pub processes: usize,
    #[structopt(short = "h", long = "hostfile", default_value = "")]
    pub hosts: String,
}

impl ConfigArgs {
    pub fn singleton(threads: usize) -> Self {
        ConfigArgs {
            threads,
            process_index: 0,
            processes: 1,
            hosts: "".to_owned()
        }
    }

    pub fn distribute(index: usize, threads: usize, processes: usize, host_file: String) -> Self {
        ConfigArgs {
            threads,
            process_index: index,
            processes,
            hosts: host_file
        }
    }

    pub fn build(&self) -> Pegasus {
        let config = Configuration::from(&self);
        let (runtime, network) = config.build();
        let runtime = Arc::new(runtime);
        runtime.start();
        let env_index = runtime.get_env().index();
        Pegasus { env_index, runtime, network }
    }
}

pub trait Data: Serialize + for<'a> Deserialize<'a> + Clone + Send + Any + Debug + 'static {}

impl<T: Serialize + for<'a> Deserialize<'a> + Clone + Send + Any + Debug + 'static> Data for T {}

/// Run job only in local(current process);
/// The runtime environment will be setup immediately, and will be destroyed after the job finish;
/// Parameter `workers` specific how much data-parallel workers are needed for this job;
/// Parameter `threads` specific how much physical threads will be initialized when runtime setup,
/// the number 0 of `threads` means the runtime will use a `Direct` executor to run workers;
pub fn run_local<F>(workers: usize, threads: usize, logic: F) -> Result<(), ExecError>
    where F: Fn(&mut Worker)
{
    let runtime = ConfigArgs::singleton(threads).build();
    let mut workers = runtime.create_workers(0, workers, 1).unwrap();

    workers.iter_mut().for_each(|worker| {
       logic(worker)
    });

    runtime.run_workers(workers)?;
    Ok(runtime.shutdown()?)
}

pub fn try_init_logger() -> Result<(), SetLoggerError> {
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            let t = time::now();
            let mut level_style = buf.style();
            match record.level() {
                Level::Error => { level_style.set_color(Color::Red).set_bold(true); }
                Level::Warn => { level_style.set_color(Color::Yellow).set_bold(true); }
                Level::Info => { level_style.set_color(Color::Green).set_bold(false); }
                Level::Debug => { level_style.set_color(Color::White); }
                Level::Trace => { level_style.set_color(Color::Blue); }
            };


            writeln!(buf, "{},{:03} {} [{}] [{}:{}] {}",
                     time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                     t.tm_nsec / 1000_000,
                     level_style.value(record.level()),
                     ::std::thread::current().name().unwrap_or("unknown"),
                     record.file().unwrap_or(""),
                     record.line().unwrap_or(0),
                     record.args()
            )
        })
        .try_init()
}

#[cfg(feature = "mem")]
#[global_allocator]
static MEM_STAT: crate::memory::MemoryStat = crate::memory::MemoryStat;
