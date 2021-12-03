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
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate enum_dispatch;
#[macro_use]
extern crate pegasus_common;

use std::cell::Cell;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, RwLock};

mod config;
mod graph;
pub mod tag;
#[macro_use]
mod worker_id;
mod channel_id;
#[macro_use]
pub mod errors;
pub mod api;
pub(crate) mod data;
#[macro_use]
pub mod macros;
pub mod communication;
mod data_plane;
pub mod dataflow;
mod event;
mod operator;
pub(crate) mod progress;
pub mod resource;
pub mod result;
mod schedule;
pub mod stream;
pub mod utils;
mod worker;

use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;

pub use config::{read_from, Configuration, JobConf, ServerConf};
pub use data::Data;
pub use pegasus_common::codec;
pub use pegasus_memory::alloc::check_current_task_memory;
pub use pegasus_network::ServerDetect;
pub use tag::Tag;
pub use worker::Worker;
pub use worker_id::{get_current_worker, WorkerId};

use crate::api::Source;
pub use crate::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
use crate::resource::PartitionedResource;
use crate::result::{ResultSink, ResultStream};
use crate::worker_id::WorkerIdIter;

lazy_static! {
    static ref SERVER_ID: Mutex<Option<u64>> = Mutex::new(None);
    static ref SERVERS: RwLock<Vec<u64>> = RwLock::new(vec![]);
}

thread_local! {
    static LOCAL_SERVER_ID : Cell<Option<u64>> = Cell::new(None);
}

/// get the id of current server among clusters;
#[inline]
pub fn server_id() -> Option<u64> {
    LOCAL_SERVER_ID.with(|id| {
        if let Some(id) = id.get() {
            Some(id)
        } else {
            let server_id = SERVER_ID.lock().expect("lock poisoned");
            if let Some(g_id) = server_id.as_ref() {
                id.set(Some(*g_id));
                Some(*g_id)
            } else {
                None
            }
        }
    })
}

pub fn get_servers() -> Vec<u64> {
    let lock = SERVERS
        .read()
        .expect("fetch read lock failure;");
    lock.to_vec()
}

pub fn get_servers_len() -> usize {
    let lock = SERVERS
        .read()
        .expect("fetch read lock failure;");
    lock.len()
}

fn set_server_id(server_id: u64) -> Option<u64> {
    let mut id = SERVER_ID.lock().expect("lock poisoned");
    if let Some(id) = &*id {
        Some(*id)
    } else {
        id.replace(server_id);
        None
    }
}

pub fn wait_servers_ready(server_conf: &ServerConf) {
    if let Some(local) = server_id() {
        let remotes = match server_conf {
            ServerConf::Local => vec![],
            ServerConf::Partial(s) => s.clone(),
            ServerConf::All => get_servers(),
        };
        if !remotes.is_empty() {
            while !pegasus_network::check_ipc_ready(local, &remotes) {
                std::thread::sleep(std::time::Duration::from_millis(100));
                info!("waiting remote servers connect ...");
            }
        }
    }
}

pub fn startup(conf: Configuration) -> Result<(), StartupError> {
    if let Some(pool_size) = conf.max_pool_size {
        pegasus_executor::set_core_pool_size(pool_size as usize);
    }
    pegasus_executor::try_start_executor_async();

    let mut servers = HashSet::new();
    let server_id = conf.server_id();
    servers.insert(server_id);
    if let Some(id) = set_server_id(server_id) {
        return Err(StartupError::AlreadyStarted(id));
    }
    if let Some(net_conf) = conf.network_config() {
        if let Some(peers) = net_conf.get_servers()? {
            let addr = net_conf.local_addr()?;
            let conn_conf = net_conf.get_connection_param();
            for p in peers.iter() {
                servers.insert(p.id);
            }
            let addr = pegasus_network::start_up(server_id, conn_conf, addr, peers)?;
            info!("server {} start on {:?}", server_id, addr);
        } else {
            return Err(StartupError::CannotFindServers);
        }
    }
    let mut lock = SERVERS
        .write()
        .expect("fetch servers lock failure;");
    assert!(lock.is_empty());
    for s in servers {
        lock.push(s);
    }
    lock.sort();
    Ok(())
}

pub fn startup_with<D: ServerDetect + 'static>(
    conf: Configuration, detect: D,
) -> Result<Option<SocketAddr>, StartupError> {
    if let Some(pool_size) = conf.max_pool_size {
        pegasus_executor::set_core_pool_size(pool_size as usize);
    }
    pegasus_executor::try_start_executor_async();

    let server_id = conf.server_id();
    if let Some(id) = set_server_id(server_id) {
        return Err(StartupError::AlreadyStarted(id));
    }

    Ok(if let Some(net_conf) = conf.network_config() {
        let addr = net_conf.local_addr()?;
        let conn_conf = net_conf.get_connection_param();
        let addr = pegasus_network::start_up(server_id, conn_conf, addr, detect)?;
        info!("server {} start on {:?}", server_id, addr);
        Some(addr)
    } else {
        None
    })
}

pub fn shutdown_all() {
    pegasus_executor::try_shutdown();
    if let Some(server_id) = server_id() {
        pegasus_network::shutdown(server_id);
        pegasus_network::await_termination(server_id);
    }
    pegasus_executor::await_termination();
}

pub fn run<DI, DO, F, FN>(conf: JobConf, func: F) -> Result<ResultStream<DO>, JobSubmitError>
where
    DI: Data,
    DO: Debug + Send + 'static,
    F: Fn() -> FN,
    FN: FnOnce(&mut Source<DI>, ResultSink<DO>) -> Result<(), BuildJobError> + 'static,
{
    let (tx, rx) = crossbeam_channel::unbounded();
    let sink = ResultSink::new(tx);
    let cancel_hook = sink.get_cancel_hook().clone();
    let results = ResultStream::new(conf.job_id, cancel_hook, rx);
    run_opt(conf, sink, |worker| worker.dataflow(func()))?;
    Ok(results)
}

pub fn run_with_resources<DI, DO, F, FN, R>(
    conf: JobConf, mut resource: PartitionedResource<R>, func: F,
) -> Result<ResultStream<DO>, JobSubmitError>
where
    DI: Data,
    DO: Debug + Send + 'static,
    R: Send + Sync + 'static,
    F: Fn() -> FN,
    FN: FnOnce(&mut Source<DI>, ResultSink<DO>) -> Result<(), BuildJobError> + 'static,
{
    let (tx, rx) = crossbeam_channel::unbounded();
    let sink = ResultSink::new(tx);
    let cancel_hook = sink.get_cancel_hook().clone();
    let results = ResultStream::new(conf.job_id, cancel_hook, rx);
    run_opt(conf, sink, |worker| {
        let index = worker.id.index as usize;
        if let Some(r) = resource.take_partition_of(index) {
            worker.add_resource(r);
        }
        worker.dataflow(func())
    })?;
    Ok(results)
}

pub fn run_opt<DI, DO, F>(conf: JobConf, sink: ResultSink<DO>, mut logic: F) -> Result<(), JobSubmitError>
where
    DI: Data,
    DO: Debug + Send + 'static,
    F: FnMut(&mut Worker<DI, DO>) -> Result<(), BuildJobError>,
{
    init_env();
    let peer_guard = Arc::new(AtomicUsize::new(0));
    let conf = Arc::new(conf);
    let workers = allocate_local_worker(&conf)?;
    if workers.is_none() {
        return Ok(());
    }
    let worker_ids = workers.unwrap();
    let mut workers = Vec::new();
    for id in worker_ids {
        let mut worker = Worker::new(&conf, id, &peer_guard, sink.clone());
        let _g = crate::worker_id::guard(worker.id);
        logic(&mut worker)?;
        workers.push(worker);
    }

    if workers.is_empty() {
        return Ok(());
    }

    info!("spawn job_{}({}) with {} workers;", conf.job_name, conf.job_id, workers.len());
    match pegasus_executor::spawn_batch(workers) {
        Ok(_) => Ok(()),
        Err(e) => {
            if pegasus_executor::is_shutdown() {
                Err(SpawnJobError("Executor has shutdown;".into()))?
            } else {
                Err(SpawnJobError(format!("{}", e)))?
            }
        }
    }
}

#[inline]
fn allocate_local_worker(conf: &Arc<JobConf>) -> Result<Option<WorkerIdIter>, BuildJobError> {
    let server_conf = conf.servers();
    let servers = match server_conf {
        ServerConf::Local => {
            return Ok(Some(WorkerIdIter::new(conf.job_id, conf.workers, 0, 0, 1)));
        }
        ServerConf::Partial(ids) => ids.clone(),
        ServerConf::All => get_servers(),
    };

    if servers.is_empty() || (servers.len() == 1) {
        Ok(Some(WorkerIdIter::new(conf.job_id, conf.workers, 0, 0, 1)))
    } else {
        if let Some(my_id) = server_id() {
            let mut my_index = -1;
            for (index, id) in servers.iter().enumerate() {
                if *id == my_id {
                    my_index = index as i64;
                }
            }
            if my_index < 0 {
                warn!("current server {} not among job {};", my_id, conf.job_id);
                Ok(None)
            } else {
                let server_index = my_index as u32;
                if pegasus_network::check_ipc_ready(my_id, &servers) {
                    Ok(Some(WorkerIdIter::new(
                        conf.job_id,
                        conf.workers,
                        my_id as u32,
                        server_index,
                        servers.len() as u32,
                    )))
                } else {
                    return BuildJobError::server_err(format!("servers {:?} are not connected;", servers));
                }
            }
        } else {
            return BuildJobError::server_err(format!("current server not start yet;"));
        }
    }
}

use std::sync::Once;
lazy_static! {
    static ref SINGLETON_INIT: Once = Once::new();
}

fn init_env() {
    if pegasus_executor::is_shutdown() {
        pegasus_common::logs::init_log();
        pegasus_executor::try_start_executor_async();
    }
}
