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
extern crate bitflags;
#[macro_use]
extern crate enum_dispatch;
#[macro_use]
extern crate pegasus_common;

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod config;
mod graph;
pub mod preclude;
#[macro_use]
mod tag;
#[macro_use]
mod worker_id;
mod channel_id;
#[macro_use]
pub mod errors;
#[macro_use]
pub mod api;
pub mod communication;
mod data;
mod data_plane;
pub mod dataflow;
mod event;
mod operator;
mod schedule;
pub mod stream;
mod worker;

pub use crate::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
pub use crate::operator::{never_clone, NeverClone};
use crate::worker_id::WorkerIdIter;
pub use config::{read_from, Configuration, JobConf, ServerConf};
pub use data::Data;
pub use pegasus_common::codec;
use pegasus_executor::{ExecError, TaskGuard};
pub use pegasus_memory::alloc::check_current_task_memory;
pub use pegasus_network::ServerDetect;
use std::collections::HashSet;
pub use tag::Tag;
pub use worker::Worker;
pub use worker_id::{get_current_worker, WorkerId};
use std::net::SocketAddr;

lazy_static! {
    static ref SERVER_ID: Mutex<Option<u64>> = Mutex::new(None);
    static ref SERVERS: RwLock<Vec<u64>> = RwLock::new(vec![]);
}

thread_local! {
    static LOCAL_SERVER_ID : Cell<Option<u64>> = Cell::new(None);
    static WOKER_POOL      : RefCell<Vec<Worker>> = RefCell::new(Vec::new());
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
    let lock = SERVERS.read().expect("fetch read lock failure;");
    lock.to_vec()
}

pub fn get_servers_len() -> usize {
    let lock = SERVERS.read().expect("fetch read lock failure;");
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

pub fn startup(conf: Configuration) -> Result<(), StartupError> {
    let mut servers = HashSet::new();
    let server_id = conf.server_id();
    servers.insert(server_id);
    if let Some(id) = set_server_id(server_id) {
        return Err(StartupError::AlreadyStarted(id));
    }

    if let Some(net_conf) = conf.network_config() {
        if let Some(peers) = net_conf.get_peers()? {
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
    if let Some(pool_size) = conf.max_pool_size {
        pegasus_executor::set_core_pool_size(pool_size as usize);
    }
    pegasus_executor::try_start_executor_async();
    let mut lock = SERVERS.write().expect("fetch servers lock failure;");
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
    let server_id = conf.server_id();
    if let Some(id) = set_server_id(server_id) {
        return Err(StartupError::AlreadyStarted(id));
    }
    let res = if let Some(net_conf) = conf.network_config() {
        let addr = net_conf.local_addr()?;
        let conn_conf = net_conf.get_connection_param();
        let addr = pegasus_network::start_up(server_id, conn_conf, addr, detect)?;
        info!("server {} start on {:?}", server_id, addr);
        Some(addr)
    } else {
        None
    };

    if let Some(pool_size) = conf.max_pool_size {
        pegasus_executor::set_core_pool_size(pool_size as usize);
    }
    pegasus_executor::try_start_executor_async();
    Ok(res)
}

pub fn shutdown_all() {
    pegasus_executor::try_shutdown();
    if let Some(server_id) = server_id() {
        pegasus_network::shutdown(server_id);
        pegasus_network::await_termination(server_id);
    }
    pegasus_executor::await_termination();
}

pub fn run<F>(conf: JobConf, logic: F) -> Result<Option<JobGuard>, JobSubmitError>
where
    F: Fn(&mut Worker) -> Result<(), BuildJobError>,
{
    let cancel_hook = Arc::new(AtomicBool::new(false));
    let peer_guard = Arc::new(AtomicUsize::new(0));
    let conf = Arc::new(conf);

    let workers = allocate_worker(&conf)?;
    if workers.is_none() {
        return Ok(None);
    }
    let worker_ids = workers.unwrap();
    let mut workers = WOKER_POOL.with(|pool| pool.replace(vec![]));
    for id in worker_ids {
        let mut worker = Worker::new(&conf, id, &peer_guard, &cancel_hook);
        logic(&mut worker)?;
        workers.push(worker);
    }

    if workers.is_empty() {
        WOKER_POOL.with(|pool| pool.replace(workers));
        return Ok(None);
    }

    let result = match pegasus_executor::spawn_batch(&mut workers.drain(..)) {
        Ok(guards) => Ok(Some(JobGuard::new(conf.job_id, guards, &cancel_hook))),
        Err(e) => {
            if pegasus_executor::is_shutdown() {
                Err(SpawnJobError("Executor has shutdown;".into()))?
            } else {
                Err(SpawnJobError(format!("{}", e)))?
            }
        }
    };
    workers.clear();
    WOKER_POOL.with(|pool| pool.replace(workers));
    result
}

#[inline]
fn allocate_worker(conf: &Arc<JobConf>) -> Result<Option<WorkerIdIter>, BuildJobError> {
    if let Some(my_id) = server_id() {
        let server_conf = conf.servers();
        let servers = match server_conf {
            ServerConf::Local => {
                return Ok(Some(WorkerIdIter::new(conf.job_id, conf.workers, 0, conf.workers)));
            }
            ServerConf::Partial(ids) => ids.clone(),
            ServerConf::All => get_servers(),
        };
        if servers.is_empty() || (servers.len() == 1 && servers[0] == my_id) {
            Ok(Some(WorkerIdIter::new(conf.job_id, conf.workers, 0, conf.workers)))
        } else {
            let mut my_index = -1;
            for (index, id) in servers.iter().enumerate() {
                if *id == my_id {
                    my_index = index as i64;
                }
            }
            if my_index < 0 {
                Ok(None)
            } else {
                if pegasus_network::check_connect(my_id, &servers) {
                    let peers = conf.workers * servers.len() as u32;
                    let start = my_index as u32 * conf.workers;
                    Ok(Some(WorkerIdIter::new(conf.job_id, peers, start, start + conf.workers)))
                } else {
                    return BuildJobError::server_err(format!(
                        "servers {:?} are not connected;",
                        servers
                    ));
                }
            }
        }
    } else {
        return BuildJobError::server_err(format!("current server not start yet;"));
    }
}

pub struct JobGuard {
    pub job_id: u64,
    task_guards: Vec<TaskGuard>,
    cancel_hook: Arc<AtomicBool>,
}

impl JobGuard {
    fn new(job_id: u64, guards: Vec<TaskGuard>, cancel: &Arc<AtomicBool>) -> Self {
        JobGuard { job_id, task_guards: guards, cancel_hook: cancel.clone() }
    }

    pub fn join(&mut self) -> Result<(), ExecError> {
        while let Some(mut task) = self.task_guards.pop() {
            if let Err(err) = task.join() {
                error!("job {} executed failure, caused by {};", self.job_id, err);
                self.cancel_execute();
                return Err(err);
            }
        }
        Ok(())
    }

    pub fn cancel_execute(&mut self) {
        self.cancel_hook.store(true, Ordering::SeqCst);
        let task_guards = std::mem::replace(&mut self.task_guards, vec![]);
        for mut task in task_guards {
            task.cancel();
        }
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        self.join().expect(&format!("job[{}] executed failure;", self.job_id));
    }
}
