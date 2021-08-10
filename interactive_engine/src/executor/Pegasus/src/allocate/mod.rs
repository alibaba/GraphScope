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

use std::io::BufRead;
use std::sync::Arc;
use super::*;
use crate::channel::{Push, Pull, IOError};
use crate::execute::{Direct, ThreadPool, TaskGuard, GenericTask, RejectError};

mod thread;
mod process;
mod distribute;

pub use self::process::{Process, ProcessPush, ProcessPull};
pub use self::thread::{Thread, ThreadPull, ThreadPush};
pub use self::distribute::Distribute;
use std::sync::atomic::{AtomicBool, Ordering};


/// Id of (job, worker, channel);
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, Abomonation)]
pub struct AllocateId(pub usize, pub usize, pub usize);

impl AllocateId {
    pub fn get_worker_id(&self) -> WorkerId {
        WorkerId(self.0, self.1)
    }

    pub fn get_channel_id(&self) -> ChannelId {
        ChannelId(self.2)
    }
}

/// Config the data-parallel degree of a job, which is equal to `processes` * `workers`;
#[derive(Copy, Clone, Debug)]
pub struct ParallelConfig {
    /// The number of processes involved in a job;
    pub processes: usize,
    /// The number of workers in each process;
    pub workers: usize,
}

impl ParallelConfig {

    pub fn new(processes: usize, local_workers: usize) -> Self {
        ParallelConfig {
            processes,
            workers: local_workers
        }
    }

    #[inline]
    pub fn total_peers(&self) -> usize {
        self.processes * self.workers
    }

    #[inline]
    pub fn local_peers(&self) -> usize {
        self.workers
    }

    #[inline]
    pub fn contains(&self, worker: usize, runtime: usize) -> bool {
        worker >= runtime * self.workers && worker < (runtime + 1) * self.workers
    }

    #[inline]
    pub fn is_in_local(&self, source: usize, target: usize) -> bool {
        source / self.workers == target / self.workers
    }
}

pub trait RuntimeEnv: 'static {
    /// total number of os's processes in this runtime environment.
    fn peers(&self) -> usize;

    /// index of this process;
    fn index(&self) -> usize;

    /// allocate and generate data channel for a given channel id.
    fn allocate<T: Data>(&self, id: AllocateId, peers: ParallelConfig) -> Option<(Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)>;

    /// send shutdown signal to the runtime environment;
    fn shutdown(&self);

    /// wait the environment to shutdown and release resources;
    fn await_termination(&self);
}

/// Enumerates known implementors of `RuntimeEnv`.
/// Passes trait method calls on to members.
pub enum GenericEnv {
    /// Single process;
    Standalone(Process),
    /// Distribute environment across multi-processes in a cluster;
    Distribute(Distribute)
}

impl RuntimeEnv for GenericEnv {
    fn peers(&self) -> usize {
        match self {
            &GenericEnv::Standalone(_) => 1,
            &GenericEnv::Distribute(ref d) => d.peers()
        }
    }

    fn index(&self) -> usize {
        match self {
            &GenericEnv::Standalone(ref p) => p.index(),
            &GenericEnv::Distribute(ref d) => d.index()
        }
    }

    fn allocate<T: Data>(&self, id: AllocateId, peers: ParallelConfig) -> Option<(Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)> {
        match self {
            &GenericEnv::Standalone(ref p) => p.allocate(id, peers),
            &GenericEnv::Distribute(ref d) => d.allocate(id, peers),
        }
    }

    fn shutdown(&self) {
        match self {
            &GenericEnv::Standalone(ref p) => p.shutdown(),
            &GenericEnv::Distribute(ref d) => d.shutdown(),
        }
    }

    fn await_termination(&self) {
        match self {
            &GenericEnv::Standalone(ref p) => p.await_termination(),
            &GenericEnv::Distribute(ref d) => d.await_termination()
        }
    }
}

pub struct Runtime {
    executor: GenericExecutor,
    env: GenericEnv,
    //running_jobs: Arc<Mutex<HashSet<WorkerId>>>,
    is_stopped: Arc<AtomicBool>
}

impl Runtime {

    pub fn new(executor: GenericExecutor, env: GenericEnv) -> Self {
        Runtime {
            executor,
            env,
            //running_jobs: Arc::new(Mutex::new(HashSet::new())),
            is_stopped: Arc::new(AtomicBool::new(false))
        }
    }

    #[inline]
    pub fn get_env(&self) -> &GenericEnv {
        &self.env
    }

    #[inline]
    pub fn get_executor(&self) -> &GenericExecutor {
        &self.executor
    }

    #[inline]
    pub fn spawn(&self, workers: Vec<Worker>) -> Result<(), ExecError> {
        if self.is_stopped.load(Ordering::Relaxed) {
            Err(ExecError::RuntimeStopped)
        } else {
//            let mut locked = self.running_jobs
//                .lock().expect("Error#Runtime: read lock poison");
            for w in workers {
                trace!("spawn worker {}", w.id);
                self.executor.spawn(GenericTask::Worker(w))?;
//                if locked.insert(w.id) {
//                    trace!("spawn worker {}", w.id);
//                    self.executor.spawn(GenericTask::Worker(w))?;
//                } else {
//                    error!("Error#Runtime: Worker with id {} is already in running; ", w.id);
//                    return Err(ExecError::JobAlreadyExist(w.id));
//                }
            }
            Ok(())
        }
    }

    #[inline]
    pub fn start(&self) {
        self.executor.start_up();
    }

    pub fn stop(&self) {
        let is_stopped = match self.is_stopped.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(x) => x,
            Err(x) => x,
        };
        if !is_stopped {
            self.executor.shutdown();
        }
    }

    pub fn await_termination(&self) -> Result<(), String> {
        if self.is_stopped.load(Ordering::Relaxed) {
            self.executor.await_termination();
            Ok(())
        } else {
            Err("Stop the runtime first;".to_owned())
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop();
        self.await_termination().unwrap()
    }
}

pub enum ThreadModel {
    Direct,
    Pool(usize)
}

impl ThreadModel {
    pub fn build(self) -> GenericExecutor {
        match self {
            ThreadModel::Direct => GenericExecutor::Direct(Direct::new()),
            ThreadModel::Pool(size) => {
                let pool = ThreadPool::new(size);
                GenericExecutor::Pool(pool)
            }
        }
    }
}

pub enum GenericExecutor {
    Direct(Direct),
    Pool(ThreadPool<GenericTask>)
}

impl Executor<GenericTask> for GenericExecutor {

    fn spawn(&self, task: GenericTask) -> Result<(), ExecError> {
        match self {
            &GenericExecutor::Direct(ref direct) => direct.spawn(task),
            &GenericExecutor::Pool(ref pool) => pool.spawn(task)
        }
    }

    fn fork(&self, task: GenericTask) -> Result<TaskGuard<GenericTask>, RejectError<GenericTask>> {
        match self {
            &GenericExecutor::Direct(ref direct) => {
                direct.fork(task)
            }
            &GenericExecutor::Pool(ref pool) => {
                pool.fork(task)
            }
        }
    }

    fn start_up(&self) {
        match self {
            &GenericExecutor::Direct(ref direct) => Executor::<GenericTask>::start_up(direct),
            &GenericExecutor::Pool(ref pool) => pool.start_up(),
        }
    }

    fn shutdown(&self) {
        match self {
            &GenericExecutor::Direct(ref direct) => Executor::<GenericTask>::shutdown(direct),
            &GenericExecutor::Pool(ref pool) => pool.shutdown(),
        }
    }

    fn await_termination(&self) {
        match self {
            &GenericExecutor::Direct(ref direct) => Executor::<GenericTask>::await_termination(direct),
            &GenericExecutor::Pool(ref pool) => pool.await_termination(),
        }
    }
}

/// Possible configurations for the runtime infrastructure.
pub enum Configuration {
    /// Use one process with some threads;
    Process(ThreadModel),
    /// Expect multiple processes.
    Cluster {
        /// Identity of this process
        index: usize,
        /// Addresses of all processes
        processes: usize,
        /// Model of executor;
        model: ThreadModel,

        addresses: Vec<String>,
    }
}

impl Configuration {
    pub fn from(config: &ConfigArgs) -> Configuration {
        debug!("Configuration : {:?}", config);
        let model = if config.threads == 0 {
            ThreadModel::Direct
        } else {
            ThreadModel::Pool(config.threads)
        };

        if config.processes > 1 {
            let mut addresses = Vec::new();
            if !config.hosts.is_empty() {
                let reader = ::std::io::BufReader::new(::std::fs::File::open(config.hosts.clone()).unwrap());
                for x in reader.lines().take(config.processes) {
                    addresses.push(x.expect("unexpected host"));
                }
                if addresses.len() < config.processes {
                    panic!("could only read {} addresses from {}, but -n: {}", addresses.len(), config.hosts, config.processes);
                }

                info!("get hosts : {:?}", addresses);
            }
            let configuration = Configuration::Cluster {
                index: config.process_index,
                processes: config.processes,
                model,
                addresses,
            };
            configuration
        } else {
            Configuration::Process(model)
        }
    }

    pub fn build(self) -> (Runtime, Option<Arc<PostOffice>>) {
        match self {
            Configuration::Process(model) => {
                let env = GenericEnv::Standalone(Process::new());
                (Runtime::new(model.build(), env), None)
            },
            Configuration::Cluster { index, processes, model, addresses} => {
                let distribute = Distribute::new(index, processes);
                if !addresses.is_empty() {
                    let listener = ::std::net::TcpListener::bind(addresses.get(index).unwrap()).expect("bind tcp listener address failed");
                    let mut await_addresses = vec![];
                    let mut start_addresses = vec![];
                    for i in 0..index {
                        start_addresses.push((i, addresses.get(i).unwrap().clone()));
                    }
                    for i in index..addresses.len() {
                        await_addresses.push((i, addresses.get(i).unwrap().clone()))
                    }

                    let tcp_streams = crate::network::reconnect(index, listener, start_addresses, await_addresses, 10).unwrap();

                    let mut connections = vec![];
                    for (index, address, tcp_stream) in tcp_streams.into_iter() {
                        connections.push(crate::network_connection::Connection::new(index, address, tcp_stream ));
                    }

                    if !connections.is_empty() {
                        distribute.setup_network(connections);
                    }
                }
                let network = distribute.get_network();
                let env = GenericEnv::Distribute(distribute);
                let executor = model.build();
                (Runtime::new(executor, env), Some(network))
            },
        }
    }
}

