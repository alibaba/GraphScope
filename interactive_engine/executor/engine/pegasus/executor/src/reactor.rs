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

use std::cell::RefCell;
#[cfg(not(debug_assertions))]
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use crossbeam_queue::SegQueue;
use pegasus_common::queue::*;
use pending::PendingPool;

use super::*;

struct SelectTask {
    pub last: Instant,
    pub(crate) pending: PendingPool<GeneralTask>,
    pub is_mirror: bool,
    pub cost: usize,
}

impl SelectTask {
    fn new(inflows: Vec<Arc<SegQueue<GeneralTask>>>, permits: usize) -> Self {
        let pending = PendingPool::new(inflows, permits);
        SelectTask { last: Instant::now(), pending, is_mirror: false, cost: 0 }
    }

    fn with(last: Instant, pending: PendingPool<GeneralTask>) -> Self {
        SelectTask { last, pending, is_mirror: true, cost: 0 }
    }

    #[inline]
    fn update_last(&mut self) {
        self.last = Instant::now();
    }

    #[inline]
    fn destroy(&self) {
        self.pending.destroy();
    }
}

impl Drop for SelectTask {
    fn drop(&mut self) {
        if *TRACE_SELECT_ENABLE && !self.is_mirror {
            if log_enabled!(log::Level::Trace) {
                trace!("Main select exit, total cost {:?} us", self.cost);
            }
            TRACE_SELECT_COST.fetch_add(self.cost, Ordering::SeqCst);
        }
    }
}

enum RunTask {
    Select(SelectTask),
    Users(GeneralTask),
}

pub struct ExecutorRuntime {
    pub max_core: usize,
    current_core: usize,
    task_rx: Receiver<TaskPackage>,
    re_active_queue: WorkStealFactory<RunTask>,
    threads_guard: Vec<JoinHandle<()>>,
    in_flows: Vec<Arc<SegQueue<GeneralTask>>>,
}

#[inline]
#[cfg(not(debug_assertions))]
fn do_user_task(
    mut task: GeneralTask, not_readies: &Arc<SegQueue<GeneralTask>>, re_active: &WorkStealQueue<RunTask>,
) {
    // 1. execute task;
    // 2. check task state:

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        match task.execute() {
            // if finished or failed, sink result;
            TaskState::Finished => (),
            // otherwise push to not-ready queue;
            TaskState::NotReady => not_readies.push(task),
            TaskState::Ready => re_active.push(RunTask::Users(task)),
        }
    }));
    if result.is_err() {
        error!("Task execute failure;")
    }
}

#[inline]
#[cfg(debug_assertions)]
fn do_user_task(
    mut task: GeneralTask, not_readies: &Arc<SegQueue<GeneralTask>>, re_active: &WorkStealQueue<RunTask>,
) {
    // 1. execute task;
    // 2. check task state:
    match task.execute() {
        // if finished or failed, sink result;
        TaskState::Finished => (),
        // otherwise push to not-ready queue;
        TaskState::NotReady => not_readies.push(task),
        TaskState::Ready => re_active.push(RunTask::Users(task)),
    }
}

pub const PEGASUS_EXECUTOR_TRACE_SELECT: &'static str = "PEGASUS_EXECUTOR_TRACE_SELECT";
lazy_static! {
    pub static ref TRACE_SELECT_ENABLE: bool = ::std::env::var(PEGASUS_EXECUTOR_TRACE_SELECT)
        .map(|s| s.parse::<bool>().unwrap_or(false))
        .unwrap_or(false);
    pub static ref TRACE_SELECT_COST: AtomicUsize = AtomicUsize::new(0);
    pub static ref SELECT_POINTS_PERMITS: AtomicUsize = AtomicUsize::new(0);
}

#[inline(never)]
fn do_select(mut select: SelectTask, re_active: &WorkStealQueue<RunTask>) {
    let time = [None, Some(Instant::now())][*TRACE_SELECT_ENABLE as usize];
    let last = select.last;
    select.pending.fill(|p| {
        trace!("add new select point;");
        re_active.push(RunTask::Select(SelectTask::with(last, p)))
    });

    let size = select.pending.len();
    if size > 0 {
        for task in select.pending.iter_mut() {
            let r = task.as_mut().map(|t| t.check_ready());
            match r {
                Some(TaskState::Ready) => {
                    let task = task.take().unwrap();
                    re_active.push(RunTask::Users(task));
                }
                Some(TaskState::Finished) => {
                    task.take().unwrap();
                }
                _ => {}
            }
        }

        select.pending.retain(|t| t.is_some());
    }

    time.map(|t| {
        let left = select.pending.len();
        let elapsed = t.elapsed();
        trace!("select {} tasks, {} tasks was selected, cost {:?}", size, size - left, elapsed);
        select.cost += elapsed.as_micros() as usize;
    });

    if select.pending.len() > 0 || !select.is_mirror {
        select.update_last();
        re_active.push(RunTask::Select(select));
    } else {
        select.destroy();
        trace!("remove one mirror select point, it total cost {} us", select.cost);
        if *TRACE_SELECT_ENABLE {
            TRACE_SELECT_COST.fetch_add(select.cost, Ordering::SeqCst);
        }
    }
}

lazy_static! {
    static ref SELECT_INTERVAL_MS: usize = ::std::env::var("PEGASUS_SELECT_INTERVAL")
        .map(|item| item.parse::<usize>().unwrap_or(1))
        .unwrap_or(1);
}

fn do_task(
    task: RunTask, not_readies: &Arc<SegQueue<GeneralTask>>, re_active: &WorkStealQueue<RunTask>,
) -> bool {
    // match task:
    match task {
        // 1. if 'select':  select not-ready queue, push all re-ready tasks to re-active queue;
        RunTask::Select(select) => {
            let elapsed = select.last.elapsed().as_millis();
            if select.last.elapsed().as_millis() as usize >= *SELECT_INTERVAL_MS {
                do_select(select, re_active);
                elapsed > 0
            } else {
                re_active.push(RunTask::Select(select));
                false
            }
        }
        // 2. if 'user-task': do_user_task();
        RunTask::Users(task) => {
            do_user_task(task, not_readies, re_active);
            true
        }
    }
}

fn work_loop(
    re_active: &WorkStealQueue<RunTask>, not_readies: &Arc<SegQueue<GeneralTask>>,
    new_task: &Receiver<TaskPackage>,
) {
    let mut is_shutdown = false;
    'main: loop {
        // 1. first pop re-active queue, check if any re-active task can do;
        while let Some(task) = re_active.pop() {
            // 2. (1)=true: do_task()
            if !do_task(task, not_readies, re_active) {
                break;
            }
        }

        if !is_shutdown {
            // 3. (1)=false: waiting new task until timeout;
            match new_task.recv_timeout(Duration::from_micros(500)) {
                // 4. if (3) a new task fetched, do_user_task()
                Ok(task) => match task {
                    TaskPackage::Single(task) => do_user_task(task, not_readies, re_active),
                    TaskPackage::Batch(mut tasks) => {
                        let first = tasks.swap_remove(0);
                        for t in tasks {
                            re_active.push(RunTask::Users(t));
                        }
                        do_user_task(first, not_readies, re_active)
                    }
                },
                Err(RecvTimeoutError::Timeout) => {
                    is_shutdown = SHUTDOWN_HOOK.load(Ordering::SeqCst);
                }
                Err(RecvTimeoutError::Disconnected) => {
                    is_shutdown = true;
                }
            }
        } else if IN_PROGRESS_TASK_COUNT.load(Ordering::SeqCst) == 0 {
            break 'main;
        } else if *SELECT_INTERVAL_MS > 0 {
            std::thread::yield_now();
        }
        // 5. go to (1);
    }
    info!(
        "{} finished all work and exit;",
        ::std::thread::current()
            .name()
            .unwrap_or("reactor 0")
    );
}

impl ExecutorRuntime {
    fn new(core: usize, task_rx: Receiver<TaskPackage>) -> Self {
        assert!(core > 0);
        let mut in_flows = Vec::with_capacity(core);
        for _ in 0..core {
            in_flows.push(Arc::new(SegQueue::new()));
        }
        ExecutorRuntime {
            max_core: core,
            current_core: 1,
            task_rx,
            re_active_queue: WorkStealFactory::new(core),
            threads_guard: Vec::with_capacity(core),
            in_flows,
        }
    }

    fn start(mut self) {
        let queue = self
            .re_active_queue
            .get_queue()
            .expect("should be unreachable: the first queue lost;");
        let select = SelectTask::new(self.in_flows.clone(), self.max_core);
        let not_readies = self
            .in_flows
            .pop()
            .expect("should be unreachable: the first not ready queue lost");
        queue.push(RunTask::Select(select));
        info!("start reactor executor with maximum {} core threads;", self.max_core);

        let mut shutdown = false;
        while self.current_core < self.max_core {
            if shutdown {
                if IN_PROGRESS_TASK_COUNT.load(Ordering::SeqCst) > self.current_core {
                    self.try_fork_new_thread(&queue, &not_readies);
                } else {
                    break;
                }
            } else {
                // if current work threads less than the core size:
                match self
                    .task_rx
                    .recv_timeout(Duration::from_millis(200))
                {
                    // fork new thread to do work;
                    Ok(task) => match task {
                        TaskPackage::Single(task) => {
                            if let Some(task) = self.fork_new_thread(task) {
                                assert_eq!(self.current_core, self.max_core);
                                queue.push(RunTask::Users(task));
                            }
                        }
                        TaskPackage::Batch(mut tasks) => {
                            while let Some(task) = tasks.pop() {
                                if let Some(task) = self.fork_new_thread(task) {
                                    assert_eq!(self.current_core, self.max_core);
                                    queue.push(RunTask::Users(task));
                                    break;
                                }
                            }

                            if !tasks.is_empty() {
                                assert_eq!(self.current_core, self.max_core);
                                for task in tasks {
                                    queue.push(RunTask::Users(task));
                                }
                            }
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        self.try_fork_new_thread(&queue, &not_readies);
                        shutdown = SHUTDOWN_HOOK.load(Ordering::SeqCst);
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // disconnected; no more tasks;
                        shutdown = true;
                    }
                }
            }
        }
        if shutdown && IN_PROGRESS_TASK_COUNT.load(Ordering::SeqCst) == 0 {
            info!("reactor 0 exist;")
        } else {
            work_loop(&queue, &not_readies, &self.task_rx);
        }
    }

    #[inline]
    fn set_core_size(&mut self, core: usize) {
        if core < 1 {
            error!("core pool size can't less than 1; keep default;");
        } else {
            self.max_core = core;
        }
    }

    #[inline]
    fn try_fork_new_thread(
        &mut self, queue: &WorkStealQueue<RunTask>, not_readies: &Arc<SegQueue<GeneralTask>>,
    ) {
        if let Some(task) = queue.pop() {
            match task {
                RunTask::Select(task) => {
                    do_task(RunTask::Select(task), not_readies, &queue);
                }
                RunTask::Users(task) => {
                    if let Some(task) = self.fork_new_thread(task) {
                        do_task(RunTask::Users(task), not_readies, &queue);
                    }
                }
            }
        }
    }

    fn fork_new_thread(&mut self, task: GeneralTask) -> Option<GeneralTask> {
        if let Some(re_active) = self.re_active_queue.get_queue() {
            let in_flow = self.in_flows.pop().expect("unreachable");
            let new_tasks = self.task_rx.clone();
            let id = self.current_core;
            let g = ::std::thread::Builder::new()
                .name(format!("reactor {}", id))
                .spawn(move || {
                    debug!("fork new thread reactor: {}", id);
                    do_user_task(task, &in_flow, &re_active);
                    work_loop(&re_active, &in_flow, &new_tasks)
                })
                .expect("fork new thread failure");
            self.threads_guard.push(g);
            self.current_core += 1;
            None
        } else {
            warn!("fork new thread failure, already forked {} threads;", self.current_core);
            Some(task)
        }
    }
}

enum TaskPackage {
    Single(GeneralTask),
    Batch(Vec<GeneralTask>),
}

pub struct ExecutorProxy {
    task_tx: Sender<TaskPackage>,
}

impl ExecutorProxy {
    fn new(task_tx: Sender<TaskPackage>) -> Self {
        ExecutorProxy { task_tx }
    }
}

impl Executor for ExecutorProxy {
    fn spawn<T: Task + 'static>(&self, task: T) -> Result<(), RejectError<T>> {
        if SHUTDOWN_HOOK.load(Ordering::SeqCst) {
            Err(RejectError(task))
        } else {
            let task = GeneralTask::new(task);
            self.task_tx
                .send(TaskPackage::Single(task))
                .expect("Executor runtime poisoned");
            Ok(())
        }
    }

    fn spawn_batch<T: Task + 'static, I: IntoIterator<Item = T>>(
        &self, tasks: I,
    ) -> Result<(), RejectError<()>> {
        if SHUTDOWN_HOOK.load(Ordering::SeqCst) {
            Err(RejectError(()))
        } else {
            let tasks = tasks.into_iter();
            let mut general_tasks = if let Some(len) = tasks.size_hint().1 {
                Vec::with_capacity(len)
            } else {
                Vec::with_capacity(2)
            };

            for task in tasks {
                let task = GeneralTask::new(task);
                general_tasks.push(task);
            }
            self.task_tx
                .send(TaskPackage::Batch(general_tasks))
                .expect("Executor runtime poisoned");
            Ok(())
        }
    }
}

impl Clone for ExecutorProxy {
    fn clone(&self) -> Self {
        ExecutorProxy { task_tx: self.task_tx.clone() }
    }
}

static CORE_POOL_SIZE: &'static str = "PEGASUS_CORE_POOL_SIZE";

pub fn init_executor() -> (Mutex<Option<ExecutorRuntime>>, ExecutorProxy) {
    let (tx, rx) = crossbeam_channel::unbounded();
    let cpus = num_cpus::get();
    let core = ::std::env::var(CORE_POOL_SIZE)
        .map(|value| value.parse::<usize>().unwrap_or(cpus))
        .unwrap_or(cpus);
    let runtime = Mutex::new(Some(ExecutorRuntime::new(core, rx)));
    let proxy = ExecutorProxy::new(tx);
    (runtime, proxy)
}

struct ExecutorGuard {
    seq: usize,
    guard: Arc<AtomicUsize>,
}

impl ExecutorGuard {
    fn new() -> Self {
        ExecutorGuard { seq: 0, guard: Arc::new(AtomicUsize::new(0)) }
    }

    fn size(&self) -> usize {
        Arc::strong_count(&self.guard)
    }
}

impl Clone for ExecutorGuard {
    fn clone(&self) -> Self {
        let seq = self.guard.fetch_add(1, Ordering::SeqCst) + 1;
        ExecutorGuard { seq, guard: self.guard.clone() }
    }
}

impl Drop for ExecutorGuard {
    fn drop(&mut self) {
        if self.seq > 0 {
            let seq = self.guard.fetch_sub(1, Ordering::SeqCst);
            if seq == 1 {}
        }
    }
}

lazy_static! {
    static ref EXECUTOR: (Mutex<Option<ExecutorRuntime>>, ExecutorProxy) = init_executor();
    static ref THREAD_JOIN: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);
    static ref EXECUTOR_GUARD: ExecutorGuard = ExecutorGuard::new();
}

thread_local! {
    static LOCAL_EXECUTOR_HOOK: RefCell<Option<ExecutorGuard>> = RefCell::new(None);
}

/// Start the [`Executor`] runtime, this function will **block** current thread;
/// The global executor runtime can only be started once, other invoking on this function will fail;
fn start_executor() {
    if SHUTDOWN_HOOK.swap(false, Ordering::SeqCst) {
        let mut lock = EXECUTOR.0.lock().expect("Executor lock poison");
        if let Some(executor) = lock.take() {
            executor.start();
        } else {
            error!("Global executor runtime is already started;");
        }
    }
}

pub fn start_executor_async() {
    let join = std::thread::Builder::new()
        .name("reactor 0".to_owned())
        .spawn(|| start_executor())
        .expect("start executor thread failure");
    THREAD_JOIN
        .lock()
        .expect("THREAD_JOIN lock poison")
        .replace(join);
}

pub fn try_start_executor_async() {
    LOCAL_EXECUTOR_HOOK.with(|x| {
        let mut borrow = x.borrow_mut();
        if borrow.is_none() {
            let guard = EXECUTOR_GUARD.clone();
            if guard.seq == 1 {
                start_executor_async();
            }
            borrow.replace(guard);
        }
    });

    while is_shutdown() {
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[inline]
fn shutdown() {
    info!("Executor going to shutdown ...");
    SHUTDOWN_HOOK.store(true, Ordering::SeqCst);
}

pub fn try_shutdown() {
    LOCAL_EXECUTOR_HOOK.with(|x| x.borrow_mut().take());
    if EXECUTOR_GUARD.size() == 1 {
        shutdown();
    }
}

pub fn await_termination() {
    try_shutdown();
    if EXECUTOR_GUARD.size() == 1 {
        if let Some(join) = THREAD_JOIN
            .lock()
            .expect("THREAD_JOIN lock poison")
            .take()
        {
            info!("waiting executor terminate...");
            join.join().expect("Executor runtime error");
        }
        if *TRACE_SELECT_ENABLE {
            let cost = TRACE_SELECT_COST.load(Ordering::SeqCst);
            info!("do select cost {}/us", cost);
        }
    }
}

pub fn is_shutdown() -> bool {
    SHUTDOWN_HOOK.load(Ordering::SeqCst)
}

/// Set the max thread size the [`Executor`] can used;
/// This function must be invoked before the [`Executor`] start;
/// It is recommended that use the environment variable `export PEGASUS_CORE_POOL_SIZE = n` to
/// set the thread size;
pub fn set_core_pool_size(core: usize) {
    let mut lock = EXECUTOR.0.lock().expect("Executor lock poison");
    if let Some(executor) = lock.as_mut() {
        executor.set_core_size(core);
    } else {
        error!("Global executor runtime is already started;");
    }
}

/// Spawn a new task to the reactor [`Executor`];
/// All tasks will be pushed to an unbound task queue, waiting for being executed, so no task will
/// be rejected unless the executor had shutdown;
/// # Return value:
/// [`TaskGuard`] : A guard for just spawned task, used to fetch the task's result;
/// See [`TaskGuard`] for more;
pub fn spawn<T: Task + 'static>(task: T) -> Result<(), RejectError<T>> {
    EXECUTOR.1.spawn(task)
}

pub fn spawn_batch<T: Task + 'static, I: IntoIterator<Item = T>>(tasks: I) -> Result<(), RejectError<()>> {
    EXECUTOR.1.spawn_batch(tasks)
}
