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

#[warn(unused_parens)]
use std::convert::From;
use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::time::Duration;
use std::panic::{self, AssertUnwindSafe};

use crate::operator::OperatorWrapper;
use crate::channel::IOError;
use crate::worker::{Worker, WorkerId};
use crossbeam_queue::{SegQueue, ArrayQueue};
use crossbeam_channel::{Receiver, Sender, RecvTimeoutError, TryRecvError};
use std::fmt::Debug;

#[derive(Debug)]
pub enum ExecError {
    IOError(IOError),
    Unknown(String),
    JobAlreadyExist(WorkerId),
    RuntimeStopped,
    ExecutorShutdown,
    Reject(GenericTask)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskState {
    Finished,
    Ready,
    NotReady
}

pub trait Task: Send + Debug + 'static {

    fn execute(&mut self) -> Result<TaskState, ExecError>;

    fn is_finished(&self) -> bool;

    fn check_ready(&mut self) -> Result<bool, ExecError> {
        Ok(false)
    }
}

#[derive(Debug)]
pub enum GenericTask {
    Worker(Worker),
    Operator(OperatorWrapper)
}

impl Task for GenericTask {
    #[inline]
    fn execute(&mut self) -> Result<TaskState, ExecError> {
        panic::set_hook(Box::new(|info| {
            println!("task execute panic: {}", info);
        }));

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            match self {
                &mut GenericTask::Worker(ref mut worker) => worker.execute(),
                &mut GenericTask::Operator(ref mut op) => op.execute()
            }
        }));

        match result {
            Ok(result) => result,
            Err(_error) => {
                let err_msg = format!("Execute GenericTask[{:?}] panic, Look into stdout/stderr for detail.", self);
                error!("{}", err_msg);
                Err(ExecError::Unknown(err_msg))
            }
        }
    }

    #[inline]
    fn is_finished(&self) -> bool {
        match self {
            &GenericTask::Worker(ref worker) => worker.is_finished(),
            &GenericTask::Operator(ref op) => op.is_finished()
        }
    }

    #[inline]
    fn check_ready(&mut self) -> Result<bool, ExecError> {

        panic::set_hook(Box::new(|info| {
            println!("check_ready panic: {}", info);
        }));

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            match self {
                &mut GenericTask::Worker(ref mut worker) => worker.check_ready(),
                &mut GenericTask::Operator(_) => Ok(false)
            }
        }));

        match result {
            Ok(result) => result,
            Err(_error) => {
                let err_msg = format!("Check GenericTask[{:?}] ready panic. Look into stdout/stderr for detail.", self);
                error!("{}", err_msg);
                Err(ExecError::Unknown(err_msg))
            }
        }
    }
}

impl Task for Worker {
    fn execute(&mut self) -> Result<TaskState, ExecError> {
        if self.run()? {
            Ok(TaskState::Finished)
        } else {
            if self.has_inner_active() {
                Ok(TaskState::Ready)
            } else {
                Ok(TaskState::NotReady)
            }
        }
    }

    #[inline]
    fn is_finished(&self) -> bool {
        self.is_finish()
    }

    #[inline]
    fn check_ready(&mut self) -> Result<bool, ExecError> {
        if self.check_active()? {
            Ok(true)
        } else if self.checkout_timeout() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Task for OperatorWrapper {
    fn execute(&mut self) -> Result<TaskState, ExecError> {
        self.fire()?;
        Ok(TaskState::Finished)
    }

    fn is_finished(&self) -> bool {
        self.is_finish()
    }
}

pub struct RejectError<T>(pub T);

impl From<IOError> for ExecError {
    fn from(err: IOError) -> Self {
        ExecError::IOError(err)
    }
}

impl From<String> for ExecError {
    fn from(msg: String) -> Self {
        ExecError::Unknown(msg)
    }
}

pub struct TaskResult<T: Task> {
    task: T,
    result: Result<(), ExecError>
}

impl<T: Task> TaskResult<T> {
    pub fn new(task: T, result: Result<(), ExecError>) -> Self {
        TaskResult {
            task,
            result
        }
    }

    pub fn unwrap(self) -> Result<T, ExecError> {
        match self.result {
            Ok(_) => Ok(self.task),
            Err(err) => Err(err)
        }
    }
}

pub struct TaskGuard<T: Task> {
    recv: Receiver<TaskResult<T>>,
    joined: bool,
}

impl<T: Task> TaskGuard<T> {
    pub fn new(recv: Receiver<TaskResult<T>>) -> Self {
        TaskGuard {
            recv,
            joined: false
        }
    }

    pub fn join_forked(self) -> TaskResult<T> {
        match self.recv.recv() {
            Ok(task) => task,
            Err(_e) => {
                panic!("Thread pool had dropped;");
            }
        }
    }

    pub fn try_join_forked(&mut self) -> Option<TaskResult<T>> {
        if !self.joined {
            match self.recv.try_recv() {
                Ok(result) => {
                    self.joined = true;
                    Some(result)
                },
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    panic!("Thread pool had dropped;")
                }
            }
        } else {
            None
        }
    }

    #[inline]
    pub fn is_joined(&self) -> bool {
        self.joined
    }
}

pub trait Executor<T: Task>: Send + Sync + 'static {

    fn spawn(&self, task: T) -> Result<(), ExecError>;

    fn fork(&self, task: T) -> Result<TaskGuard<T>, RejectError<T>>;

    fn start_up(&self);

    fn shutdown(&self);

    fn await_termination(&self);
}

pub struct Direct {
    handles: SegQueue<JoinHandle<()>>
}

impl Direct {
    pub fn new() -> Self {
        Direct {
            handles: SegQueue::new()
        }
    }
}

impl<T: Task> Executor<T> for Direct {
    fn spawn(&self, mut task: T) -> Result<(), ExecError> {
        let handle = thread::Builder::new()
            .name(format!("Direct-{}", self.handles.len()))
            .spawn(move || {
                while !task.is_finished() {
                    match task.execute() {
                        Err(err) => {
                            error!("Error#Direct: task error, caused by {:?}", err);
                            break
                        },
                        _ => ()
                    }
                }
            }).expect("spawn thread failed.");

        self.handles.push(handle);

        Ok(())
    }

    #[inline]
    fn fork(&self, task: T) -> Result<TaskGuard<T>, RejectError<T>> {
        Err(RejectError(task))
    }

    #[inline]
    fn start_up(&self) {
        info!("Direct executor start; ");
    }

    #[inline]
    fn shutdown(&self) {
        info!("Direct executor shutdown;");
    }

    fn await_termination(&self) {
        while let Ok(h) = self.handles.pop() {
            if let Err(e) = h.join() {
                error!("Error#Direct: await termination: {:?}", e);
            }
        }
    }
}

#[derive(Debug)]
pub enum ForkJoinTask<T: Task> {
    Main(T),
    Sub((T, Sender<TaskResult<T>>)),
}

thread_local! {
    pub static S_R : (Sender<TaskResult<GenericTask>>, Receiver<TaskResult<GenericTask>>)
                     = crossbeam_channel::unbounded();
}

pub struct TaskQueue<T> {
    push: Option<Sender<T>>,
    pull: Receiver<T>,
    size: Arc<AtomicUsize>,
}

unsafe impl<T: Send> Send for TaskQueue<T> {}
unsafe impl<T: Send> Sync for TaskQueue<T> {}

impl<T> TaskQueue<T> {
    pub fn new() -> Self {
        let (s, r) = crossbeam_channel::unbounded();
        TaskQueue {
            push: Some(s),
            pull: r,
            size: Arc::new(AtomicUsize::new(0))
        }
    }

    #[inline]
    pub fn push(&self, task: T) {
        if let Some(ref push) = self.push {
            match push.send(task) {
                Err(_) => {
                    error!("Error#TaskQueue: push queue failure, should be unreachable;")
                },
                Ok(_) => {
                    self.size.fetch_add(1, Ordering::SeqCst);
                }
            }
        } else {
            error!("Error#TaskQueue: task queue has already closed;");
        }
    }

    #[inline]
    pub fn pull(&self) -> Result<T, RecvTimeoutError> {
        let result = self.pull.recv_timeout(Duration::from_millis(16));
        match &result {
            Ok(_) => {
                self.size.fetch_sub(1, Ordering::SeqCst);
            },
            _ => {}
        }
        result
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
}

impl<T> Clone for TaskQueue<T> {
    fn clone(&self) -> Self {
        TaskQueue {
            push: self.push.clone(),
            pull: self.pull.clone(),
            size: self.size.clone(),
        }
    }
}



#[allow(dead_code)]
pub struct ThreadPool<T: Task> {
    size: usize,
    max_concurrent: usize,
    in_running: Arc<AtomicUsize>,
    ready_queue: TaskQueue<ForkJoinTask<T>>,
    not_ready_queue: Sender<T>,
    pending_queue: Sender<T>,
    threads: ArrayQueue<JoinHandle<()>>,
    shutdown_signal: Arc<AtomicBool>,
    dispatch_guard: Arc<(JoinHandle<()>, JoinHandle<()>)>,
}

impl<T: Task> ThreadPool<T> {
    pub fn new(size : usize) -> Self {
        let max_concurrent = size * 3;
        let (tx, rx) = crossbeam_channel::unbounded::<T>();
        let (not_ready_tx, not_ready_rx) = crossbeam_channel::unbounded::<T>();

        let in_running = Arc::new(AtomicUsize::default());
        let in_running_arc = in_running.clone();
        let task_queue = TaskQueue::new();
        let task_queue_cp = task_queue.clone();

        let readies = Arc::new(SegQueue::<T>::new());
        let readies_cp = readies.clone();
        let not_ready_tx_cp = not_ready_tx.clone();
        let tracker = ::std::thread::Builder::new().name("ready_tracker".into())
            .spawn(move || {
                // continuously track not ready tasks;
                // if there are some task keey not ready for a long time, may cause a busy loop;
                while let Ok(mut not_ready) = not_ready_rx.recv() {
                    match not_ready.check_ready() {
                        Ok(true) => {
                            readies.push(not_ready);
                        },
                        Ok(false) => {
                            not_ready_tx_cp.send(not_ready)
                                .map_err(|_| {
                                    error!("return not ready task back failure;");
                                }).ok();
                        },
                        Err(e) => {
                            error!("check task ready failure: {:?}", e);
                        }
                    }
                }
            }).unwrap();

        let dispatch = ::std::thread::Builder::new().name("pool-dispatch".into())
            .spawn(move || {
                let mut is_shutdown = false;
                let mut ready = None;
                let mut count = 0;
                loop {
                    if ready.is_none() {
                        // try to figure out next ready task;
                        if let Ok(r) = readies_cp.pop() {
                            debug!("spawn re-ready task;");
                            ready.replace(r);
                        } else if !is_shutdown {
                            if in_running.load(Ordering::SeqCst) == 0 {
                                match rx.recv_timeout(Duration::from_millis(8)) {
                                    Ok(r) => {
                                        count += 1;
                                        debug!("spawn {}th new task;", count);
                                        ready.replace(r);
                                    }
                                    Err(RecvTimeoutError::Disconnected) => {
                                        info!("stop to dispatch new task;");
                                        is_shutdown = true;
                                    },
                                    _ => ()
                                }
                            } else {
                                match rx.try_recv() {
                                    Ok(r) => {
                                        count += 1;
                                        debug!("spawn {}th new task;", count);
                                        ready.replace(r);
                                    }
                                    Err(TryRecvError::Disconnected) => {
                                        info!("stop to dispatch new task;");
                                        is_shutdown = true;
                                    },
                                    _ => ()
                                }
                            }
                        }
                    }
                    if ready.is_some() {
                        let mut running = in_running.load(Ordering::SeqCst);
                        while running >= max_concurrent {
                            info!("waiting idle slots...");
                            ::std::thread::park();
                            running = in_running.load(Ordering::SeqCst);
                        }
                        let try_swap = match in_running.compare_exchange(running, running + 1, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(x) => x,
                            Err(x) => x,
                        };
                        if try_swap == running {
                            let task = ready.take().unwrap();
                            debug!("task {:?} will be spawn, running {}", task, running);
                            task_queue.push(ForkJoinTask::Main(task));
                        }
                    }
                }
            }).unwrap();

        let dispatch_guard = (dispatch, tracker);
        ThreadPool {
            size,
            max_concurrent,
            in_running: in_running_arc,
            ready_queue: task_queue_cp,
            not_ready_queue: not_ready_tx,
            pending_queue: tx,
            threads: ArrayQueue::new(size),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            dispatch_guard: Arc::new(dispatch_guard),
        }
    }
}

impl Executor<GenericTask> for ThreadPool<GenericTask> {
    #[inline]
        fn spawn(&self, task: GenericTask) -> Result<(), ExecError> {
        if self.shutdown_signal.load(Ordering::SeqCst) {
            Err(ExecError::ExecutorShutdown)
        } else {
            self.pending_queue.send(task)
                .map_err(|err| {
                    ExecError::Reject(err.0)
                })
        }
    }

    fn fork(&self, task: GenericTask) -> Result<TaskGuard<GenericTask>, RejectError<GenericTask>> {
        let mut running = self.in_running.load(Ordering::SeqCst);
        if running >= self.size {
            // no idle pool thread presents to steal forked job immediately;
            Err(RejectError(task))
        } else {
            loop {
                let new_running = match self.in_running.compare_exchange(running, running + 1,  Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(x) => x,
                    Err(x) => x,
                } ;
                if new_running == running {
                    let guard = S_R.with(|(s, r)| {
                        self.ready_queue.push(ForkJoinTask::Sub((task, s.clone())));
                        TaskGuard::new(r.clone())
                    });
                    return Ok(guard);
                } else {
                    if new_running >= self.size {
                        return Err(RejectError(task));
                    } else {
                        running = new_running;
                    }
                }
            }
        }
    }

    fn start_up(&self) {
        for i in 0..self.size {
            let task_queue = self.ready_queue.clone();
            let shutdown_signal = self.shutdown_signal.clone();
            let not_ready = self.not_ready_queue.clone();
            let running = self.in_running.clone();
            let dispatch_guard = self.dispatch_guard.clone();
            let slots = self.max_concurrent;
            let handle = thread::Builder::new().name(format!("pool-{}", i))
                .spawn(move || {
                    'Main: loop {
                        match task_queue.pull() {
                            Ok(task) => {
                                match task {
                                    ForkJoinTask::Main(mut t) => {
                                        match t.execute() {
                                            Ok(TaskState::Finished) => {
                                                let current = running.fetch_sub(1, Ordering::SeqCst);
                                                if current == slots {
                                                    dispatch_guard.0.thread().unpark()
                                                }
                                                debug!("ThreadPool: running {} tasks", current);
                                            },
                                            Ok(state) => {
                                                let mut task_finish = false;
                                                let mut ready_state = state;
                                                while task_queue.len() == 0 {
                                                    debug!("no more task in queue, execute current;");
                                                    match t.execute() {
                                                        Ok(TaskState::Finished) => {
                                                            task_finish = true;
                                                            let current = running.fetch_sub(1, Ordering::SeqCst);
                                                            if current == slots {
                                                                dispatch_guard.0.thread().unpark()
                                                            }
                                                            break
                                                        },
                                                        Ok(state) => ready_state = state,
                                                        Err(err) => {
                                                            task_finish = true;
                                                            let current = running.fetch_sub(1, Ordering::SeqCst);
                                                            if current == slots {
                                                                dispatch_guard.0.thread().unpark()
                                                            }
                                                            error!("Error#ThreadPool: main task execute failure, caused by {:?}", err);
                                                            break
                                                        },
                                                    }
                                                }
                                                if !task_finish {
                                                    if ready_state == TaskState::NotReady {
                                                        debug!("swap out not ready task...");
                                                        let current = running.fetch_sub(1, Ordering::SeqCst);
                                                        if current == slots {
                                                            dispatch_guard.0.thread().unpark();
                                                        }
                                                        not_ready.send(t)
                                                            .map_err(|_|{
                                                                error!("swap out not ready task failure...")
                                                            }).ok();
                                                    } else {
                                                        task_queue.push(ForkJoinTask::Main(t));
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                let current = running.fetch_sub(1, Ordering::SeqCst);
                                                if current == slots {
                                                    dispatch_guard.0.thread().unpark();
                                                }
                                                error!("Error#ThreadPool: main task execute failure, caused by {:?}", err);
                                            }
                                        };
                                    },
                                    ForkJoinTask::Sub((mut t , s)) => {
                                        let result = t.execute().map(|_| ());
                                        s.send(TaskResult::new(t, result)).expect("send task result failure");
                                        let current = running.fetch_sub(1, Ordering::SeqCst);
                                        if current == slots {
                                            dispatch_guard.0.thread().unpark();
                                        }
                                    },
                                }
                            },
                            Err(RecvTimeoutError::Timeout) => {
                                if shutdown_signal.load(Ordering::SeqCst) {
                                    if running.load(Ordering::SeqCst) == 0 {
                                        break 'Main
                                    }
                                }
                            },
                            Err(RecvTimeoutError::Disconnected) => {
                                break 'Main
                            }
                        }
                    }
                    info!("Thread {:?} exit;", thread::current().name());
                }).expect("Error#ThreadPool: Create thread failure");
            self.threads.push(handle).expect("Error#ThreadPool: store thread join handle failure");
        }
    }

    #[inline]
    fn shutdown(&self) {
        info!("ThreadPool begin shutdown...");
        match self.shutdown_signal.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(x) => x,
            Err(x) => x,
        };
//        unsafe {
//            let queue_ptr = &self.spawn_queue as *const TaskQueue<ForkJoinTask<GenericTask>>
//                as *mut TaskQueue<ForkJoinTask<GenericTask>>;
//            let queue = &mut (*queue_ptr);
//            queue.close();
//        }
    }

    fn await_termination(&self) {
        while let Ok(t) = self.threads.pop() {
            t.join().unwrap()
        }
    }
}

