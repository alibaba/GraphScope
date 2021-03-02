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

use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::ThreadId;

mod error;
mod pending;
mod reactor;
pub use error::{ExecError, RejectError, TaskExecError};
pub use reactor::*;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TaskState {
    /// Indicate a task had finished execution;
    Finished,
    /// Indicate the task can't make progress at this point;
    NotReady,
    /// Indicate the task is ready to be executed;
    Ready,
}

/// Task abstraction which can be spawned to the [`Executor`];
/// The [`Task`]'s implementation can customize the execution program, and the [`Executor`] will
/// be responsible for it's executing until finished once it is spawned;
///
/// The [`Task`] implementation should be interruptable and resumable, when it's computation is
/// going to block on some conditions, it will yield the computation resources by
/// return [`TaskState::NotReady`]. The runtime will be aware of that, and resume it's computation
/// once the conditions are ready;
pub trait Task: Send {
    /// User customized computation logic;
    ///
    /// # Return value:
    /// - [`Ok(TaskState::Finished)`] : Indicate that the task has totally finished; Join it's
    /// [`TaskGuard`] can fetch the result immediately;
    /// - [`Ok(TaskState::NotReady)`] : Indicate that the task can't make progress because some conditions
    /// at this point; The [`Executor`] will suspend current execution, try to execute next task, but
    /// keep watching the computation state;
    /// - [`Ok(TaskState::Ready)`] : todo
    /// - [Err(Error)] : Indicate that the task has failed because of [`Error`] occurred; The [`Error`] will
    /// be delivered to the [`TaskGuard`];
    fn execute(&mut self) -> Result<TaskState, Box<dyn TaskExecError>>;

    /// Once the task was interrupted, The [`Executor`] will keep watching the computation state by
    /// invoke this function to check if the computation can be continued or finished;
    ///
    /// # Return value:
    ///
    /// todo;
    fn check_ready(&mut self) -> Result<TaskState, Box<dyn TaskExecError>>;
}

lazy_static! {
    static ref TASK_RESULTS: ShardedLock<HashMap<ThreadId, Sender<(usize, Option<ExecError>)>>> =
        ShardedLock::new(HashMap::new());
    static ref SHUTDOWN_HOOK: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    static ref IN_PROGRESS_TASK_COUNT: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

struct TaskResultSink {
    seq: Cell<usize>,
    rx: Receiver<(usize, Option<ExecError>)>,
    map: RefCell<HashMap<usize, Option<ExecError>>>,
    canceled: RefCell<HashSet<usize>>,
}

impl TaskResultSink {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let mut lock = TASK_RESULTS.write().expect("TASK_RESULTS lock poison;");
        lock.insert(std::thread::current().id(), tx);
        TaskResultSink {
            seq: Cell::new(0),
            rx,
            map: RefCell::new(HashMap::new()),
            canceled: RefCell::new(HashSet::new()),
        }
    }

    pub fn join_task(&self, id: usize) -> Result<(), ExecError> {
        if self.canceled.borrow().contains(&id) {
            return Ok(());
        }

        let result = self.map.borrow_mut().remove(&id);
        match result {
            Some(Some(err)) => Err(err),
            Some(None) => Ok(()),
            None => loop {
                match self.rx.recv() {
                    Ok((id_x, result)) => {
                        if id_x == id {
                            return result.map(|err| Err(err)).unwrap_or(Ok(()));
                        } else if !self.canceled.borrow().contains(&id_x) {
                            self.map.borrow_mut().insert(id_x, result);
                        }
                    }
                    Err(_err) => {
                        return Err(ExecError::executor_error("executor shutdown;".into()))
                    }
                }
            },
        }
    }

    pub fn cancel_task(&self, id: usize) {
        self.map.borrow_mut().remove(&id);
        self.canceled.borrow_mut().insert(id);
    }

    #[inline]
    pub fn next_seq(&self) -> usize {
        let id = self.seq.get();
        self.seq.set(id + 1);
        id
    }
}

thread_local! {
    static TASK_RESULT_SINK: TaskResultSink = TaskResultSink::new();
}

pub struct TaskGuard {
    id: usize,
    is_joined: Arc<AtomicBool>,
}

impl TaskGuard {
    pub fn new(id: usize) -> Self {
        TaskGuard { id, is_joined: Arc::new(AtomicBool::new(false)) }
    }

    pub fn join(&mut self) -> Result<(), ExecError> {
        if !self.is_joined.load(Ordering::SeqCst) {
            self.is_joined.store(true, Ordering::SeqCst);
            TASK_RESULT_SINK.with(|sink| sink.join_task(self.id))
        } else {
            Ok(())
        }
    }

    pub fn cancel(&mut self) {
        TASK_RESULT_SINK.with(|sink| sink.cancel_task(self.id));
    }
}

impl Clone for TaskGuard {
    fn clone(&self) -> Self {
        TaskGuard { id: self.id, is_joined: self.is_joined.clone() }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if !self.is_joined.load(Ordering::SeqCst) {
            self.cancel()
        }
    }
}

pub trait Executor: Send + Sync {
    fn spawn<T: Task + 'static>(&self, task: T) -> Result<TaskGuard, RejectError<T>>;

    fn spawn_batch<T: Task + 'static, I: Iterator<Item = T>>(
        &self, tasks: &mut I,
    ) -> Result<Vec<TaskGuard>, RejectError<()>>;
}

struct GeneralTask {
    inner: Box<dyn Task>,
    thread: ThreadId,
    seq: usize,
}

impl GeneralTask {
    fn new<T: Task + 'static>(task: T) -> Self {
        let seq = TASK_RESULT_SINK.with(|s| s.next_seq());
        IN_PROGRESS_TASK_COUNT.fetch_add(1, Ordering::SeqCst);
        GeneralTask { inner: Box::new(task), thread: ::std::thread::current().id(), seq }
    }

    fn get_thread_id(&self) -> &ThreadId {
        &self.thread
    }

    fn get_seq(&self) -> usize {
        self.seq
    }

    fn get_task_guard(&self) -> TaskGuard {
        TaskGuard::new(self.seq)
    }
}

impl Drop for GeneralTask {
    fn drop(&mut self) {
        IN_PROGRESS_TASK_COUNT.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Task for GeneralTask {
    #[inline(always)]
    fn execute(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
        let state = self.inner.execute()?;
        if let Some(err) = crate::check_error() {
            Err(err)
        } else {
            Ok(state)
        }
    }

    #[inline(always)]
    fn check_ready(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
        let state = self.inner.check_ready()?;
        if let Some(err) = crate::check_error() {
            Err(err)
        } else {
            Ok(state)
        }
    }
}

#[inline]
pub fn sink_task_result(tid: ThreadId, seq: usize, result: Option<ExecError>) {
    let lock = TASK_RESULTS.read().expect("TASK_RESULTS lock poison");
    if let Some(sink) = lock.get(&tid) {
        sink.send((seq, result)).expect("sink result failure");
    } else {
        error!(
            "abandon result of task submitted by thread {:?} as submit thread disconnected;",
            tid
        );
    }
}

#[cfg(test)]
mod test {
    // use super::*;
    // use std::io::ErrorKind;
    //
    // struct DirectThread;
    //
    // impl Executor for DirectThread {
    //     fn spawn<T: Task + 'static>(&self, task: T) -> Result<TaskGuard, RejectError<T>> {
    //         let mut task = GeneralTask::new(task);
    //         let task_guard = TaskGuard::new(task.get_seq());
    //         match task.execute() {
    //             Ok(TaskState::Finished) => sink_task_result(task.thread, task.seq, None),
    //             Ok(TaskState::NotReady) => {
    //                 sink_task_result(task.thread, task.seq, Some(ExecError::unknown()))
    //             }
    //             Err(err) => sink_task_result(task.thread, task.seq, Some(ExecError::Task(err))),
    //             _ => (),
    //         }
    //         Ok(task_guard)
    //     }
    //
    //     fn spawn_batch<T: Task + 'static, I: Iterator<Item = T>>(
    //         &self, _tasks: &mut I,
    //     ) -> Result<Vec<TaskGuard>, RejectError<()>> {
    //         unimplemented!()
    //     }
    // }
    //
    // struct TestTask(bool);
    // impl Task for TestTask {
    //     fn execute(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
    //         // println!("This is TestTask");
    //         if self.0 {
    //             Ok(TaskState::Finished)
    //         } else {
    //             Err(std::io::Error::from(ErrorKind::Other))?
    //         }
    //     }
    //
    //     fn check_ready(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
    //         Ok(TaskState::Finished)
    //     }
    // }
    //
    // #[test]
    // fn test_executor() {
    //     let executor = DirectThread;
    //     {
    //         let mut guard = executor.spawn(TestTask(true)).unwrap();
    //         let result = guard.join();
    //         assert!(result.is_ok());
    //     }
    //
    //     {
    //         let mut guard = executor.spawn(TestTask(false)).unwrap();
    //         let result = guard.join();
    //         assert!(result.is_err());
    //         match result {
    //             Err(ExecError::Task(err)) => {
    //                 println!("{}", err);
    //             }
    //             _ => assert!(false, "result not match"),
    //         }
    //     }
    // }
    //
    // use std::sync::Arc;
    // #[test]
    // fn test_concurrent_spawn() {
    //     let executor = Arc::new(DirectThread);
    //     let mut threads = Vec::new();
    //     for _ in 0..2 {
    //         let executor = executor.clone();
    //         let g = ::std::thread::spawn(move || {
    //             {
    //                 let mut guard = executor.spawn(TestTask(true)).unwrap();
    //                 assert_eq!(guard.id, 0);
    //                 let result = guard.join();
    //                 assert!(result.is_ok());
    //             }
    //
    //             {
    //                 let mut guard = executor.spawn(TestTask(false)).unwrap();
    //                 assert_eq!(guard.id, 1);
    //                 let result = guard.join();
    //                 assert!(result.is_err());
    //                 match result {
    //                     Err(ExecError::Task(err)) => {
    //                         println!("{}", err);
    //                     }
    //                     _ => assert!(false, "result not match"),
    //                 }
    //             }
    //         });
    //         threads.push(g);
    //     }
    // }
}
