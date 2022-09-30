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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

mod error;
mod pending;
mod reactor;
pub use error::RejectError;
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
    fn execute(&mut self) -> TaskState;

    /// Once the task was interrupted, The [`Executor`] will keep watching the computation state by
    /// invoke this function to check if the computation can be continued or finished;
    ///
    /// # Return value:
    ///
    /// todo;
    fn check_ready(&mut self) -> TaskState;
}

lazy_static! {
    static ref SHUTDOWN_HOOK: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    static ref IN_PROGRESS_TASK_COUNT: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

pub trait Executor: Send + Sync {
    fn spawn<T: Task + 'static>(&self, task: T) -> Result<(), RejectError<T>>;

    fn spawn_batch<T: Task + 'static, I: IntoIterator<Item = T>>(
        &self, tasks: I,
    ) -> Result<(), RejectError<()>>;
}

struct GeneralTask {
    inner: Box<dyn Task>,
}

impl GeneralTask {
    fn new<T: Task + 'static>(task: T) -> Self {
        IN_PROGRESS_TASK_COUNT.fetch_add(1, Ordering::SeqCst);
        GeneralTask { inner: Box::new(task) }
    }
}

impl Drop for GeneralTask {
    fn drop(&mut self) {
        IN_PROGRESS_TASK_COUNT.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Task for GeneralTask {
    #[inline(always)]
    fn execute(&mut self) -> TaskState {
        self.inner.execute()
    }

    #[inline(always)]
    fn check_ready(&mut self) -> TaskState {
        self.inner.check_ready()
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
