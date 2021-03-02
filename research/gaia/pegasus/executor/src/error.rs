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

use std::error::Error;
use std::fmt::{self, Debug, Display};

pub trait TaskExecError: Error + Send + 'static {}

impl<E: Sized + TaskExecError> TaskExecError for Box<E> {}

impl<E: Sized + TaskExecError> From<E> for Box<dyn TaskExecError> {
    fn from(raw: E) -> Self {
        Box::new(raw)
    }
}

impl TaskExecError for std::io::Error {}

pub struct RejectError<T>(pub T);

impl<T> Debug for RejectError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task is rejected by the executor, try later;")
    }
}

impl<T> Display for RejectError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task is rejected by the executor, try later;")
    }
}

impl<T> Error for RejectError<T> {}

pub struct TaskPanic;

impl Debug for TaskPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task panic;")
    }
}

impl Display for TaskPanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task panic;")
    }
}

impl Error for TaskPanic {}

impl TaskExecError for TaskPanic {}

pub enum ExecError {
    /// Errors occurred when executing task, it usually caused by incorrect computation in task.
    /// It contains a point to the raw error which can be send through thread boundary;
    Task(Box<dyn TaskExecError>),

    Executor(String),
}

impl ExecError {
    pub fn unknown() -> Self {
        ExecError::Executor("unknown".to_string())
    }

    pub(crate) fn executor_error(msg: String) -> Self {
        ExecError::Executor(msg)
    }
}

impl Debug for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Task(err) => Debug::fmt(err, f),
            ExecError::Executor(msg) => write!(f, "Executor error: {:?}", msg),
        }
    }
}

impl Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for ExecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ExecError::Task(err) => err.source(),
            ExecError::Executor(_) => None,
        }
    }
}
