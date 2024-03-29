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

pub struct InternalError {
    pub reason: String,
}

impl InternalError {
    pub fn new(reason: String) -> Self {
        InternalError { reason }
    }
}

impl Debug for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Internal error occured {};", self.reason)
    }
}

impl Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Internal error occurs {};", self.reason)
    }
}

impl Error for InternalError {}

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
