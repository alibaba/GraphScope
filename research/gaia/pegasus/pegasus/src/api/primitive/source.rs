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

use crate::dataflow::DataflowBuilder;
use crate::errors::{BuildJobError, IOError};
use crate::stream::Stream;
use crate::Data;
use crossbeam_channel::{Receiver, TryRecvError};

pub trait IntoStream<D: Data> {
    fn into_stream(self, dfb: &DataflowBuilder) -> Result<Stream<D>, BuildJobError>;
}

pub trait FromStream<D: Data> {
    fn from_stream(&self, origin: &Stream<D>) -> Result<Stream<D>, BuildJobError>;
}

pub trait ExternSource: Send {
    type Item;

    fn pull_next(&mut self) -> Result<Option<Self::Item>, IOError>;
}

pub struct NonBlockReceiver<T> {
    rx: Receiver<T>,
}

impl<T> NonBlockReceiver<T> {
    pub fn new(rx: Receiver<T>) -> Self {
        NonBlockReceiver { rx }
    }
}

impl<T: Send> ExternSource for NonBlockReceiver<T> {
    type Item = T;

    fn pull_next(&mut self) -> Result<Option<Self::Item>, IOError> {
        match self.rx.try_recv() {
            Ok(data) => Ok(Some(data)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(IOError::source_exhaust()),
        }
    }
}
