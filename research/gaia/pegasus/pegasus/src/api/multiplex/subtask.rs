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

use crate::api::ResultSet;
use crate::data::DataSet;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::{Data, JobConf};
use pegasus_common::codec::*;
use std::fmt::Debug;

pub struct SubtaskResult<T> {
    pub seq: u32,
    result: ResultSet<T>,
}

pub trait SubTask<D: Data> {
    fn fork_subtask<F, T>(&self, func: F) -> Result<Stream<SubtaskResult<T>>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<Stream<T>, BuildJobError> + Send;

    fn fork_detached_subtask<F, T>(
        &self, conf: JobConf, func: F,
    ) -> Result<Stream<SubtaskResult<T>>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<Stream<T>, BuildJobError> + Send;

    fn join_subtask<T, R, F>(
        &self, subtask: Stream<SubtaskResult<T>>, func: F,
    ) -> Result<Stream<R>, BuildJobError>
    where
        T: Data,
        R: Data,
        F: Fn(&D, T) -> Option<R> + Send + 'static;
}

impl<T: Data> Encode for SubtaskResult<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.seq.write_to(writer)?;
        self.result.write_to(writer)?;
        Ok(())
    }
}

impl<T: Data> Decode for SubtaskResult<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let seq = <u32>::read_from(reader)?;
        let result = <ResultSet<T>>::read_from(reader)?;
        Ok(SubtaskResult { seq, result })
    }
}

impl<T> From<&mut DataSet<T>> for SubtaskResult<T> {
    fn from(origin: &mut DataSet<T>) -> Self {
        let seq = origin.tag.current_uncheck();
        let result = std::mem::replace(origin.data(), vec![]);
        SubtaskResult { seq, result: ResultSet::Data(result) }
    }
}

impl<T> SubtaskResult<T> {
    pub fn new(seq: u32, result: ResultSet<T>) -> Self {
        SubtaskResult { seq, result }
    }

    pub fn take(self) -> ResultSet<T> {
        self.result
    }
}

impl<T: Data> Clone for SubtaskResult<T> {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl<T> Debug for SubtaskResult<T> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
