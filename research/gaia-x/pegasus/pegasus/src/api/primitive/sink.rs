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

use crate::api::function::FnResult;
use crate::BuildJobError;

/// `FromStream` provides the capability to consume the data from the stream
pub trait FromStream<D>: Send + 'static {
    fn on_next(&mut self, next: D) -> FnResult<()>;
}

/// `Sink` the final results for further processing.  
pub trait Sink<D> {
    /// Upon the completion of the computation, `sink_into()` attempts to sink all resulted data
    /// into the given `collector`, which most typically is a [`ResultSink`]. The [`ResultSink`]
    /// is able to collect all resulted data into the first worker to allow the user to play
    /// with the results as if it is a sequential programming. This greatly simplifies distributed
    /// programming, especially while debugging or verifying test results.
    ///
    /// After sinking the results, user can treat the results just like an iterator of
    /// `Result<D>`, where `D` is the resulted data type. One can see the following codes
    /// in a lot of test cases/examples:
    ///
    /// let mut results = pegasus::run(..., |input. output| {
    ///         <main_dataflow>
    ///         .collect::<Vec<_>>()
    ///         .sink_into(output)
    ///     }).expect("build job failure");
    ///
    /// Then the results can be verified locally to check correctness as
    /// assert_eq!(results.next().unwrap().unwrap(), [x, y, z, ...]);
    ///
    /// [`ResultSink`]: crate::result::ResultSink
    fn sink_into<C: FromStream<D>>(self, collector: C) -> Result<(), BuildJobError>;
}

impl<D: Send + 'static> FromStream<D> for crossbeam_channel::Sender<D> {
    fn on_next(&mut self, next: D) -> FnResult<()> {
        self.send(next)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}

impl<D: Send + 'static> FromStream<D> for std::sync::mpsc::Sender<D> {
    fn on_next(&mut self, next: D) -> FnResult<()> {
        self.send(next)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}

impl<D: Send + 'static> FromStream<D> for Vec<D> {
    fn on_next(&mut self, next: D) -> FnResult<()> {
        self.push(next);
        Ok(())
    }
}
