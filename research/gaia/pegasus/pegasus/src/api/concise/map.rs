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

use crate::api::function::{FlatMapFunction, FnResult, MapFunction};
use crate::communication::Channel;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;
use std::error::Error;

pub trait Map<I: Data> {
    fn map<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: MapFunction<I, O>;

    fn map_with_fn<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: Fn(I) -> FnResult<O> + Send + 'static;

    fn map_in_place<C, F>(&self, channel: C, func: F) -> Result<Stream<I>, BuildJobError>
    where
        C: Into<Channel<I>>,
        F: Fn(&mut I) + Send + 'static;

    fn flat_map<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: FlatMapFunction<I, O>;

    fn flat_map_with_fn<O, C, R, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        R: Iterator<Item = Result<O, Box<dyn Error + Send>>> + Send + 'static,
        F: Fn(I) -> FnResult<R> + Send + 'static;
}
