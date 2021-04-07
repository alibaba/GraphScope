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

use crate::api::function::*;
use crate::api::meta::OperatorKind;
use crate::api::{LazyUnary, Map, Unary};
use crate::communication::Channel;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;
use std::error::Error;

impl<I: Data> Map<I> for Stream<I> {
    fn map<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: MapFunction<I, O>,
    {
        self.unary("map", channel, |meta| {
            meta.set_kind(OperatorKind::Map);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    for datum in dataset.drain(..) {
                        let resp = func.exec(datum)?;
                        output.give(resp)?;
                    }
                    Ok(())
                })
            }
        })
    }

    fn map_with_fn<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: Fn(I) -> FnResult<O> + Send + 'static,
    {
        self.map(channel, map!(func))
    }

    fn map_in_place<C, F>(&self, channel: C, func: F) -> Result<Stream<I>, BuildJobError>
    where
        C: Into<Channel<I>>,
        F: Fn(&mut I) + Send + 'static,
    {
        self.unary("map_in_place", channel, |meta| {
            meta.set_kind(OperatorKind::Map);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    for datum in dataset.iter_mut() {
                        func(datum);
                    }
                    output.forward(dataset)?;
                    Ok(())
                })
            }
        })
    }

    fn flat_map<O, C, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: FlatMapFunction<I, O>,
    {
        self.lazy_unary("flat_map", channel, move |_| func)
    }

    fn flat_map_with_fn<O, C, R, F>(&self, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        R: Iterator<Item = Result<O, Box<dyn Error + Send>>> + Send + 'static,
        F: Fn(I) -> FnResult<R> + Send + 'static,
    {
        self.flat_map(channel, flat_map!(func))
    }
}
