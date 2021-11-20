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
use crate::api::Map;
use crate::api::Unary;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

impl<I: Data> Map<I> for Stream<I> {
    fn map<O, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<O> + Send + 'static,
    {
        self.unary("map", |_info| {
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for item in batch.drain() {
                            let r = func(item)?;
                            session.give(r)?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }

    fn filter_map<O, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<Option<O>> + Send + 'static,
    {
        self.unary("filter_map", |_info| {
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for item in batch.drain() {
                            if let Some(r) = func(item)? {
                                session.give(r)?;
                            }
                        }
                    }
                    Ok(())
                })
            }
        })
    }

    fn flat_map<O, R, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        R: Iterator<Item = O> + Send + 'static,
        F: Fn(I) -> FnResult<R> + Send + 'static,
    {
        self.unary("flat_map", |info| {
            let index = info.index;
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let mut session = output.new_session(&dataset.tag)?;
                        for item in dataset.drain() {
                            let iter = func(item)?;
                            if let Err(err) = session.give_iterator(iter) {
                                if err.is_would_block() || err.is_interrupted() {
                                    trace_worker!("flat_map_{} is blocked on {:?};", index, session.tag,);
                                }
                                return Err(err)?;
                            }
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
