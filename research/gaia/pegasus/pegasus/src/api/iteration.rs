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

use crate::api::function::{FilterFunction, FnResult};
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

pub trait Iteration<D: Data> {
    fn iterate<F>(&self, max_iters: u32, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>;

    fn iterate_until<F>(
        &self, until: LoopCondition<D>, func: F,
    ) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>;
}

pub struct LoopCondition<D> {
    pub max_iters: u32,
    until: Option<Box<dyn FilterFunction<D>>>,
}

impl<D: 'static> LoopCondition<D> {
    pub fn new() -> Self {
        LoopCondition { max_iters: !0u32, until: None }
    }

    pub fn max_iters(max_iters: u32) -> Self {
        LoopCondition { max_iters, until: None }
    }

    pub fn until(&mut self, func: Box<dyn FilterFunction<D>>) {
        self.until = Some(func);
    }

    #[inline]
    pub fn is_converge(&self, data: &D) -> FnResult<bool> {
        if let Some(cond) = self.until.as_ref() {
            cond.exec(data)
        } else {
            Ok(false)
        }
    }

    #[inline]
    pub fn has_until_cond(&self) -> bool {
        self.until.is_some()
    }
}
