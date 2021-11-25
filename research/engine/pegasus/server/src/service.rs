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
//!

#![allow(dead_code)]

use std::fmt::Debug;
use std::sync::Arc;

use pegasus::api::Source;
use pegasus::result::ResultSink;
use pegasus::{BuildJobError, Data};
use prost::Message;

use crate::generated::protocol as pb;

pub trait JobParser<I: Data, O: Send + Debug + 'static>: Send + Sync + 'static {
    fn parse(
        &self, plan: &pb::JobRequest, input: &mut Source<I>, output: ResultSink<O>,
    ) -> Result<(), BuildJobError>;
}

pub struct Service<I, O, P> {
    parser: Arc<P>,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, P> Clone for Service<I, O, P> {
    fn clone(&self) -> Self {
        Service { parser: self.parser.clone(), _ph: std::marker::PhantomData }
    }
}

unsafe impl<I: Data, O: Send + 'static, P> Sync for Service<I, O, P> {}

impl<I: Data, O: Send + Debug + Message + 'static, P: JobParser<I, O>> Service<I, O, P> {
    pub fn new(parser: P) -> Self {
        Service { parser: Arc::new(parser), _ph: std::marker::PhantomData }
    }

    pub fn accept<'a>(
        &'a self, req: &'a pb::JobRequest,
    ) -> impl FnOnce(&mut Source<I>, ResultSink<O>) -> Result<(), BuildJobError> + 'a {
        move |input, output| self.parser.parse(req, input, output)
    }
}
