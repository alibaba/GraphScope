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

use crate::communication::decorator::ScopeStreamPush;
use crate::communication::output::output::OutputHandle;
use crate::data::DataSet;
use crate::errors::IOResult;
use crate::progress::EndSignal;
use crate::{Data, Tag};
use std::cell::RefMut;

pub struct OutputSession<'a, D: Data> {
    pub tag: Tag,
    output: RefMut<'a, OutputHandle<D>>,
}

impl<'a, D: Data> OutputSession<'a, D> {
    pub(crate) fn new(output: RefMut<'a, OutputHandle<D>>, tag: Tag) -> Self {
        OutputSession { tag, output }
    }

    pub fn give(&mut self, msg: D) -> IOResult<()> {
        self.output.push(&self.tag, msg)
    }

    pub fn give_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
        assert_eq!(self.tag, end.tag);
        self.output.push_last(msg, end)
    }

    pub(crate) fn forward_batch(&mut self, dataset: &mut DataSet<D>) -> IOResult<()> {
        let seq = dataset.seq;
        let data = DataSet::new(self.tag.clone(), dataset.src, seq, dataset.take_batch());
        self.output.forward(data)
    }

    pub fn give_iterator<I>(&mut self, iter: I) -> IOResult<()>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        self.output.push_entire_iter(&self.tag, iter)
    }

    pub fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
        assert_eq!(self.tag, end.tag);
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }
}
