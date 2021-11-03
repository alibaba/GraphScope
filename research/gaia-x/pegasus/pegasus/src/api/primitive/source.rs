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

use crate::communication::output::OutputBuilderImpl;
use crate::dataflow::DataflowBuilder;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

pub trait IntoDataflow<D: Data> {
    fn into_dataflow(self, entry: Stream<D>) -> Result<Stream<D>, BuildJobError>;
}

pub struct Source<D: Data> {
    output: OutputBuilderImpl<D>,
    dfb: DataflowBuilder,
}

impl<D: Data> Source<D> {
    pub(crate) fn new(output: OutputBuilderImpl<D>, dfb: &DataflowBuilder) -> Self {
        Source { output, dfb: dfb.clone() }
    }

    pub fn input_from<I>(&mut self, source: I) -> Result<Stream<D>, BuildJobError>
    where
        I: IntoIterator<Item = D>,
        I::IntoIter: Send + 'static,
    {
        let output = self.output.copy_data();
        let output = std::mem::replace(&mut self.output, output);
        let stream = Stream::new(output, &self.dfb);
        source.into_dataflow(stream)
    }
}
