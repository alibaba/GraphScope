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

use crate::api::{IntoDataflow, Unary};
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

impl<D: Data, T: IntoIterator<Item = D>> IntoDataflow<D> for T
where
    T::IntoIter: Iterator + Send + 'static,
{
    fn into_dataflow(self, entry: Stream<D>) -> Result<Stream<D>, BuildJobError> {
        let mut iter = Some(self.into_iter());
        entry.unary("source", |_info| {
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let mut session = output.new_session(&dataset.tag)?;
                    if let Some(iter) = iter.take() {
                        session.give_iterator(iter.fuse())?;
                        session.flush()?;
                    }
                    Ok(())
                })
            }
        })
    }
}
