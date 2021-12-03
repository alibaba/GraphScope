//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use super::*;
use crate::operator::unary::Unary;

pub trait Count<D: Data, A> {
    fn count<P: Communicate<D>>(&self, comm: P) -> Stream<usize, A>;
}

impl<D: Data, A: DataflowBuilder> Count<D, A> for Stream<D, A> {
    fn count<P: Communicate<D>>(&self, comm: P) -> Stream<usize, A> {
        self.unary_state("count", comm, |info| {
            info.set_clip();
            (
                |input, _output, counts| {
                    input.for_each_batch(|dataset| {
                        let (t, data) = dataset.take();
                        let count_of = counts.entry(t).or_insert(0usize);
                        *count_of += data.len();
                        Ok(true)
                    })?;
                    Ok(())
                },
                |output, counts| {
                    for (t, count) in counts {
                        let mut session = output.session(&t);
                        session.give(count)?;
                    }
                    Ok(())
                }
            )
        })
    }
}
