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

use crate::api::{Limit, Unary};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};
// TODO : optimize limit into channel;
impl<D: Data> Limit<D> for Stream<D> {
    fn limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        self.limit_partition(size)?
            .aggregate()
            .limit_partition(size)
    }

    fn limit_partition(mut self, size: u32) -> Result<Stream<D>, BuildJobError> {
        self.set_upstream_batch_size(size as usize);
        self.set_upstream_batch_capacity(1);
        self.unary("limit_partition", |info| {
            let mut table = TidyTagMap::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        let count = table.get_mut_or_else(&batch.tag, || 0u32);
                        if *count < size {
                            for d in batch.drain() {
                                *count += 1;
                                session.give(d)?;
                                if *count >= size {
                                    break;
                                }
                            }
                            if *count >= size {
                                // trigger early-stop
                                batch.discard();
                            }
                        } else {
                            batch.discard();
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
