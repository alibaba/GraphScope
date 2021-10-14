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

use std::hash::Hash;
use super::*;
use crate::operator::unary::Unary;

pub trait Dedup<D: Data, A> {
    fn dedup<P, F, K>(&self, comm: P, func: F) -> Stream<D, A>
        where P: Communicate<D>, K: Hash + Eq + Send + 'static,
              F: Fn(&D) -> K + Send + 'static;
}

impl<D: Data, A: DataflowBuilder> Dedup<D, A> for Stream<D, A> {
    fn dedup<P, F, K>(&self, comm: P, func: F) -> Stream<D, A>
        where P: Communicate<D>, K: Hash + Eq + Send + 'static,
              F: Fn(&D) -> K + Send + 'static
    {
        self.unary_state("dedup", comm, |info| {
            info.set_clip();
            (
                move |input, output, set| {
                    input.for_each_batch(|dataset| {
                        let mut session = output.session(&dataset);
                        let (t, data) = dataset.take();
                        let dup = set.entry(t).or_insert(HashSet::new());
                        for datum in data {
                            let key = func(&datum);
                            if !dup.contains(&key) {
                                dup.insert(key);
                                session.give(datum)?;
                            }
                        }
                        Ok(session.has_capacity())
                    })?;
                    Ok(())
                },
                |_output, _set| { Ok(()) }
            )
        })
    }
}
