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

use std::cmp::Ordering;

use crate::api::{Sort, SortBy, Unary};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data + Ord> Sort<D> for Stream<D> {
    fn sort(self) -> Result<Stream<D>, BuildJobError> {
        self.aggregate().unary("sort", |info| {
            let mut map = TidyTagMap::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let vec = map.get_mut_or_else(&dataset.tag, Vec::new);
                        for d in dataset.drain() {
                            vec.push(d);
                        }
                    }

                    if dataset.is_last() {
                        let mut session = output.new_session(&dataset.tag)?;
                        if let Some(mut vec) = map.remove(&dataset.tag) {
                            vec.sort();
                            session.give_iterator(vec.into_iter())?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}

impl<D: Data> SortBy<D> for Stream<D> {
    fn sort_by<F>(self, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D, &D) -> Ordering + Send + 'static,
    {
        self.aggregate().unary("sort_by", |info| {
            let mut map = TidyTagMap::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let vec = map.get_mut_or_else(&dataset.tag, Vec::new);
                        for d in dataset.drain() {
                            vec.push(d);
                        }
                    }

                    if dataset.is_last() {
                        let mut session = output.new_session(&dataset.tag)?;
                        if let Some(mut vec) = map.remove(&dataset.tag) {
                            vec.sort_by(|x, y| cmp(x, y));
                            session.give_iterator(vec.into_iter())?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
