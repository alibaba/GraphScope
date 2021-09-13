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

use crate::api::{Limit, SortLimit, SortLimitBy, Unary};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::rc::Rc;

impl<D: Data> Limit<D> for Stream<D> {
    fn limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        self.limit_partition(size)?
            .aggregate()
            .limit_partition(size)
    }

    fn limit_partition(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        if size == 0 {
            return BuildJobError::unsupported("limit n cannot equal to zero");
        }
        self.unary("limit_partition", |info| {
            let mut table = TidyTagMap::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let mut session = output.new_session(&dataset.tag)?;
                        let count = table.get_mut_or_else(&dataset.tag, || 0u32);
                        if *count < size {
                            for d in dataset.drain() {
                                session.give(d)?;
                                *count += 1;
                                if *count >= size {
                                    break;
                                }
                            }
                            if *count >= size {
                                // trigger early-stop
                                dataset.discard();
                            }
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}

impl<D: Data + Ord> SortLimit<D> for Stream<D> {
    fn sort_limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        if size == 0 {
            return BuildJobError::unsupported("sort_limit n cannot equal to zero");
        }
        self.sort_limit_by(size, |x, y| x.cmp(y))
    }
}

impl<D: Data> SortLimitBy<D> for Stream<D> {
    fn sort_limit_by<F>(self, size: u32, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D, &D) -> Ordering + Send + 'static,
    {
        if size == 0 {
            return BuildJobError::unsupported("sort_limit n cannot equal to zero");
        }
        let share_cmp = ShadeCmp { cmp: Rc::new(cmp) };
        let cmp_clone = ShadeCmp { cmp: share_cmp.cmp.clone() };
        let local_sort = sort_limit_by_partition(self, size, cmp_clone)?;
        sort_limit_by_partition(local_sort.aggregate(), size, share_cmp)
    }
}

#[inline]
fn sort_limit_by_partition<D: Data, F>(
    stream: Stream<D>, size: u32, share_cmp: ShadeCmp<F>,
) -> Result<Stream<D>, BuildJobError>
where
    F: Fn(&D, &D) -> Ordering + Send + 'static,
{
    stream.unary("sort_limit_by_partition", |info| {
        let mut table = TidyTagMap::new(info.scope_level);
        move |input, output| {
            input.for_each_batch(|dataset| {
                if !dataset.is_empty() {
                    let custom_heap = table.get_mut_or_else(&dataset.tag, || {
                        FixedSizeHeap::with_cmp(size as usize, share_cmp.cmp.clone())
                    });
                    for d in dataset.drain() {
                        custom_heap.add(d)?;
                    }
                }
                if dataset.is_last() {
                    let mut session = output.new_session(&dataset.tag)?;
                    if let Some(custom_heap) = table.remove(&dataset.tag) {
                        session.give_iterator(custom_heap.into_iter())?
                    }
                }
                Ok(())
            })
        }
    })
}

type Cmp<D> = Rc<dyn Fn(&D, &D) -> Ordering + Send>;

struct FixedSizeHeap<D> {
    pub limit: usize,
    cmp: Cmp<D>,
    pub heap: BinaryHeap<Item<D>>,
}

impl<D> IntoIterator for FixedSizeHeap<D> {
    type Item = D;
    type IntoIter = std::vec::IntoIter<D>;

    fn into_iter(self) -> Self::IntoIter {
        let mut vec = Vec::with_capacity(self.heap.len());
        for item in self.heap.into_sorted_vec() {
            vec.push(item.inner);
        }
        vec.into_iter()
    }
}

#[allow(dead_code)]
impl<D: Ord> FixedSizeHeap<D> {
    fn new(limit: usize) -> Self {
        FixedSizeHeap::with_cmp(limit, Rc::new(|x, y| x.cmp(y)))
    }
}

impl<D> FixedSizeHeap<D> {
    fn with_cmp(limit: usize, cmp: Cmp<D>) -> Self {
        if limit < 10240 {
            FixedSizeHeap { limit, cmp, heap: BinaryHeap::with_capacity(limit) }
        } else {
            FixedSizeHeap { limit, cmp, heap: BinaryHeap::with_capacity(10240) }
        }
    }

    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.limit {
            if let Some(mut head) = self.heap.peek_mut() {
                if Some(Ordering::Greater) == head.partial_cmp(&item) {
                    head.inner = item;
                }
            }
        } else {
            self.heap
                .push(Item { inner: item, cmp: self.cmp.clone() });
        }
        Ok(())
    }
}

unsafe impl<D: Send> Send for FixedSizeHeap<D> {}

struct Item<D> {
    inner: D,
    cmp: Cmp<D>,
}

impl<D> PartialEq for Item<D> {
    fn eq(&self, other: &Self) -> bool {
        let left = &self.inner;
        (*(self.cmp))(left, &other.inner) == Ordering::Equal
    }
}

impl<D> PartialEq<D> for Item<D> {
    fn eq(&self, other: &D) -> bool {
        let left = &self.inner;
        (*(self.cmp))(left, other) == Ordering::Equal
    }
}

impl<D> Eq for Item<D> {}

impl<D> PartialOrd for Item<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let left = &self.inner;
        Some((*(self.cmp))(left, &other.inner))
    }
}

impl<D> PartialOrd<D> for Item<D> {
    fn partial_cmp(&self, other: &D) -> Option<Ordering> {
        let left = &self.inner;
        Some((*(self.cmp))(left, other))
    }
}

impl<D> Ord for Item<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = &self.inner;
        (*(self.cmp))(left, &other.inner)
    }
}

unsafe impl<D: Send> Send for Item<D> {}

struct ShadeCmp<C> {
    cmp: Rc<C>,
}

unsafe impl<C: Send> Send for ShadeCmp<C> {}
