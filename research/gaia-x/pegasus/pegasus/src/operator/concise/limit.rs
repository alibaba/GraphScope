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

use crate::api::{Limit, OrderLimit, OrderLimitBy, Unary};
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

struct SmallHeap<D: Ord> {
    pub limit: usize,
    pub heap: BinaryHeap<D>,
}

impl<D: Ord> IntoIterator for SmallHeap<D> {
    type Item = D;
    type IntoIter = std::vec::IntoIter<D>;

    fn into_iter(self) -> Self::IntoIter {
        self.heap.into_sorted_vec().into_iter()
    }
}

impl<D: Ord> SmallHeap<D> {
    fn new(limit: usize) -> Self {
        SmallHeap { limit, heap: BinaryHeap::new() }
    }

    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.limit {
            let mut head = self
                .heap
                .peek_mut()
                .expect("unreachable: len > 0");
            // if others <= head > item,
            if Ordering::Greater == head.cmp(&item) {
                *head = item;
            }
        } else {
            self.heap.push(item);
        }
        Ok(())
    }
}

impl<D: Data + Ord> OrderLimit<D> for Stream<D> {
    fn sort_limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        if size == 0 {
            return BuildJobError::unsupported("sort_limit n cannot equal to zero");
        }
        let local_sort = sort_limit_partition(self, size)?;
        sort_limit_partition(local_sort.aggregate(), size)
    }
}

#[inline]
fn sort_limit_partition<D: Data + Ord>(stream: Stream<D>, size: u32) -> Result<Stream<D>, BuildJobError> {
    stream.unary("sort_limit_local", |info| {
        let mut table = TidyTagMap::new(info.scope_level);
        move |input, output| {
            input.for_each_batch(|dataset| {
                if !dataset.is_empty() {
                    let small_heap = table.get_mut_or_else(&dataset.tag, || SmallHeap::new(size as usize));
                    for d in dataset.drain() {
                        small_heap.add(d)?;
                    }
                }
                if dataset.is_last() {
                    let mut session = output.new_session(&dataset.tag)?;
                    if let Some(small_heap) = table.remove(&dataset.tag) {
                        session.give_iterator(small_heap.into_iter())?
                    }
                }
                Ok(())
            })
        }
    })
}

struct CustomHeap<D, C: Fn(&D, &D) -> Ordering + Send + 'static> {
    pub limit: usize,
    cmp: Rc<C>,
    pub heap: BinaryHeap<Item<D, C>>,
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> IntoIterator for CustomHeap<D, C> {
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

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> CustomHeap<D, C> {
    fn new(limit: usize, cmp: Rc<C>) -> Self {
        CustomHeap { limit, cmp, heap: BinaryHeap::new() }
    }

    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.limit {
            let mut head = self
                .heap
                .peek_mut()
                .expect("unreachable: len > 0");
            // if others <= head > item,
            if Some(Ordering::Greater) == head.partial_cmp(&item) {
                head.inner = item;
            }
        } else {
            self.heap
                .push(Item { inner: item, cmp: self.cmp.clone() });
        }
        Ok(())
    }
}

unsafe impl<D: Send, C: Fn(&D, &D) -> Ordering + Send + 'static> Send for CustomHeap<D, C> {}

struct Item<D, C: Fn(&D, &D) -> Ordering + Send + 'static> {
    inner: D,
    cmp: Rc<C>,
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> PartialEq for Item<D, C> {
    fn eq(&self, other: &Self) -> bool {
        let left = &self.inner;
        (*(self.cmp))(left, &other.inner) == Ordering::Equal
    }
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> PartialEq<D> for Item<D, C> {
    fn eq(&self, other: &D) -> bool {
        let left = &self.inner;
        (*(self.cmp))(left, other) == Ordering::Equal
    }
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> Eq for Item<D, C> {}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> PartialOrd for Item<D, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let left = &self.inner;
        Some((*(self.cmp))(left, &other.inner))
    }
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> PartialOrd<D> for Item<D, C> {
    fn partial_cmp(&self, other: &D) -> Option<Ordering> {
        let left = &self.inner;
        Some((*(self.cmp))(left, other))
    }
}

impl<D, C: Fn(&D, &D) -> Ordering + Send + 'static> Ord for Item<D, C> {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = &self.inner;
        (*(self.cmp))(left, &other.inner)
    }
}

unsafe impl<D: Send, C: Fn(&D, &D) -> Ordering + Send + 'static> Send for Item<D, C> {}

struct ShadeCmp<C> {
    cmp: Rc<C>,
}

unsafe impl<C: Send> Send for ShadeCmp<C> {}

impl<D: Data> OrderLimitBy<D> for Stream<D> {
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
                        CustomHeap::new(size as usize, share_cmp.cmp.clone())
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
