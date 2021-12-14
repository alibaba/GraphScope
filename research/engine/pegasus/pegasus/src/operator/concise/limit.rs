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
use crate::communication::output::OutputProxy;
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

// TODO : optimize limit into channel;
impl<D: Data> Limit<D> for Stream<D> {
    fn limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        let stream = self.limit_partition(size)?;
        if stream.get_partitions() > 1 {
            stream.aggregate().limit_partition(size)
        } else {
            Ok(stream)
        }
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

impl<D: Data + Ord> SortLimit<D> for Stream<D> {
    fn sort_limit(self, size: u32) -> Result<Stream<D>, BuildJobError> {
        self.sort_limit_by(size, |x, y| x.cmp(y))
    }
}

impl<D: Data> SortLimitBy<D> for Stream<D> {
    fn sort_limit_by<F>(self, size: u32, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D, &D) -> Ordering + Send + 'static,
    {
        let cmp = ShadeCmp { cmp: Arc::new(cmp) };
        let cmp_clone = cmp.clone();

        let local_sort = sort_limit_by_partition(self, "sort_limit_by_partition_locally", size, cmp_clone)?;
        sort_limit_by_partition(local_sort.aggregate(), "sort_limit_by_partition_globally", size, cmp)
    }
}

type Cmp<D> = Arc<dyn Fn(&D, &D) -> Ordering + Send + 'static>;

struct ShadeCmp<C> {
    cmp: Arc<C>,
}

unsafe impl<C: Send> Send for ShadeCmp<C> {}

impl<C> Clone for ShadeCmp<C> {
    fn clone(&self) -> Self {
        ShadeCmp { cmp: self.cmp.clone() }
    }
}

struct Item<D> {
    inner: D,
    cmp: Cmp<D>,
}

impl<D> Eq for Item<D> {}

impl<D> PartialEq<Self> for Item<D> {
    fn eq(&self, other: &Self) -> bool {
        (*(self.cmp))(&self.inner, &other.inner) == Ordering::Equal
    }
}

impl<D> PartialOrd<Self> for Item<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((*(self.cmp))(&self.inner, &other.inner))
    }
}

impl<D> Ord for Item<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        (*(self.cmp))(&self.inner, &other.inner)
    }
}

unsafe impl<D: Send> Send for Item<D> {}

fn sort_limit_by_partition<D: Data, F>(
    stream: Stream<D>, name: &str, size: u32, cmp: ShadeCmp<F>,
) -> Result<Stream<D>, BuildJobError>
where
    F: Fn(&D, &D) -> Ordering + Send + 'static,
{
    if size == 0 {
        stream.limit(0)
    } else if size == 1 {
        stream.unary(name, |info| {
            let mut table = TidyTagMap::<D>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    if let Some(min_value) = dataset.drain().min_by(|x, y| (*cmp.cmp)(x, y)) {
                        let new_min = if let Some(cur) = table.remove(&dataset.tag) {
                            if (*cmp.cmp)(&min_value, &cur) == Ordering::Less {
                                min_value
                            } else {
                                cur
                            }
                        } else {
                            min_value
                        };

                        if let Some(end) = dataset.take_end() {
                            let mut session = output.new_session(&dataset.tag)?;
                            session.give_last(new_min, end)?;
                        } else {
                            table.insert(dataset.tag.clone(), new_min);
                        }

                        return Ok(());
                    }

                    if let Some(end) = dataset.take_end() {
                        if let Some(cur) = table.remove(&end.tag) {
                            let mut session = output.new_session(&dataset.tag)?;
                            session.give_last(cur, end)?;
                        } else {
                            output.notify_end(end)?;
                        }
                    }

                    Ok(())
                })
            }
        })
    } else {
        stream.unary(name, |info| {
            let mut table = TidyTagMap::<BinaryHeap<Item<D>>>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let cmp_clone = cmp.cmp.clone();
                    if !dataset.is_empty() {
                        let heap = table
                            .get_mut_or_else(&dataset.tag, || BinaryHeap::with_capacity(size as usize));
                        for d in dataset.drain() {
                            if heap.len() < size as usize {
                                heap.push(Item { inner: d, cmp: cmp_clone.clone() });
                            } else {
                                if (*cmp_clone)(&d, &heap.peek().unwrap().inner) == Ordering::Less {
                                    heap.pop();
                                    heap.push(Item { inner: d, cmp: cmp_clone.clone() });
                                }
                            }
                        }
                    }

                    if dataset.is_last() {
                        let mut session = output.new_session(&dataset.tag)?;
                        if let Some(heap) = table.remove(&dataset.tag) {
                            session.give_iterator(
                                heap.into_sorted_vec()
                                    .into_iter()
                                    .map(|item| item.inner),
                            )?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
