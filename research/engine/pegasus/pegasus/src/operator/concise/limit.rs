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
use std::mem::ManuallyDrop;
use std::ptr;
use std::rc::Rc;
use crate::api::{Limit, SortLimit, SortLimitBy, Unary};
use crate::stream::Stream;
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};
use crate::communication::output::OutputProxy;

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
    fn sort_limit_by<F>(self, size: u32, cmp: F) -> Result<Stream<D>, BuildJobError> where F: Fn(&D, &D) -> Ordering + Send + 'static {
        let share_cmp = ShadeCmp { cmp: Rc::new(cmp) };
        let cmp_clone = ShadeCmp { cmp: share_cmp.cmp.clone() };

        let local_sort = sort_limit_by_partition(self, size, cmp_clone, false)?;
        sort_limit_by_partition(local_sort.aggregate(), size, share_cmp, true)
    }
}

fn sort_limit_by_partition<D: Data, F>(stream: Stream<D>, size: u32, share_cmp: ShadeCmp<F>, last_sort: bool) -> Result<Stream<D>, BuildJobError>
where F: Fn(&D, &D) -> Ordering + Send + 'static {
    if size == 0 {
        stream.limit(0)
    } else if size == 1 {
        stream.unary("sort_limit_by_partition", |info| {
            let mut table = TidyTagMap::<D>::new(info.scope_level);
            move |input, output| {
                let cmp_clone = share_cmp.cmp.clone();
                input.for_each_batch(|dataset| {
                    if let Some(min_value) = dataset.drain().min_by(|x, y| (*cmp_clone)(x, y)) {
                        let new_min = if let Some(cur) = table.remove(&dataset.tag) {
                            if (*cmp_clone)(&min_value, &cur) == Ordering::Less {
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
        stream.unary("sort_limit_by_partition", |info| {
            let mut table = TidyTagMap::<FixedSizeHeap<D>>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let cmp_clone = share_cmp.cmp.clone();
                    if !dataset.is_empty() {
                        let heap = table.get_mut_or_else(&dataset.tag, || { FixedSizeHeap::new(size as usize, cmp_clone)});
                        for d in dataset.drain() {
                            heap.push(d);
                        }
                    }

                    if dataset.is_last() {
                        let mut session = output.new_session(&dataset.tag)?;
                        if let Some(mut heap) = table.remove(&dataset.tag) {
                            if last_sort {
                                heap.sort();
                            }
                            let im_heap = heap;
                            session.give_iterator(im_heap.into_iter())?;
                        }
                    }

                    Ok(())
                })
            }
        })
    }
}

type Cmp<D> = Rc<dyn Fn(&D, &D) -> Ordering + Send + 'static>;

struct ShadeCmp<C> {
    pub cmp: Rc<C>,
}

unsafe impl<C: Send> Send for ShadeCmp<C> {}

struct FixedSizeHeap<D> {
    limit: usize,
    cmp: Cmp<D>,
    data: Vec<D>,
}

unsafe impl<D: Send> Send for FixedSizeHeap<D> {}

struct Hole<'a, T: 'a> {
    data: &'a mut[T],
    elt: ManuallyDrop<T>,
    pos: usize,
}

impl<'a, T> Hole<'a, T> {
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        let elt = unsafe { ptr::read(data.get_unchecked(pos)) };
        Hole { data, elt: ManuallyDrop::new(elt), pos }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    fn element(&self) -> &T {
        &self.elt
    }

    #[inline]
    fn get(&self, index: usize) -> &T {
        unsafe { self.data.get_unchecked(index) }
    }

    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        unsafe {
            let ptr = self.data.as_mut_ptr();
            let index_ptr: *const _ = ptr.add(index);
            let hole_ptr = ptr.add(self.pos);
            ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        }
        self.pos = index;
    }
}

impl<T> Drop for Hole<'_, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let pos = self.pos;
            ptr::copy_nonoverlapping(&*self.elt, self.data.get_unchecked_mut(pos), 1);
        }
    }
}

impl<T> FixedSizeHeap<T> {
    pub fn new(limit: usize, cmp: Cmp<T>) -> Self {
        FixedSizeHeap { limit, cmp, data: Vec::with_capacity(limit) }
    }

    pub fn push(&mut self, item: T) {
        let old_len = self.data.len();
        if old_len == self.limit {
            if (*self.cmp)(&self.data[0], &item) == Ordering::Greater {
                self.data[0] = item;
                unsafe { self.sift_down_to_bottom(0) };
            }
        } else {
            self.data.push(item);
            unsafe { self.sift_up(0, old_len) };
        }
    }

    unsafe fn sift_up(&mut self, start: usize, pos: usize) -> usize {
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };

        while hole.pos() > start {
            let parent = (hole.pos() - 1) / 2;
            if (*self.cmp)(hole.element(), unsafe { hole.get(parent)}) != Ordering::Greater {
                break;
            }
            unsafe { hole.move_to(parent) };
        }

        hole.pos()
    }

    unsafe fn sift_down_to_bottom(&mut self, mut pos: usize) {
        let end = self.data.len();
        let start = pos;

        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        while child <= end.saturating_sub(2) {
            child += unsafe { (*self.cmp)(hole.get(child), hole.get(child + 1)) != Ordering::Greater } as usize;
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1
        }

        if child == end - 1 {
            unsafe { hole.move_to(child) };
        }

        pos = hole.pos();
        drop(hole);

        unsafe { self.sift_up(start, pos) };
    }

    pub fn sort(&mut self) {
        let cmp = self.cmp.clone();
        self.data.sort_by(|x, y| (*cmp)(&x, &y));
    }
}

impl<T> IntoIterator for FixedSizeHeap<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}