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

use crate::api::concise::reduce::barrier::Barrier;
use crate::api::concise::reduce::order::{Order, OrderDirect};
use crate::api::function::*;
use crate::api::{Map, OrderBy, Range};
use crate::codec::{shade_codec, ShadeCodec};
use crate::communication::Pipeline;
use crate::operator::concise::{never_clone, NeverClone};
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::codec::{Codec, Decode, Encode, ReadExt, WriteExt};
use pegasus_common::collections::{Collection, CollectionFactory};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

impl<D: Data + Ord> Order<D> for Stream<D> {
    fn sort(&self, range: Range, order: OrderDirect) -> Result<Stream<D>, BuildJobError> {
        let barrier = self.barrier::<Vec<D>>(range)?;
        barrier.flat_map_with_fn(Pipeline, move |mut input| {
            match order {
                OrderDirect::Asc => input.sort(),
                OrderDirect::Desc => input.sort_by(|a, b| b.cmp(a)),
            }
            Ok(input.into_iter().map(|item| Ok(item)))
        })
    }

    fn top(
        &self, limit: u32, range: Range, order: OrderDirect,
    ) -> Result<Stream<D>, BuildJobError> {
        if limit == 0 {
            return BuildJobError::unsupported("top n can't equal to 0");
        }
        let limit = limit as usize;

        let stream = get_top(&self, Range::Local, order, limit)?;
        if Range::Global == range {
            get_top(&stream, Range::Global, order, limit)
        } else {
            Ok(stream)
        }
    }
}

impl<D: Data> OrderBy<D> for Stream<D> {
    fn sort_by<F>(&self, range: Range, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: CompareFunction<D> + 'static,
    {
        let barrier = self.barrier::<Vec<D>>(range)?;
        barrier.flat_map_with_fn(Pipeline, move |mut input| {
            input.sort_by(|a, b| cmp.compare(a, b));
            Ok(input.into_iter().map(|item| Ok(item)))
        })
    }

    fn top_by<F>(&self, limit: u32, range: Range, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: CompareFunction<D> + 'static,
    {
        if limit == 0 {
            return BuildJobError::unsupported("top n can't equal to 0");
        }

        let param = OrdParam::<D, F>::new(limit as usize, Box::new(cmp));
        let factory = CustomOrdQueueFactory::new(param.clone());
        let barrier = self.barrier_with(Range::Local, factory)?;
        let stream = barrier.flat_map_with_fn(Pipeline, move |input| {
            let input = input.take().take();
            Ok(input.into_iter().map(|item| Ok(item)))
        });

        match range {
            Range::Local => stream,
            Range::Global => {
                let factory = CustomOrdQueueFactory::new(param.clone());
                stream?.barrier_with(range, factory)?.flat_map_with_fn(Pipeline, move |input| {
                    let input = input.take().take();
                    Ok(input.into_iter().map(|item| Ok(item)))
                })
            }
        }
    }
}

#[inline]
fn get_top<D: Ord + Data>(
    stream: &Stream<D>, range: Range, order: OrderDirect, limit: usize,
) -> Result<Stream<D>, BuildJobError> {
    match order {
        OrderDirect::Asc => {
            let factory = SmallHeapFactory { limit, _ph: std::marker::PhantomData };
            stream.barrier_with(range, factory)?.flat_map_with_fn(Pipeline, move |input| {
                let input = input.take().take();
                Ok(input.into_iter().map(|item| Ok(item)))
            })
        }
        OrderDirect::Desc => {
            let factory = LargeHeapFactory { limit, _ph: std::marker::PhantomData };
            stream.barrier_with(range, factory)?.flat_map_with_fn(Pipeline, move |input| {
                let input = input.take().take();
                Ok(input.into_iter().map(|item| Ok(item)))
            })
        }
    }
}

#[derive(PartialEq, Eq)]
struct Reverse<D: Ord>(pub D);

impl<D: Ord + Debug> Debug for Reverse<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "reverse({:?})", self.0)
    }
}

impl<D: Ord> PartialOrd for Reverse<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0).map(|o| o.reverse())
    }
}

impl<D: Ord> Ord for Reverse<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

impl<D: Ord + Clone> Clone for Reverse<D> {
    fn clone(&self) -> Self {
        Reverse(self.0.clone())
    }
}

impl<D: Ord + Codec> Encode for Reverse<D> {
    fn write_to<W: WriteExt>(&self, _: &mut W) -> std::io::Result<()> {
        unreachable!()
    }
}

impl<D: Ord + Codec> Decode for Reverse<D> {
    fn read_from<R: ReadExt>(_: &mut R) -> std::io::Result<Self> {
        unreachable!()
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

impl<D: Ord + Debug> Debug for SmallHeap<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}] limit {}", self.heap, self.limit)
    }
}

impl<D: Ord + Send> Collection<D> for SmallHeap<D> {
    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.limit {
            let mut head = self.heap.peek_mut().expect("unreachable: len > 0");
            // if others <= head > item,
            if Ordering::Greater == head.cmp(&item) {
                *head = item;
            }
        } else {
            self.heap.push(item);
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.heap.clear();
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}

struct LargeHeap<D: Ord> {
    pub limit: usize,
    pub heap: BinaryHeap<Reverse<D>>,
}

impl<D: Ord> IntoIterator for LargeHeap<D> {
    type Item = D;
    type IntoIter = std::vec::IntoIter<D>;

    fn into_iter(self) -> Self::IntoIter {
        let mut vec = Vec::with_capacity(self.limit);
        for item in self.heap.into_sorted_vec() {
            vec.push(item.0)
        }
        vec.into_iter()
    }
}

impl<D: Ord + Debug> Debug for LargeHeap<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}] limit {}", self.heap, self.limit)
    }
}

impl<D: Ord + Send> Collection<D> for LargeHeap<D> {
    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.limit {
            let item = Reverse(item);
            let mut head = self.heap.peek_mut().expect("unreachable: len > 0");
            // others <= head < item;
            if Ordering::Greater == head.cmp(&item) {
                *head = item;
            }
        } else {
            self.heap.push(Reverse(item));
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.heap.clear();
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}

struct SmallHeapFactory<D: Ord> {
    limit: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Ord + Send> CollectionFactory<D> for SmallHeapFactory<D> {
    type Target = NeverClone<ShadeCodec<SmallHeap<D>>>;

    fn create(&self) -> Self::Target {
        let heap = SmallHeap { limit: self.limit, heap: BinaryHeap::new() };
        never_clone(shade_codec(heap))
    }
}

struct LargeHeapFactory<D: Ord> {
    limit: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Ord + Send> CollectionFactory<D> for LargeHeapFactory<D> {
    type Target = NeverClone<ShadeCodec<LargeHeap<D>>>;

    fn create(&self) -> Self::Target {
        let heap = LargeHeap { limit: self.limit, heap: BinaryHeap::new() };
        never_clone(shade_codec(heap))
    }
}

struct Item<D, C: CompareFunction<D>> {
    inner: D,
    cmp: NonNull<C>,
}

impl<D: Debug, C: CompareFunction<D>> Debug for Item<D, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Item={:?}", self.inner)
    }
}

impl<D, C: CompareFunction<D>> PartialEq for Item<D, C> {
    fn eq(&self, other: &Self) -> bool {
        let left = &self.inner;
        unsafe { self.cmp.as_ref().compare(left, &other.inner) == Ordering::Equal }
    }
}

impl<D, C: CompareFunction<D>> PartialEq<D> for Item<D, C> {
    fn eq(&self, other: &D) -> bool {
        let left = &self.inner;
        unsafe { self.cmp.as_ref().compare(left, other) == Ordering::Equal }
    }
}

impl<D, C: CompareFunction<D>> Eq for Item<D, C> {}

impl<D, C: CompareFunction<D>> PartialOrd for Item<D, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let left = &self.inner;
        unsafe { Some(self.cmp.as_ref().compare(left, &other.inner)) }
    }
}

impl<D, C: CompareFunction<D>> Ord for Item<D, C> {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = &self.inner;
        unsafe { self.cmp.as_ref().compare(left, &other.inner) }
    }
}

impl<D, C: CompareFunction<D>> PartialOrd<D> for Item<D, C> {
    fn partial_cmp(&self, other: &D) -> Option<Ordering> {
        let left = &self.inner;
        unsafe { Some(self.cmp.as_ref().compare(left, other)) }
    }
}

unsafe impl<D: Send, C: CompareFunction<D>> Send for Item<D, C> {}

struct OrdParam<D, C: CompareFunction<D>> {
    limit: usize,
    cmp: NonNull<C>,
    ref_count: Arc<AtomicUsize>,
    _ph: std::marker::PhantomData<D>,
}

impl<D, C: CompareFunction<D>> OrdParam<D, C> {
    pub fn new(limit: usize, cmp: Box<C>) -> Self {
        let cmp = unsafe { NonNull::new_unchecked(Box::into_raw(cmp)) };
        OrdParam {
            limit,
            cmp,
            ref_count: Arc::new(AtomicUsize::new(1)),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D, C: CompareFunction<D>> Clone for OrdParam<D, C> {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, SeqCst);
        OrdParam {
            limit: self.limit,
            cmp: self.cmp,
            ref_count: self.ref_count.clone(),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D, C: CompareFunction<D>> Drop for OrdParam<D, C> {
    fn drop(&mut self) {
        let before_sub = self.ref_count.fetch_sub(1, SeqCst);
        if before_sub == 1 {
            let ptr = self.cmp.as_ptr();
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

//
unsafe impl<D: Send, C: CompareFunction<D> + Send> Send for OrdParam<D, C> {}

struct CustomOrdQueue<D, C: CompareFunction<D>> {
    heap: BinaryHeap<Item<D, C>>,
    param: OrdParam<D, C>,
}

impl<D, C: CompareFunction<D>> IntoIterator for CustomOrdQueue<D, C> {
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

impl<D: Send, C: CompareFunction<D>> Collection<D> for CustomOrdQueue<D, C> {
    fn add(&mut self, item: D) -> Result<(), io::Error> {
        if self.heap.len() >= self.param.limit {
            let mut head = self.heap.peek_mut().expect("unreachable: len > 0");
            // if others <= head > item,
            if Some(Ordering::Greater) == head.partial_cmp(&item) {
                head.inner = item;
            }
        } else {
            let cmp = self.param.cmp;
            self.heap.push(Item { inner: item, cmp });
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.heap.clear();
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}

impl<D: Debug, C: CompareFunction<D>> Debug for CustomOrdQueue<D, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "custom priority queue: {:?} ", self.heap)
    }
}

struct CustomOrdQueueFactory<D, C: CompareFunction<D>> {
    param: OrdParam<D, C>,
}

impl<D, C: CompareFunction<D>> CustomOrdQueueFactory<D, C> {
    pub fn new(param: OrdParam<D, C>) -> Self {
        CustomOrdQueueFactory { param }
    }
}

impl<D: Send, C: CompareFunction<D>> CollectionFactory<D> for CustomOrdQueueFactory<D, C> {
    type Target = NeverClone<ShadeCodec<CustomOrdQueue<D, C>>>;

    fn create(&self) -> Self::Target {
        let queue = CustomOrdQueue { heap: BinaryHeap::new(), param: self.param.clone() };
        never_clone(shade_codec(queue))
    }
}
