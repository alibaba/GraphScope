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

use crate::api::function::{CompareFunction, SumFunction};
use pegasus_common::collections::{Collection, CollectionFactory};
use pegasus_common::rc::RcPointer;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub trait Accumulator<I, O>: Send + Debug {
    fn accum(&mut self, next: I);

    fn merge(&mut self, other: O);

    fn finalize(&mut self) -> O;
}

pub trait AccumFactory<I, O>: Send {
    type Target: Accumulator<I, O>;

    fn create(&self) -> Self::Target;

    fn is_associative(&self) -> bool {
        false
    }
}

impl<I, O, A: Accumulator<I, O> + ?Sized> Accumulator<I, O> for Box<A> {
    fn accum(&mut self, next: I) {
        (**self).accum(next);
    }

    fn merge(&mut self, other: O) {
        (**self).merge(other)
    }

    fn finalize(&mut self) -> O {
        (**self).finalize()
    }
}

impl<I, O, A: AccumFactory<I, O> + ?Sized> AccumFactory<I, O> for Box<A> {
    type Target = A::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }

    fn is_associative(&self) -> bool {
        (**self).is_associative()
    }
}

impl<I, O, A: AccumFactory<I, O>> AccumFactory<I, O> for RcPointer<A> {
    type Target = A::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }

    fn is_associative(&self) -> bool {
        (**self).is_associative()
    }
}

pub struct Count<D> {
    value: u64,
    _ph: std::marker::PhantomData<D>,
}

impl<D> Debug for Count<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "count={}", self.value)
    }
}

impl<D: Send> Accumulator<D, u64> for Count<D> {
    fn accum(&mut self, _next: D) {
        self.value += 1;
    }

    fn merge(&mut self, other: u64) {
        self.value += other;
    }

    fn finalize(&mut self) -> u64 {
        let value = self.value;
        self.value = 0;
        value
    }
}

pub struct ToList<D> {
    inner: Vec<D>,
}

impl<D: Debug> Debug for ToList<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}
impl<D: Debug + Send> Accumulator<D, Vec<D>> for ToList<D> {
    fn accum(&mut self, next: D) {
        self.inner.push(next);
    }

    fn merge(&mut self, other: Vec<D>) {
        self.inner.extend(other)
    }

    fn finalize(&mut self) -> Vec<D> {
        std::mem::replace(&mut self.inner, vec![])
    }
}

pub struct ToListAccum<D> {
    capacity: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D> ToListAccum<D> {
    pub fn new() -> Self {
        ToListAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ToListAccum { capacity, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send> AccumFactory<D, Vec<D>> for ToListAccum<D> {
    type Target = ToList<D>;

    fn create(&self) -> Self::Target {
        if self.capacity > 0 {
            ToList { inner: Vec::with_capacity(self.capacity) }
        } else {
            ToList { inner: vec![] }
        }
    }
}

#[derive(Debug)]
pub struct ToSet<D: Eq + Hash + Debug> {
    inner: HashSet<D>,
}

impl<D: Eq + Hash + Debug + Send> Accumulator<D, Vec<D>> for ToSet<D> {
    fn accum(&mut self, next: D) {
        self.inner.insert(next);
    }

    fn merge(&mut self, other: Vec<D>) {
        for item in other {
            self.inner.insert(item);
        }
    }

    fn finalize(&mut self) -> Vec<D> {
        let mut result = Vec::with_capacity(self.inner.len());
        let set = std::mem::replace(&mut self.inner, HashSet::new());
        result.extend(set.into_iter());
        result
    }
}

pub struct CountAccum<D> {
    _ph: std::marker::PhantomData<D>,
}

impl<D> CountAccum<D> {
    pub fn new() -> Self {
        CountAccum { _ph: std::marker::PhantomData }
    }
}

impl<D: Send> AccumFactory<D, u64> for CountAccum<D> {
    type Target = Count<D>;

    fn create(&self) -> Self::Target {
        Count { value: 0, _ph: std::marker::PhantomData }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct HashSetAccum<D: Eq + Hash> {
    capacity: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Eq + Hash> HashSetAccum<D> {
    pub fn new() -> Self {
        HashSetAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(cap: usize) -> Self {
        HashSetAccum { capacity: cap, _ph: std::marker::PhantomData }
    }
}

impl<D: Eq + Hash + Debug + Send> AccumFactory<D, Vec<D>> for HashSetAccum<D> {
    type Target = ToSet<D>;

    fn create(&self) -> Self::Target {
        ToSet { inner: HashSet::with_capacity(self.capacity) }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct Maximum<D, P: CompareFunction<D>> {
    max: Option<D>,
    cmp: RcPointer<P>,
}

impl<D: Debug, P: CompareFunction<D>> Debug for Maximum<D, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "max={:?}", self.max)
    }
}

unsafe impl<D: Send, P: CompareFunction<D>> Send for Maximum<D, P> {}

impl<D: Debug + Send, P: CompareFunction<D>> Accumulator<D, Option<D>> for Maximum<D, P> {
    fn accum(&mut self, next: D) {
        if let Some(pre) = self.max.take() {
            match self.cmp.compare(&pre, &next) {
                Ordering::Less => {
                    self.max = Some(next);
                }
                Ordering::Equal => self.max = Some(pre),
                Ordering::Greater => self.max = Some(pre),
            }
        } else {
            self.max = Some(next);
        }
    }

    fn merge(&mut self, other: Option<D>) {
        if let Some(item) = other {
            self.accum(item)
        }
    }

    fn finalize(&mut self) -> Option<D> {
        self.max.take()
    }
}

pub struct Minimum<D, P: CompareFunction<D>> {
    min: Option<D>,
    cmp: RcPointer<P>,
}

impl<D: Debug, P: CompareFunction<D>> Debug for Minimum<D, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "min={:?}", self.min)
    }
}

unsafe impl<D: Send, P: CompareFunction<D>> Send for Minimum<D, P> {}

impl<D: Debug + Send, P: CompareFunction<D>> Accumulator<D, Option<D>> for Minimum<D, P> {
    fn accum(&mut self, next: D) {
        if let Some(pre) = self.min.take() {
            match self.cmp.compare(&pre, &next) {
                Ordering::Less => {
                    self.min = Some(pre);
                }
                Ordering::Equal => self.min = Some(pre),
                Ordering::Greater => self.min = Some(next),
            }
        } else {
            self.min = Some(next);
        }
    }

    fn merge(&mut self, other: Option<D>) {
        if let Some(item) = other {
            self.accum(item)
        }
    }

    fn finalize(&mut self) -> Option<D> {
        self.min.take()
    }
}

pub struct MaxAccum<D, P: CompareFunction<D>> {
    cmp: RcPointer<P>,
    _ph: std::marker::PhantomData<D>,
}

impl<D, P: CompareFunction<D>> MaxAccum<D, P> {
    pub fn new(cmp: P) -> Self {
        let cmp = RcPointer::new(cmp);
        MaxAccum { cmp, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send, P: CompareFunction<D>> AccumFactory<D, Option<D>> for MaxAccum<D, P> {
    type Target = Maximum<D, P>;

    fn create(&self) -> Self::Target {
        let cmp = self.cmp.clone();
        Maximum { max: None, cmp }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct MinAccum<D, P: CompareFunction<D>> {
    cmp: RcPointer<P>,
    _ph: std::marker::PhantomData<D>,
}

impl<D, P: CompareFunction<D>> MinAccum<D, P> {
    pub fn new(cmp: P) -> Self {
        let cmp = RcPointer::new(cmp);
        MinAccum { cmp, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send, P: CompareFunction<D>> AccumFactory<D, Option<D>> for MinAccum<D, P> {
    type Target = Minimum<D, P>;

    fn create(&self) -> Self::Target {
        let cmp = self.cmp.clone();
        Minimum { min: None, cmp }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct DataSum<D, A: SumFunction<D>> {
    seed: Option<D>,
    add_func: RcPointer<A>,
}

impl<D: Debug, A: SumFunction<D>> Debug for DataSum<D, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sum={:?}", self.seed)
    }
}

impl<D: Send + Debug, A: SumFunction<D>> Accumulator<D, Option<D>> for DataSum<D, A> {
    fn accum(&mut self, next: D) {
        if let Some(ref mut seed) = self.seed {
            self.add_func.add(seed, next)
        } else {
            self.seed = Some(next);
        }
    }

    fn merge(&mut self, other: Option<D>) {
        if let Some(other) = other {
            self.accum(other)
        }
    }

    fn finalize(&mut self) -> Option<D> {
        self.seed.take()
    }
}

pub struct DataSumAccum<D, A: SumFunction<D>> {
    add_func: RcPointer<A>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Debug, A: SumFunction<D>> AccumFactory<D, Option<D>> for DataSumAccum<D, A> {
    type Target = DataSum<D, A>;

    fn create(&self) -> Self::Target {
        DataSum { seed: None, add_func: self.add_func.clone() }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct ToCollection<D: Send, C: Collection<D>> {
    collect: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Debug, C: Collection<D> + Debug> Debug for ToCollection<D, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "collection={:?}", self.collect)
    }
}

impl<D, C> Accumulator<D, C> for ToCollection<D, C>
where
    D: Send + Debug,
    C: Collection<D> + Debug + Default + IntoIterator<Item = D>,
{
    fn accum(&mut self, next: D) {
        self.collect.add(next);
    }

    fn merge(&mut self, other: C) {
        for item in other.into_iter() {
            self.collect.add(item);
        }
    }

    fn finalize(&mut self) -> C {
        std::mem::replace(&mut self.collect, C::default())
    }
}

pub struct ToCollectionAccum<D, C> {
    factory: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D, C> AccumFactory<D, C::Target> for ToCollectionAccum<D, C>
where
    D: Send + Debug,
    C: CollectionFactory<D>,
    C::Target: Debug + Default + IntoIterator<Item = D>,
{
    type Target = ToCollection<D, C::Target>;

    fn create(&self) -> Self::Target {
        let collect = self.factory.create();
        ToCollection { collect, _ph: std::marker::PhantomData }
    }
}
