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

use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::rc::RcPointer;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::ops::Add;

pub trait Accumulator<I, O>: Send + Debug {
    fn accum(&mut self, next: I) -> Result<(), io::Error>;

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
    fn accum(&mut self, next: I) -> Result<(), io::Error> {
        (**self).accum(next)
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

#[derive(Clone, Eq, PartialEq)]
pub struct Count<D> {
    pub value: u64,
    pub _ph: std::marker::PhantomData<D>,
}

impl<D> Debug for Count<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "count={}", self.value)
    }
}

impl<D: Send + 'static> Accumulator<D, u64> for Count<D> {
    fn accum(&mut self, _next: D) -> Result<(), io::Error> {
        self.value += 1;
        Ok(())
    }

    fn finalize(&mut self) -> u64 {
        let value = self.value;
        self.value = 0;
        value
    }
}

impl<D> Encode for Count<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.value.write_to(writer)?;
        Ok(())
    }
}

impl<D> Decode for Count<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let value = u64::read_from(reader)?;
        Ok(Count { value, _ph: std::marker::PhantomData })
    }
}

pub struct CountAccum<D> {
    _ph: std::marker::PhantomData<D>,
}

#[allow(dead_code)]
impl<D> CountAccum<D> {
    pub fn new() -> Self {
        CountAccum { _ph: std::marker::PhantomData }
    }
}

impl<D: Send + 'static> AccumFactory<D, u64> for CountAccum<D> {
    type Target = Count<D>;

    fn create(&self) -> Self::Target {
        Count { value: 0, _ph: std::marker::PhantomData }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ToList<D> {
    pub inner: Vec<D>,
}

impl<D: Debug> Debug for ToList<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}
impl<D: Debug + Send + 'static> Accumulator<D, Vec<D>> for ToList<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.inner.push(next);
        Ok(())
    }

    fn finalize(&mut self) -> Vec<D> {
        std::mem::replace(&mut self.inner, vec![])
    }
}

impl<D: Encode> Encode for ToList<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.inner.write_to(writer)?;
        Ok(())
    }
}

impl<D: Decode> Decode for ToList<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let inner = <Vec<D>>::read_from(reader)?;
        Ok(ToList { inner })
    }
}

pub struct ToListAccum<D> {
    capacity: usize,
    _ph: std::marker::PhantomData<D>,
}

#[allow(dead_code)]
impl<D> ToListAccum<D> {
    pub fn new() -> Self {
        ToListAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ToListAccum { capacity, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send + 'static> AccumFactory<D, Vec<D>> for ToListAccum<D> {
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
    pub inner: HashSet<D>,
}

impl<D: Eq + Hash + Debug + Send + 'static> Accumulator<D, Vec<D>> for ToSet<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.inner.insert(next);
        Ok(())
    }

    fn finalize(&mut self) -> Vec<D> {
        let mut result = Vec::with_capacity(self.inner.len());
        let set = std::mem::replace(&mut self.inner, HashSet::new());
        result.extend(set.into_iter());
        result
    }
}

pub struct HashSetAccum<D: Eq + Hash> {
    capacity: usize,
    _ph: std::marker::PhantomData<D>,
}

#[allow(dead_code)]
impl<D: Eq + Hash> HashSetAccum<D> {
    pub fn new() -> Self {
        HashSetAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(cap: usize) -> Self {
        HashSetAccum { capacity: cap, _ph: std::marker::PhantomData }
    }
}

impl<D: Eq + Hash + Debug + Send + 'static> AccumFactory<D, Vec<D>> for HashSetAccum<D> {
    type Target = ToSet<D>;

    fn create(&self) -> Self::Target {
        ToSet { inner: HashSet::with_capacity(self.capacity) }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct Maximum<D> {
    pub max: Option<D>,
}

impl<D: Debug> Debug for Maximum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "max={:?}", self.max)
    }
}

unsafe impl<D: Send> Send for Maximum<D> {}

impl<D: Debug + Send + Ord + 'static> Accumulator<D, Option<D>> for Maximum<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        if let Some(pre) = self.max.take() {
            match &pre.cmp(&next) {
                Ordering::Less => {
                    self.max = Some(next);
                }
                Ordering::Equal => self.max = Some(pre),
                Ordering::Greater => self.max = Some(pre),
            }
        } else {
            self.max = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.max.take()
    }
}

pub struct Minimum<D> {
    pub min: Option<D>,
}

impl<D: Debug> Debug for Minimum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "min={:?}", self.min)
    }
}

unsafe impl<D: Send> Send for Minimum<D> {}

impl<D: Debug + Send + Ord + 'static> Accumulator<D, Option<D>> for Minimum<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        if let Some(pre) = self.min.take() {
            match &pre.cmp(&next) {
                Ordering::Less => {
                    self.min = Some(pre);
                }
                Ordering::Equal => self.min = Some(pre),
                Ordering::Greater => self.min = Some(next),
            }
        } else {
            self.min = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.min.take()
    }
}

pub struct MaxAccum<D> {
    _ph: std::marker::PhantomData<D>,
}

#[allow(dead_code)]
impl<D> MaxAccum<D> {
    pub fn new() -> Self {
        MaxAccum { _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send + Ord + 'static> AccumFactory<D, Option<D>> for MaxAccum<D> {
    type Target = Maximum<D>;

    fn create(&self) -> Self::Target {
        Maximum { max: None }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct MinAccum<D> {
    _ph: std::marker::PhantomData<D>,
}

#[allow(dead_code)]
impl<D> MinAccum<D> {
    pub fn new() -> Self {
        MinAccum { _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send + Ord + 'static> AccumFactory<D, Option<D>> for MinAccum<D> {
    type Target = Minimum<D>;

    fn create(&self) -> Self::Target {
        Minimum { min: None }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct DataSum<D> {
    pub seed: Option<D>,
}

impl<D: Debug> Debug for DataSum<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sum={:?}", self.seed)
    }
}

impl<D: Send + Debug + Add<Output = D> + 'static> Accumulator<D, Option<D>> for DataSum<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        if let Some(seed) = self.seed.take() {
            self.seed = Some(seed.add(next));
        } else {
            self.seed = Some(next);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Option<D> {
        self.seed.take()
    }
}

pub struct DataSumAccum<D> {
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Debug + Add<Output = D> + 'static> AccumFactory<D, Option<D>> for DataSumAccum<D> {
    type Target = DataSum<D>;

    fn create(&self) -> Self::Target {
        DataSum { seed: None }
    }

    fn is_associative(&self) -> bool {
        true
    }
}
