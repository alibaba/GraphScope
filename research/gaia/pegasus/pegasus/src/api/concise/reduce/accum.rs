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
use crate::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::collections::{Collection, CollectionFactory};
use pegasus_common::downcast::{Any, AsAny};
use pegasus_common::rc::RcPointer;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::io::Error;

pub trait Accumulator<I>: Send + Debug + AsAny {
    fn accum(&mut self, next: I) -> Result<(), io::Error>;
}

pub trait AccumFactory<I>: Send {
    type Target: Accumulator<I>;

    fn create(&self) -> Self::Target;

    fn is_associative(&self) -> bool {
        false
    }
}

impl<I, A: Accumulator<I> + ?Sized> Accumulator<I> for Box<A> {
    fn accum(&mut self, next: I) -> Result<(), io::Error> {
        (**self).accum(next)
    }
}

impl<I, A: AccumFactory<I> + ?Sized> AccumFactory<I> for Box<A> {
    type Target = A::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }

    fn is_associative(&self) -> bool {
        (**self).is_associative()
    }
}

impl<I, A: AccumFactory<I>> AccumFactory<I> for RcPointer<A> {
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
    _ph: std::marker::PhantomData<D>,
}

impl<D> Debug for Count<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "count={}", self.value)
    }
}

impl<D: Send + 'static> Accumulator<D> for Count<D> {
    fn accum(&mut self, _next: D) -> Result<(), io::Error> {
        self.value += 1;
        Ok(())
    }
}

impl<D: 'static> AsAny for Count<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
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

#[derive(Clone, Eq, PartialEq)]
pub struct ToList<D> {
    pub inner: Vec<D>,
}

impl<D: Debug> Debug for ToList<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}
impl<D: Debug + Send + 'static> Accumulator<D> for ToList<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.inner.push(next);
        Ok(())
    }
}

impl<D: 'static> AsAny for ToList<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
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

impl<D> ToListAccum<D> {
    pub fn new() -> Self {
        ToListAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ToListAccum { capacity, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send + 'static> AccumFactory<D> for ToListAccum<D> {
    type Target = ToList<D>;

    fn create(&self) -> Self::Target {
        if self.capacity > 0 {
            ToList { inner: Vec::with_capacity(self.capacity) }
        } else {
            ToList { inner: vec![] }
        }
    }
}

pub struct ToVecAccum<D> {
    capacity: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D> ToVecAccum<D> {
    pub fn new() -> Self {
        ToVecAccum { capacity: 0, _ph: std::marker::PhantomData }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ToVecAccum { capacity, _ph: std::marker::PhantomData }
    }
}

impl<D: Debug + Send + AsAny + 'static> AccumFactory<D> for ToVecAccum<D> {
    type Target = Vec<D>;

    fn create(&self) -> Self::Target {
        if self.capacity > 0 {
            Vec::with_capacity(self.capacity)
        } else {
            vec![]
        }
    }
}

impl<D: Debug + Send + AsAny> Accumulator<D> for Vec<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.push(next);
        Ok(())
    }
}

impl<D> Accumulator<D> for u64 {
    fn accum(&mut self, _: D) -> Result<(), Error> {
        *self = *self + 1u64;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ToSet<D: Eq + Hash + Debug> {
    pub inner: HashSet<D>,
}

impl<D: Eq + Hash + Debug + Send + 'static> Accumulator<D> for ToSet<D> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.inner.insert(next);
        Ok(())
    }
}

impl<D: Eq + Hash + Debug + 'static> AsAny for ToSet<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
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

impl<D: Send + 'static> AccumFactory<D> for CountAccum<D> {
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

impl<D: Eq + Hash + Debug + Send + 'static> AccumFactory<D> for HashSetAccum<D> {
    type Target = ToSet<D>;

    fn create(&self) -> Self::Target {
        ToSet { inner: HashSet::with_capacity(self.capacity) }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct Maximum<D, P: CompareFunction<D>> {
    pub max: Option<D>,
    cmp: RcPointer<P>,
}

impl<D: Debug, P: CompareFunction<D>> Debug for Maximum<D, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "max={:?}", self.max)
    }
}

unsafe impl<D: Send, P: CompareFunction<D>> Send for Maximum<D, P> {}

impl<D: Debug + Send + 'static, P: CompareFunction<D>> Accumulator<D> for Maximum<D, P> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
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
        Ok(())
    }
}

impl<D: 'static, P: CompareFunction<D>> AsAny for Maximum<D, P> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
    }
}

pub struct Minimum<D, P: CompareFunction<D>> {
    pub min: Option<D>,
    cmp: RcPointer<P>,
}

impl<D: Debug, P: CompareFunction<D>> Debug for Minimum<D, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "min={:?}", self.min)
    }
}

unsafe impl<D: Send, P: CompareFunction<D>> Send for Minimum<D, P> {}

impl<D: Debug + Send + 'static, P: CompareFunction<D>> Accumulator<D> for Minimum<D, P> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
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
        Ok(())
    }
}

impl<D: 'static, P: CompareFunction<D>> AsAny for Minimum<D, P> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
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

impl<D: Debug + Send + 'static, P: CompareFunction<D>> AccumFactory<D> for MaxAccum<D, P> {
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

impl<D: Debug + Send + 'static, P: CompareFunction<D>> AccumFactory<D> for MinAccum<D, P> {
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
    pub seed: Option<D>,
    add_func: RcPointer<A>,
}

impl<D: Debug, A: SumFunction<D>> Debug for DataSum<D, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sum={:?}", self.seed)
    }
}

impl<D: Send + Debug + 'static, A: SumFunction<D>> Accumulator<D> for DataSum<D, A> {
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        if let Some(ref mut seed) = self.seed {
            self.add_func.add(seed, next)
        } else {
            self.seed = Some(next);
        }
        Ok(())
    }
}

impl<D: 'static, A: SumFunction<D>> AsAny for DataSum<D, A> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
    }
}

pub struct DataSumAccum<D, A: SumFunction<D>> {
    add_func: RcPointer<A>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Debug + 'static, A: SumFunction<D>> AccumFactory<D> for DataSumAccum<D, A> {
    type Target = DataSum<D, A>;

    fn create(&self) -> Self::Target {
        DataSum { seed: None, add_func: self.add_func.clone() }
    }

    fn is_associative(&self) -> bool {
        true
    }
}

pub struct ToCollection<D: Send, C: Collection<D>> {
    pub collect: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Debug, C: Collection<D> + Debug> Debug for ToCollection<D, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "collection={:?}", self.collect)
    }
}

impl<D: 'static, C: 'static> Accumulator<D> for ToCollection<D, C>
where
    D: Send + Debug,
    C: Collection<D> + Debug + Default + IntoIterator<Item = D>,
{
    fn accum(&mut self, next: D) -> Result<(), io::Error> {
        self.collect.add(next)
    }
}

impl<D: Send + 'static, C: Collection<D> + 'static> AsAny for ToCollection<D, C> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        unimplemented!()
    }

    fn as_any_ref(&self) -> &dyn Any {
        unimplemented!()
    }
}

pub struct ToCollectionAccum<D, C> {
    factory: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D, C> AccumFactory<D> for ToCollectionAccum<D, C>
where
    D: Send + Debug + 'static,
    C: CollectionFactory<D>,
    C::Target: Debug + Default + IntoIterator<Item = D> + 'static,
{
    type Target = ToCollection<D, C::Target>;

    fn create(&self) -> Self::Target {
        let collect = self.factory.create();
        ToCollection { collect, _ph: std::marker::PhantomData }
    }
}
