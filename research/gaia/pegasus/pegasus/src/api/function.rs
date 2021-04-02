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

use crate::Data;
use std::cmp::Ordering;
use std::sync::Arc;

///
/// Here contains abstractions of all user defined functions;
///

pub type DynError = Box<dyn std::error::Error + Send>;
pub type DynIter<D> = Box<dyn Iterator<Item = Result<D, DynError>> + Send>;

pub type FnResult<T> = Result<T, Box<dyn std::error::Error + Send>>;

pub trait RouteFunction<D>: Send + 'static {
    fn route(&self, data: &D) -> FnResult<u64>;
}

pub trait MultiRouteFunction<D>: Send + 'static {
    fn route(&self, data: &D) -> FnResult<&[u64]>;
}

pub trait Partition {
    fn get_partition(&self) -> FnResult<u64>;
}

macro_rules! impl_partition {
    ($ty: ty) => {
        impl Partition for $ty {
            fn get_partition(&self) -> FnResult<u64> {
                Ok(*self as u64)
            }
        }
    };
}

impl_partition!(u32);
impl_partition!(u64);

/// The user defined function used to extract a key from origin data
pub trait Keyed {
    type Key;
    type Value;

    fn get_key(&self) -> FnResult<&Self::Key>;

    fn take_key(&mut self) -> FnResult<Self::Key>;

    fn take_value(&mut self) -> FnResult<Self::Value>;
}

macro_rules! impl_keyed {
    ($ty: ty) => {
        impl Keyed for $ty {
            type Key = $ty;
            type Value = $ty;

            fn get_key(&self) -> FnResult<&Self::Key> {
                Ok(self)
            }

            fn take_key(&mut self) -> FnResult<Self::Key> {
                Ok(*self)
            }

            fn take_value(&mut self) -> FnResult<Self::Value> {
                Ok(*self)
            }
        }
    };
}

impl_keyed!(u32);

impl<K, V> Keyed for Pair<K, V> {
    type Key = K;
    type Value = V;

    fn get_key(&self) -> FnResult<&Self::Key> {
        if let Some(k) = &self.0 {
            Ok(k)
        } else {
            let err: Box<dyn std::error::Error + Send + Sync> =
                "get_key failed: key of pair lost".into();
            Err(err)
        }
    }

    fn take_key(&mut self) -> FnResult<Self::Key> {
        if let Some(k) = self.0.take() {
            Ok(k)
        } else {
            let err: Box<dyn std::error::Error + Send + Sync> =
                "take_key failed: key of pair is lost".into();
            Err(err)
        }
    }

    fn take_value(&mut self) -> FnResult<Self::Value> {
        if let Some(k) = self.1.take() {
            Ok(k)
        } else {
            let err: Box<dyn std::error::Error + Send + Sync> =
                "take_value failed: value of pair is lost".into();
            Err(err)
        }
    }
}

pub trait KeyFunction<D>: Send + 'static {
    type Key;

    fn select_key(&self, item: &D) -> FnResult<Self::Key>;
}

pub type Pair<K, V> = (Option<K>, Option<V>);

pub trait SumFunction<D>: Send + 'static {
    fn add(&self, seed: &mut D, next: D);
}

pub trait CompareFunction<D>: Send + 'static {
    fn compare(&self, left: &D, right: &D) -> Ordering;
}

pub trait MapFunction<I, O>: Send + 'static {
    fn exec(&self, input: I) -> FnResult<O>;
}

pub trait FlatMapFunction<I, O>: Send + 'static {
    type Target: Iterator<Item = Result<O, DynError>> + Send + 'static;

    fn exec(&self, input: I) -> FnResult<Self::Target>;
}

pub trait FilterFunction<D>: Send + 'static {
    fn exec(&self, input: &D) -> FnResult<bool>;
}

pub trait LeftJoinFunction<D>: Send + 'static {
    fn exec(&self, left: &D, right: D) -> Option<D>;
}

pub trait EncodeFunction<D>: Send + 'static {
    fn encode(&self, data: Vec<D>) -> Vec<u8>;
}

///
/// Function impls for Box<T>, Box<dyn T> if T impls some function;
///

impl<D: Data, R: RouteFunction<D> + ?Sized> RouteFunction<D> for Box<R> {
    fn route(&self, data: &D) -> FnResult<u64> {
        (**self).route(data)
    }
}

impl<D: Data, T: MultiRouteFunction<D> + ?Sized> MultiRouteFunction<D> for Box<T> {
    fn route(&self, data: &D) -> FnResult<&[u64]> {
        (**self).route(data)
    }
}

impl<D, K: KeyFunction<D> + ?Sized> KeyFunction<D> for Box<K> {
    type Key = K::Key;

    fn select_key(&self, item: &D) -> FnResult<Self::Key> {
        (**self).select_key(item)
    }
}

impl<D, S: SumFunction<D> + ?Sized> SumFunction<D> for Box<S> {
    fn add(&self, seed: &mut D, next: D) {
        (**self).add(seed, next)
    }
}

impl<D, C: CompareFunction<D> + ?Sized> CompareFunction<D> for Box<C> {
    fn compare(&self, left: &D, right: &D) -> Ordering {
        (**self).compare(left, right)
    }
}

impl<I, O, M: MapFunction<I, O> + ?Sized> MapFunction<I, O> for Box<M> {
    fn exec(&self, input: I) -> FnResult<O> {
        (**self).exec(input)
    }
}

impl<I, O, M: FlatMapFunction<I, O> + ?Sized> FlatMapFunction<I, O> for Box<M> {
    type Target = M::Target;

    fn exec(&self, input: I) -> FnResult<Self::Target> {
        (**self).exec(input)
    }
}

impl<D, F: FilterFunction<D> + ?Sized> FilterFunction<D> for Box<F> {
    fn exec(&self, input: &D) -> FnResult<bool> {
        (**self).exec(input)
    }
}

impl<D, L: LeftJoinFunction<D> + ?Sized> LeftJoinFunction<D> for Box<L> {
    fn exec(&self, left: &D, right: D) -> Option<D> {
        (**self).exec(left, right)
    }
}

impl<D, E: EncodeFunction<D> + ?Sized> EncodeFunction<D> for Box<E> {
    fn encode(&self, data: Vec<D>) -> Vec<u8> {
        (**self).encode(data)
    }
}

///
/// Function impls for Arc<T>, Arc<dyn T> if T impls some function;
///

impl<D: Data, T: LeftJoinFunction<D> + Sync + ?Sized> LeftJoinFunction<D> for Arc<T> {
    fn exec(&self, parent: &D, sub: D) -> Option<D> {
        (**self).exec(parent, sub)
    }
}

/// impl functions for closure;
/// Because of conflict implementation with functions impl for box, we compromised by using a
/// named structure to wrap the closure, and use macros for easy use;
///

pub struct RouteClosure<D, F: Fn(&D) -> u64> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D, F: Fn(&D) -> u64> RouteClosure<D, F> {
    pub fn new(func: F) -> Self {
        RouteClosure { func, _ph: std::marker::PhantomData }
    }
}

impl<F, D> RouteFunction<D> for RouteClosure<D, F>
where
    D: Send + 'static,
    F: Fn(&D) -> u64 + Send + 'static,
{
    fn route(&self, data: &D) -> FnResult<u64> {
        Ok((self.func)(data))
    }
}

#[macro_export]
macro_rules! route {
    ($func: expr) => {
        RouteClosure::new($func)
    };
}

#[macro_export]
macro_rules! box_route {
    ($func: expr) => {
        Box::new(RouteClosure::new($func))
    };
}

// pub struct MultiRouteClosure<D, F: Fn(&D) -> &[u64]> {
//     func: F,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D, F> MultiRouteClosure<D, F>
// where
//     F: Fn(&D) -> &[u64],
// {
//     pub fn new(func: F) -> Self {
//         MultiRouteClosure { func, _ph: std::marker::PhantomData }
//     }
// }
//
// impl<D, F> MultiRouteFunction<D> for MultiRouteClosure<D, F>
// where
//     D: Send + 'static,
//     F: Fn(&D) -> &[u64] + Send + 'static,
// {
//     fn route(&self, data: &D) -> &[u64] {
//         (self.func)(data)
//     }
// }
//
// #[macro_export]
// macro_rules! multi_route {
//     ($func: expr) => {
//         MultiRouteClosure::new($func)
//     };
// }

pub struct MapClosure<I, O, F: Fn(I) -> FnResult<O>> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F: Fn(I) -> FnResult<O>> MapClosure<I, O, F> {
    pub fn new(func: F) -> Self {
        MapClosure { func, _ph: std::marker::PhantomData }
    }
}

impl<I, O, F> MapFunction<I, O> for MapClosure<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> FnResult<O> + Send + 'static,
{
    fn exec(&self, input: I) -> FnResult<O> {
        (self.func)(input)
    }
}

#[macro_export]
macro_rules! map {
    ($func: expr) => {
        MapClosure::new($func)
    };
}

pub struct FlatMapClosure<I, O, F, R> {
    func: F,
    _ph: std::marker::PhantomData<(I, O, R)>,
}

impl<I, O, F, R> FlatMapClosure<I, O, F, R>
where
    I: Send + 'static,
    O: Send + 'static,
    R: Iterator<Item = Result<O, DynError>> + Send + 'static,
    F: Fn(I) -> FnResult<R> + Send + 'static,
{
    pub fn new(func: F) -> Self {
        FlatMapClosure { func, _ph: std::marker::PhantomData }
    }
}

impl<I, O, F, R> FlatMapFunction<I, O> for FlatMapClosure<I, O, F, R>
where
    I: Send + 'static,
    O: Send + 'static,
    R: Iterator<Item = Result<O, DynError>> + Send + 'static,
    F: Fn(I) -> FnResult<R> + Send + 'static,
{
    type Target = R;

    fn exec(&self, input: I) -> FnResult<Self::Target> {
        (self.func)(input)
    }
}

#[macro_export]
macro_rules! flat_map {
    ($func: expr) => {
        FlatMapClosure::new($func)
    };
}

pub struct FilterClosure<D, F: Fn(&D) -> FnResult<bool>> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D, F> FilterClosure<D, F>
where
    F: Fn(&D) -> FnResult<bool>,
{
    pub fn new(func: F) -> Self {
        FilterClosure { func, _ph: std::marker::PhantomData }
    }
}

impl<D, F> FilterFunction<D> for FilterClosure<D, F>
where
    D: Send + 'static,
    F: Fn(&D) -> FnResult<bool> + Send + 'static,
{
    fn exec(&self, input: &D) -> FnResult<bool> {
        (self.func)(input)
    }
}

#[macro_export]
macro_rules! filter {
    ($func: expr) => {
        FilterClosure::new($func)
    };
}

pub struct CompareClosure<D, F: Fn(&D, &D) -> Ordering + Send> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + 'static, F> CompareFunction<D> for CompareClosure<D, F>
where
    F: Fn(&D, &D) -> Ordering + Send + 'static,
{
    fn compare(&self, left: &D, right: &D) -> Ordering {
        (self.func)(left, right)
    }
}

impl<D, F> CompareClosure<D, F>
where
    F: Fn(&D, &D) -> Ordering + Send,
{
    pub fn new(func: F) -> Self {
        CompareClosure { func, _ph: std::marker::PhantomData }
    }
}

#[macro_export]
macro_rules! compare {
    ($func: expr) => {
        CompareClosure::new($func)
    };
}

pub struct EncodeClosure<D, F> {
    func: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D, F> EncodeClosure<D, F>
where
    F: Fn(Vec<D>) -> Vec<u8>,
{
    pub fn new(func: F) -> Self {
        EncodeClosure { func, _ph: std::marker::PhantomData }
    }
}

impl<D, F> EncodeFunction<D> for EncodeClosure<D, F>
where
    D: Send + 'static,
    F: Fn(Vec<D>) -> Vec<u8> + Send + 'static,
{
    fn encode(&self, data: Vec<D>) -> Vec<u8> {
        (self.func)(data)
    }
}

#[macro_export]
macro_rules! encode {
    ($func: expr) => {
        EncodeClosure::new($func)
    };
}
