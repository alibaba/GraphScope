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

use crate::data::MicroBatch;
use crate::Data;

///
/// Here contains abstractions of all user defined functions;
///

pub type DynError = Box<dyn std::error::Error + Send>;
pub type DynIter<D> = Box<dyn Iterator<Item = D> + Send>;
pub type FnResult<T> = Result<T, Box<dyn std::error::Error + Send>>;

pub trait RouteFunction<D>: Send + 'static {
    fn route(&self, data: &D) -> FnResult<u64>;
}

pub trait BatchRouteFunction<D: Data>: Send + 'static {
    fn route(&self, batch: &MicroBatch<D>) -> FnResult<u64>;
}

pub trait MapFunction<I, O>: Send + 'static {
    fn exec(&self, input: I) -> FnResult<O>;
}

pub trait FlatMapFunction<I, O>: Send + 'static {
    type Target: Iterator<Item = O> + Send + 'static;

    fn exec(&self, input: I) -> FnResult<Self::Target>;
}

pub trait FilterFunction<D>: Send + 'static {
    fn test(&self, input: &D) -> FnResult<bool>;
}

pub trait MutFilterFunction<D>: Send + 'static {
    fn test(&mut self, input: &D) -> FnResult<bool>;
}

pub trait BinaryFunction<L, R, O>: Send + 'static {
    fn exec(&self, left: L, right: R) -> FnResult<O>;
}

pub trait BinaryLeftMutFunction<L, R, O>: Send + 'static {
    fn exec(&self, left: &mut L, right: R) -> FnResult<O>;
}

///
/// Function impls for Box<T>, Box<dyn T> if T impls some function;
///
mod box_impl {
    use super::*;

    impl<D, R: RouteFunction<D> + ?Sized> RouteFunction<D> for Box<R> {
        fn route(&self, data: &D) -> FnResult<u64> {
            (**self).route(data)
        }
    }

    impl<D: Data, R: BatchRouteFunction<D> + ?Sized> BatchRouteFunction<D> for Box<R> {
        fn route(&self, batch: &MicroBatch<D>) -> FnResult<u64> {
            (**self).route(batch)
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
        fn test(&self, input: &D) -> FnResult<bool> {
            (**self).test(input)
        }
    }

    impl<L, R, O, F: BinaryFunction<L, R, O> + ?Sized> BinaryFunction<L, R, O> for Box<F> {
        fn exec(&self, left: L, right: R) -> FnResult<O> {
            (**self).exec(left, right)
        }
    }

    impl<L, R, O, F: BinaryLeftMutFunction<L, R, O> + ?Sized> BinaryLeftMutFunction<L, R, O> for Box<F> {
        fn exec(&self, left: &mut L, right: R) -> FnResult<O> {
            (**self).exec(left, right)
        }
    }
}
