//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use ir_common::generated::algebra::join::JoinKind;
use pegasus::api::function::FnResult;
use std::cmp::Ordering;

pub trait CompareFunction<D>: Send + 'static {
    fn compare(&self, left: &D, right: &D) -> Ordering;
}

pub trait KeyFunction<D, K, V>: Send + 'static {
    fn select_key(&self, input: D) -> FnResult<(K, V)>;
}

pub trait JoinFunction<D, K, V>: Send + 'static {
    fn left_key(&self) -> FnResult<Box<dyn KeyFunction<D, K, V>>>;

    fn right_key(&self) -> FnResult<Box<dyn KeyFunction<D, K, V>>>;

    fn get_join_kind(&self) -> JoinKind;
}

///
/// Function impls for Box<T>, Box<dyn T> if T impls some function;
///
mod box_impl {
    use super::*;

    impl<D, F: CompareFunction<D> + ?Sized> CompareFunction<D> for Box<F> {
        fn compare(&self, left: &D, right: &D) -> Ordering {
            (**self).compare(left, right)
        }
    }

    impl<D, K, V, F: KeyFunction<D, K, V> + ?Sized> KeyFunction<D, K, V> for Box<F> {
        fn select_key(&self, input: D) -> FnResult<(K, V)> {
            (**self).select_key(input)
        }
    }

    impl<D, K, V, F: JoinFunction<D, K, V> + ?Sized> JoinFunction<D, K, V> for Box<F> {
        fn left_key(&self) -> FnResult<Box<dyn KeyFunction<D, K, V>>> {
            (**self).left_key()
        }

        fn right_key(&self) -> FnResult<Box<dyn KeyFunction<D, K, V>>> {
            (**self).right_key()
        }

        fn get_join_kind(&self) -> JoinKind {
            (**self).get_join_kind()
        }
    }
}
