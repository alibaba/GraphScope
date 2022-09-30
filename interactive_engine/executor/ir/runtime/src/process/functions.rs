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

use std::cmp::Ordering;

use ir_common::generated::algebra::join::JoinKind;
use pegasus::api::function::{BinaryFunction, FnResult, MapFunction};
use pegasus_server::job_pb::AccumKind;

use crate::error::FnGenResult;
use crate::process::operator::accum::RecordAccumulator;

pub trait CompareFunction<D>: Send + 'static {
    fn compare(&self, left: &D, right: &D) -> Ordering;
}

pub trait KeyFunction<D, K, V>: Send + 'static {
    fn get_kv(&self, input: D) -> FnResult<(K, V)>;
}

pub trait JoinKeyGen<D, K, V>: Send + 'static {
    fn gen_left_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>>;

    fn gen_right_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>>;

    fn get_join_kind(&self) -> JoinKind;
}

pub trait GroupGen<D, K, V>: Send + 'static {
    fn gen_group_key(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>>;

    fn gen_group_accum(&self) -> FnGenResult<RecordAccumulator>;

    fn gen_group_map(&self) -> FnGenResult<Box<dyn MapFunction<(K, V), D>>>;
}

pub trait FoldGen<I, O>: Send + 'static {
    // TODO(bingqing): get_accum_kind() and gen_fold_map() is for simple count optimization for tmp;
    // This will be processed in gen_fold_accum() in a unified way later
    fn get_accum_kind(&self) -> AccumKind;

    fn gen_fold_map(&self) -> FnGenResult<Box<dyn MapFunction<I, O>>>;

    // TODO(bingqing): enable fold_partition + fold_global optimization in RecordAccumulator
    fn gen_fold_accum(&self) -> FnGenResult<RecordAccumulator>;
}

pub trait ApplyGen<L, R, O>: Send + 'static {
    fn get_join_kind(&self) -> JoinKind;

    fn gen_left_join_func(&self) -> FnGenResult<Box<dyn BinaryFunction<L, R, O>>>;
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
        fn get_kv(&self, input: D) -> FnResult<(K, V)> {
            (**self).get_kv(input)
        }
    }

    impl<D, K, V, F: JoinKeyGen<D, K, V> + ?Sized> JoinKeyGen<D, K, V> for Box<F> {
        fn gen_left_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>> {
            (**self).gen_left_kv_fn()
        }

        fn gen_right_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>> {
            (**self).gen_right_kv_fn()
        }

        fn get_join_kind(&self) -> JoinKind {
            (**self).get_join_kind()
        }
    }

    impl<D, K, V, F: GroupGen<D, K, V> + ?Sized> GroupGen<D, K, V> for Box<F> {
        fn gen_group_key(&self) -> FnGenResult<Box<dyn KeyFunction<D, K, V>>> {
            (**self).gen_group_key()
        }

        fn gen_group_accum(&self) -> FnGenResult<RecordAccumulator> {
            (**self).gen_group_accum()
        }

        fn gen_group_map(&self) -> FnGenResult<Box<dyn MapFunction<(K, V), D>>> {
            (**self).gen_group_map()
        }
    }

    impl<I, O, F: FoldGen<I, O> + ?Sized> FoldGen<I, O> for Box<F> {
        fn get_accum_kind(&self) -> AccumKind {
            (**self).get_accum_kind()
        }

        fn gen_fold_map(&self) -> FnGenResult<Box<dyn MapFunction<I, O>>> {
            (**self).gen_fold_map()
        }

        fn gen_fold_accum(&self) -> FnGenResult<RecordAccumulator> {
            (**self).gen_fold_accum()
        }
    }

    impl<L, R, O, F: ApplyGen<L, R, O> + ?Sized> ApplyGen<L, R, O> for Box<F> {
        fn get_join_kind(&self) -> JoinKind {
            (**self).get_join_kind()
        }

        fn gen_left_join_func(&self) -> FnGenResult<Box<dyn BinaryFunction<L, R, O>>> {
            (**self).gen_left_join_func()
        }
    }
}
