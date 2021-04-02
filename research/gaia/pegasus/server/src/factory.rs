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

use crate::AnyData;
use pegasus::api::accum::{AccumFactory, Accumulator};
use pegasus::api::function::*;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus::BuildJobError;
use pegasus_common::collections::{Collection, CollectionFactory, Map, MapFactory, Set};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;

pub type CompileResult<T> = Result<T, BuildJobError>;

pub type DynMap<T> = Box<dyn Map<T, T, Target = Box<dyn Iterator<Item = (T, T)> + Send>>>;

pub type DynMapFactory<T> = Box<dyn MapFactory<T, T, Target = DynMap<T>>>;

pub type DynGroupUnfold<T> = Box<dyn FlatMapFunction<DynMap<T>, T, Target = DynIter<T>>>;

pub type DynGroupSink<T> = Box<dyn EncodeFunction<DynMap<T>>>;

pub trait GroupFunction<D>: Send + 'static {
    fn key(&self) -> CompileResult<Box<dyn KeyFunction<D, Key = D>>>;

    fn map_factory(&self) -> CompileResult<DynMapFactory<D>>;

    fn unfold(&self) -> CompileResult<DynGroupUnfold<D>>;

    fn sink(&self) -> CompileResult<DynGroupSink<D>>;
}

pub trait FoldFunction<D>: Send + 'static {
    fn accumulate(
        &self,
    ) -> CompileResult<Box<dyn AccumFactory<D, Target = Box<dyn Accumulator<D>>>>>;

    fn fold_unfold(
        &self,
    ) -> CompileResult<Box<dyn FlatMapFunction<Box<dyn Accumulator<D>>, D, Target = DynIter<D>>>>;

    fn fold_sink(&self) -> CompileResult<Box<dyn EncodeFunction<Box<dyn Accumulator<D>>>>>;
}

/// Compile binary resource into executable user defined function;
pub trait JobCompiler<D: AnyData>: Send + Sync + 'static {
    fn shuffle(&self, res: &[u8]) -> CompileResult<Box<dyn RouteFunction<D>>>;

    fn broadcast(&self, res: &[u8]) -> CompileResult<Box<dyn MultiRouteFunction<D>>>;

    fn source(&self, src: &[u8]) -> CompileResult<Box<dyn Iterator<Item = D> + Send>>;

    fn map(&self, res: &[u8]) -> CompileResult<Box<dyn MapFunction<D, D>>>;

    fn flat_map(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn FlatMapFunction<D, D, Target = DynIter<D>>>>;

    fn filter(&self, res: &[u8]) -> CompileResult<Box<dyn FilterFunction<D>>>;

    fn left_join(&self, res: &[u8]) -> CompileResult<Box<dyn LeftJoinFunction<D>>>;

    fn compare(&self, res: &[u8]) -> CompileResult<Box<dyn CompareFunction<D>>>;

    fn group(
        &self, map_factory: &[u8], unfold: &[u8], sink: &[u8],
    ) -> CompileResult<Box<dyn GroupFunction<D>>>;

    fn fold(
        &self, accum: &[u8], unfold: &[u8], sink: &[u8],
    ) -> CompileResult<Box<dyn FoldFunction<D>>>;

    fn collection_factory(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<D, Target = Box<dyn Collection<D>>>>>;

    fn set_factory(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<D, Target = Box<dyn Set<D>>>>>;

    fn sink(&self, res: &[u8]) -> CompileResult<Box<dyn EncodeFunction<D>>>;
    // others undefined;
}

pub struct HashKey<T> {
    hash: u64,
    value: T,
}

impl<T> Hash for HashKey<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash)
    }
}

impl<T: PartialEq> PartialEq<T> for HashKey<T> {
    fn eq(&self, other: &T) -> bool {
        self.value.eq(other)
    }
}

impl<T: PartialEq> PartialEq for HashKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl<T: Eq> Eq for HashKey<T> {}

impl<T: Clone> Clone for HashKey<T> {
    fn clone(&self) -> Self {
        HashKey { hash: self.hash, value: self.value.clone() }
    }
}

impl<T: Debug> Debug for HashKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.value)
    }
}

impl<T: Encode> Encode for HashKey<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.hash)?;
        self.value.write_to(writer)
    }
}

impl<T: Decode> Decode for HashKey<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let hash = reader.read_u64()?;
        let value = T::read_from(reader)?;
        Ok(HashKey { hash, value })
    }
}

impl<T> Deref for HashKey<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
