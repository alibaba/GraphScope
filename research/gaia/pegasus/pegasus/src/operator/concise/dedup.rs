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

use crate::api::{Barrier, Dedup, Map, Range};
use crate::codec::{shade_codec, ShadeCodec};
use crate::communication::Pipeline;
use crate::operator::concise::{never_clone, NeverClone};
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::collections::{CollectionFactory, Drain, DrainSet, DrainSetFactory};

impl<D: Data + Eq> Dedup<D> for Stream<D> {
    fn dedup<S>(&self, range: Range) -> Result<Stream<D>, BuildJobError>
    where
        S: DrainSet<D> + Default + 'static,
        S::Target: Send,
    {
        let factory: DefaultSetFactory<D, S> = DefaultSetFactory { _ph: std::marker::PhantomData };
        let barrier = self.barrier_with(range, factory)?;
        barrier.flat_map_with_fn(Pipeline, move |input| {
            let mut input = input.take().take();
            input.drain().map(|item| Ok(item))
        })
    }

    fn dedup_with<S>(&self, range: Range, factory: S) -> Result<Stream<D>, BuildJobError>
    where
        S: DrainSetFactory<D> + 'static,
        <S::Target as Drain<D>>::Target: Send,
    {
        let factory = ShadeSetFactory { inner: factory, _ph: std::marker::PhantomData };
        let barrier = self.barrier_with(range, factory)?;
        barrier.flat_map_with_fn(Pipeline, move |input| {
            let mut input = input.take().take();
            input.drain().map(|item| Ok(item))
        })
    }
}

struct DefaultSetFactory<T: Send + Eq, C: DrainSet<T> + Default> {
    _ph: std::marker::PhantomData<(T, C)>,
}

impl<T: Send + Eq, C: DrainSet<T> + Default> CollectionFactory<T> for DefaultSetFactory<T, C> {
    type Target = NeverClone<ShadeCodec<C>>;

    fn create(&self) -> Self::Target {
        never_clone(shade_codec(C::default()))
    }
}

struct ShadeSetFactory<T: Send + Eq, S: DrainSetFactory<T>> {
    inner: S,
    _ph: std::marker::PhantomData<T>,
}

impl<T: Send + Eq, S: DrainSetFactory<T>> CollectionFactory<T> for ShadeSetFactory<T, S> {
    type Target = NeverClone<ShadeCodec<S::Target>>;

    fn create(&self) -> Self::Target {
        never_clone(shade_codec(self.inner.create()))
    }
}
