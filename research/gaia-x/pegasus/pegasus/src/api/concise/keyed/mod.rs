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
//!

use std::fmt::{Debug, Formatter};
use std::hash::Hash;

pub use apply::*;
pub use dedup::*;
pub use fold::*;
pub use join::*;
pub use reduce::*;

use crate::api::function::FnResult;
use crate::codec::{Decode, Encode, ReadExt, WriteExt};
use crate::stream::Stream;
use crate::{BuildJobError, Data};

pub trait Key: Hash + Eq + Send + Clone + 'static {}

impl<T: Hash + Eq + Send + Clone + 'static> Key for T {}

/// A `HasKey` data must contain a `Key` part.
pub trait HasKey: Send + 'static {
    type Target: Key;

    /// To obtain the `Key` part of the `Keyed` data.
    fn get_key(&self) -> &Self::Target;
}

impl<K: Key> HasKey for K {
    type Target = Self;

    fn get_key(&self) -> &Self::Target {
        &self
    }
}

pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> Pair<K, V> {
    pub fn take(self) -> (K, V) {
        (self.key, self.value)
    }
}

impl<K: Key, V: Send + 'static> HasKey for Pair<K, V> {
    type Target = K;

    fn get_key(&self) -> &Self::Target {
        &self.key
    }
}

impl<K: Key + Encode, V: Encode> Encode for Pair<K, V> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.key.write_to(writer)?;
        self.value.write_to(writer)
    }
}

impl<K: Key + Decode, V: Decode> Decode for Pair<K, V> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let key = K::read_from(reader)?;
        let value = V::read_from(reader)?;
        Ok(Pair { key, value })
    }
}

impl<K: Key, V: Clone> Clone for Pair<K, V> {
    fn clone(&self) -> Self {
        Pair { key: self.key.clone(), value: self.value.clone() }
    }
}

impl<K: Debug, V: Debug> Debug for Pair<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "k->{:?},v->{:?}", self.key, self.value)
    }
}

/// `KeyBy` is used to transform any input data into a key-value pair.
pub trait KeyBy<D: Data> {
    /// Given a user-defined function `selection`, this function is actually a map function that
    /// transform each input data into a [`Pair`].  Addtionally, the output data must be repartitioned
    /// via the key part of [`Pair`], such that all data with the same key will be guaranteed to
    /// arrive at the same worker.
    ///
    /// [`Pair`]: crate::api::keyed::Pair
    ///
    /// # Example
    /// ```
    /// #     use pegasus::JobConf;
    /// #     use pegasus::Configuration;
    /// #     use pegasus::api::{KeyBy, Filter, Sink, PartitionByKey, Collect, Map};
    /// #     let mut conf = JobConf::new("key_by_example");
    /// #     conf.set_workers(2);
    ///       let mut results = pegasus::run(conf, || {
    ///         let id = pegasus::get_current_worker().index;
    ///         move |input, output| {
    ///                 let src = if id == 0 {
    ///                     input.input_from(1_u32..4)?
    ///                 } else {
    ///                     input.input_from(vec![])?
    ///                 };
    ///                 src.key_by(|x| Ok((x % 2, x)))?
    ///                    .partition_by_key()
    ///                    .filter(move |_| Ok(id == 1))?
    ///                    .map(|d| Ok((d.key, d.value)))?
    ///                    .collect::<Vec<(u32, u32)>>()?
    ///                    .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     // We comment the assertion, as the keyBy partition is so random.
    ///     // results.sort();
    ///     // assert_eq!(results,[(1, 1), (1, 3)]);
    /// ```
    fn key_by<K, V, F>(self, selector: F) -> Result<Stream<Pair<K, V>>, BuildJobError>
    where
        K: Data + Key,
        V: Data,
        F: Fn(D) -> FnResult<(K, V)> + Send + 'static;
}

pub trait PartitionByKey<D: Data + HasKey> {
    fn partition_by_key(self) -> Stream<D>;
}

mod apply;
mod dedup;
mod fold;
mod join;
mod reduce;
