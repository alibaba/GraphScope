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

use std::hash::{BuildHasher, Hash, Hasher};

use crate::api::function::{FnResult, RouteFunction};
use crate::api::{HasKey, Key, KeyBy, Map, Pair, PartitionByKey};
use crate::stream::Stream;
use crate::{BuildJobError, Data};

impl<D: Data> KeyBy<D> for Stream<D> {
    fn key_by<K, V, F>(self, selector: F) -> Result<Stream<Pair<K, V>>, BuildJobError>
    where
        K: Data + Key,
        V: Data,
        F: Fn(D) -> FnResult<(K, V)> + Send + 'static,
    {
        self.map(move |item| {
            let (key, value) = selector(item)?;
            Ok(Pair { key, value })
        })
    }
}

impl<D: Data + HasKey> PartitionByKey<D> for Stream<D> {
    fn partition_by_key(self) -> Stream<D> {
        let job_id = crate::worker_id::get_current_worker().job_id;
        let bh = ahash::RandomState::with_seeds(job_id, job_id & 3, job_id & 7, job_id & 15);
        let router = KeyRouter::new(bh);

        self.repartition(move |item| router.route(item))
    }
}

pub struct KeyRouter<D: Data + HasKey, H: BuildHasher + Send + 'static> {
    hash_builder: H,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data + HasKey, H: BuildHasher + Send> KeyRouter<D, H> {
    pub fn new(hash_builder: H) -> Self {
        KeyRouter { hash_builder, _ph: std::marker::PhantomData }
    }
}

impl<D: Data + HasKey, H: BuildHasher + Send + 'static> RouteFunction<D> for KeyRouter<D, H> {
    fn route(&self, data: &D) -> FnResult<u64> {
        let mut hasher = self.hash_builder.build_hasher();
        data.get_key().hash(&mut hasher);
        Ok(hasher.finish())
    }
}

mod dedup;
mod fold;
mod join;
mod reduce;

#[cfg(test)]
mod test {
    use crate::api::function::RouteFunction;
    use crate::operator::concise::keyed::KeyRouter;

    #[test]
    fn key_router_test() {
        let bh = ahash::RandomState::new();
        let router = KeyRouter::<u32, ahash::RandomState>::new(bh.clone());
        let router_mirror = KeyRouter::<u32, ahash::RandomState>::new(bh);
        let router_another = KeyRouter::<u32, ahash::RandomState>::new(ahash::RandomState::new());
        for i in 0..65536 {
            let pre = router.route(&i).unwrap();
            assert_eq!(pre, router.route(&i).unwrap());
            assert_eq!(pre, router_mirror.route(&i).unwrap());
            assert_ne!(pre, router_another.route(&i).unwrap());
        }
    }
}
