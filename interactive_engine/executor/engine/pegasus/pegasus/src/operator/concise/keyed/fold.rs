use std::collections::HashMap;

use ahash::AHashMap;

use crate::api::function::FnResult;
use crate::api::{Fold, FoldByKey, Key, Map, Pair, PartitionByKey, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<K: Data + Key, V: Data> FoldByKey<K, V> for Stream<Pair<K, V>> {
    fn fold_by_key<I, B, F>(self, init: I, builder: B) -> Result<SingleItem<HashMap<K, I>>, BuildJobError>
    where
        I: Data,
        F: FnMut(I, V) -> FnResult<I> + Send + 'static,
        B: Fn() -> F + Send + 'static,
    {
        self.partition_by_key()
            .unary("fold_by_key", |info| {
                let mut ttm = TidyTagMap::new(info.scope_level);
                move |input, output| {
                    let result = input.for_each_batch(|dataset| {
                        let group = ttm.get_mut_or_else(&dataset.tag, AHashMap::<K, (Option<I>, F)>::new);
                        for item in dataset.drain() {
                            let (k, v) = item.take();
                            let (seed, func) = group
                                .entry(k)
                                .or_insert_with(|| (Some(init.clone()), builder()));
                            let mut s = seed.take().expect("fold seed lost");
                            s = (*func)(s, v)?;
                            seed.replace(s);
                        }

                        if dataset.is_last() {
                            let group = std::mem::replace(group, Default::default());
                            let mut map = HashMap::new();
                            // todo: reuse group map;
                            for (k, v) in group {
                                map.insert(k, v.0.unwrap_or_else(|| init.clone()));
                            }
                            output
                                .new_session(&dataset.tag)?
                                .give(Single(map))?;
                        }

                        Ok(())
                    });

                    ttm.retain(|_, map| !map.is_empty());
                    result
                }
            })?
            .flat_map(|x| Ok(x.0.into_iter()))?
            .fold(HashMap::new(), || {
                |mut map, (k, v)| {
                    map.insert(k, v);
                    Ok(map)
                }
            })
    }
}
