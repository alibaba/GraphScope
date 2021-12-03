use std::collections::HashMap;

use ahash::AHashMap;

use crate::api::function::FnResult;
use crate::api::{Fold, Key, Map, Pair, PartitionByKey, ReduceByKey, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<K: Data + Key, V: Data> ReduceByKey<K, V> for Stream<Pair<K, V>> {
    fn reduce_by_key<B, F>(self, builder: B) -> Result<SingleItem<HashMap<K, V>>, BuildJobError>
    where
        F: FnMut(V, V) -> FnResult<V> + Send + 'static,
        B: Fn() -> F + Send + 'static,
    {
        self.partition_by_key()
            .unary("reduce_by_key", |info| {
                let mut ttm = TidyTagMap::new(info.scope_level);
                move |input, output| {
                    input.for_each_batch(|dataset| {
                        let groups = ttm.get_mut_or_else(&dataset.tag, AHashMap::<K, (Option<V>, F)>::new);
                        for item in dataset.drain() {
                            let (k, v) = item.take();
                            if let Some((r, f)) = groups.get_mut(&k) {
                                let detach = r.take().expect("reduce value lost;");
                                let x = (*f)(detach, v)?;
                                r.replace(x);
                            } else {
                                groups.insert(k, (Some(v), builder()));
                            }
                        }
                        if dataset.is_last() {
                            let groups = std::mem::replace(groups, Default::default());
                            let mut map = HashMap::with_capacity(groups.len());
                            for (k, v) in groups {
                                if let Some(value) = v.0 {
                                    map.insert(k, value);
                                }
                            }
                            output
                                .new_session(&dataset.tag)?
                                .give(Single(map))?;
                        }
                        Ok(())
                    })
                }
            })?
            .flat_map(|map| Ok(map.0.into_iter()))?
            .fold(HashMap::new(), || {
                |mut map, (k, v)| {
                    map.insert(k, v);
                    Ok(map)
                }
            })
    }
}
