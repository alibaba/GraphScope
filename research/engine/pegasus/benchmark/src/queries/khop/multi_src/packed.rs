use std::collections::HashMap;
use std::time::Instant;

use pegasus::api::{Binary, Fold, Iteration, Limit, Map, Sink};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::JobConf;

use super::super::one_hop;
use crate::graph::Graph;

pub fn packed_multi_src_k_hop<G: Graph, R: PartitionedResource<Res = G>>(
    src: Vec<u64>, k_hop: u32, use_loop: bool, is_limit_one: bool, conf: JobConf, graph: R,
) -> ResultStream<(u64, u64, u64)> {
    let start = Instant::now();
    let src = src
        .into_iter()
        .enumerate()
        .map(|(i, id)| (i as u64, id))
        .collect::<Vec<_>>();
    pegasus::run_with_resources(conf, graph, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        move |input, output| {
            let stream = input.input_from(src)?;
            let (left, mut right) = stream.copied()?;
            if use_loop {
                right = right.iterate(k_hop, |start| {
                    let graph = pegasus::resource::get_resource::<R::Res>().expect("Graph not found");
                    start
                        .repartition(|(_, id)| Ok(*id))
                        .flat_map(move |(src, id)| Ok(one_hop(id, &*graph).map(move |id| (src, id))))
                })?
            } else {
                for _i in 0..k_hop {
                    let graph = pegasus::resource::get_resource::<R::Res>().expect("Graph not found");
                    right = right
                        .repartition(|(_, id)| Ok(*id))
                        .flat_map(move |(src, id)| Ok(one_hop(id, &*graph).map(move |id| (src, id))))?;
                    if is_limit_one {
                        right = right.limit(1)?;
                    }
                }
            };

            let group_cnt = right
                .fold_partition(HashMap::new(), || {
                    |mut count, (src, _)| {
                        let cnt = count.entry(src).or_insert(0u64);
                        *cnt += 1;
                        Ok(count)
                    }
                })?
                .unfold(|map| Ok(map.into_iter()))?
                .repartition(|(src, _)| Ok(*src))
                .fold_partition(HashMap::new(), || {
                    |mut count, (src, cnt)| {
                        let sum = count.entry(src).or_insert(0u64);
                        *sum += cnt;
                        Ok(count)
                    }
                })?
                .unfold(move |map| Ok(map.into_iter()))?;

            left.repartition(|(src, _)| Ok(*src))
                .binary("join", group_cnt, |info| {
                    let mut map = TidyTagMap::<HashMap<u64, u64>>::new(info.scope_level);
                    move |input_left, input_right, output| {
                        input_left.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            let id_map = map.get_mut_or_insert(&dataset.tag);
                            for (k, v) in dataset.drain() {
                                if let Some(right) = id_map.remove(&k) {
                                    session.give((v, right, start.elapsed().as_micros() as u64))?;
                                } else {
                                    id_map.insert(k, v);
                                }
                            }
                            Ok(())
                        })?;

                        input_right.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            let id_map = map.get_mut_or_insert(&dataset.tag);
                            for (k, v) in dataset.drain() {
                                if let Some(left) = id_map.remove(&k) {
                                    session.give((left, v, start.elapsed().as_millis() as u64))?;
                                } else {
                                    id_map.insert(k, v);
                                }
                            }
                            Ok(())
                        })
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
