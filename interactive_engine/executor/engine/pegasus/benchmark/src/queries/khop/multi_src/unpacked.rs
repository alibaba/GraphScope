use std::time::Instant;

use pegasus::api::{CorrelatedSubTask, Count, Iteration, Limit, Map, Sink};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::JobConf;

use super::super::one_hop;
use crate::graph::Graph;

pub fn unpacked_multi_src_k_hop<G: Graph, R: PartitionedResource<Res = G>>(
    src: Vec<u64>, k_hop: u32, use_loop: bool, is_limit_one: bool, conf: JobConf, graph: R,
) -> ResultStream<(u64, u64, u64)> {
    let start = Instant::now();
    pegasus::run_with_resources(conf, graph, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        move |input, output| {
            let stream = input.input_from(src)?;
            stream
                .repartition(|id| Ok(*id))
                .apply(|sub| {
                    let count_or_limit = if use_loop {
                        sub.iterate(k_hop, |start| {
                            let graph =
                                pegasus::resource::get_resource::<R::Res>().expect("Graph not found");
                            start
                                .repartition(|id| Ok(*id))
                                .flat_map(move |id| Ok(one_hop(id, &*graph)))
                        })?
                    } else {
                        let graph = pegasus::resource::get_resource::<G>().expect("Graph not found");
                        let mut stream = sub
                            .repartition(|id| Ok(*id))
                            .flat_map(move |id| Ok(one_hop(id, &*graph)))?;

                        for _i in 1..k_hop {
                            let graph =
                                pegasus::resource::get_resource::<R::Res>().expect("Graph not found");
                            stream = stream
                                .repartition(|id| Ok(*id))
                                .flat_map(move |id| Ok(one_hop(id, &*graph)))?;
                        }
                        stream
                    };
                    if is_limit_one {
                        count_or_limit.limit(1)?.count()
                    } else {
                        count_or_limit.count()
                    }
                })?
                .map(move |(id, cnt)| Ok((id, cnt, start.elapsed().as_millis() as u64)))?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
