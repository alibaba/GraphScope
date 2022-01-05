use std::sync::Arc;
use std::time::Instant;

use pegasus::api::{CorrelatedSubTask, Count, Iteration, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::{Data, JobConf};

use super::super::one_hop;
use crate::graph::{Graph, VertexId};

pub fn unpacked_multi_src_k_hop<G: Graph>(
    src: Vec<G::VID>, k_hop: u32, use_loop: bool, conf: JobConf, graph: Arc<G>,
) -> ResultStream<(u64, u64, u64)> where G::VID : Data {
    let start = Instant::now();
    pegasus::run_with_resources(conf, graph, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { src.clone() } else { vec![] };
        move |input, output| {
            let stream = input.input_from(src)?;
            stream
                .repartition(|id| Ok(id.get_id()))
                .apply(|sub| {
                    if use_loop {
                        sub.iterate(k_hop, |start| {
                            let graph =
                                pegasus::resource::get_resource::<Arc<G>>().expect("Graph not found");
                            start
                                .repartition(|id| Ok(id.get_id()))
                                .flat_map(move |id| Ok(one_hop(id, &*graph)))
                        })?
                    } else {
                        let graph = pegasus::resource::get_resource::<Arc<G>>().expect("Graph not found");
                        let mut stream = sub
                            .repartition(|id| Ok(id.get_id()))
                            .flat_map(move |id| Ok(one_hop(id, &*graph)))?;

                        for _i in 1..k_hop {
                            let graph =
                                pegasus::resource::get_resource::<Arc<G>>().expect("Graph not found");
                            stream = stream
                                .repartition(|id| Ok(id.get_id()))
                                .flat_map(move |id| Ok(one_hop(id, &*graph)))?;
                        }
                        stream
                    }
                    .count()
                })?
                .map(move |(id, cnt)| Ok((id.get_id(), cnt, start.elapsed().as_millis() as u64)))?
                .sink_into(output)
        }
    })
    .expect("submit job failure")
}
