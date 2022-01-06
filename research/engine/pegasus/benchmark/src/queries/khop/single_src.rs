use std::time::{Duration, Instant};

use pegasus::api::{Count, Iteration, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::{Data, JobConf};
use pegasus::resource::PartitionedResource;

use super::one_hop;
use crate::graph::{Graph, VertexId};

/// count k-hop neighbors for single source vertex;

pub fn single_src_k_hop<G: Graph, R: PartitionedResource<Res=G>>(
    id: G::VID, k_hop: u32, use_loop: bool, conf: JobConf, graph: R,
) -> ResultStream<(u64, u64, Duration)> where G::VID : Data {
    let start = Instant::now();
    pegasus::run_with_resources(conf, graph, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { vec![id.clone()] } else { vec![] };
        let id = id.clone();
        move |input, output| {
            let mut stream = input.input_from(src)?;
            if use_loop {
                stream = stream.iterate(k_hop, |start| {
                    let graph = pegasus::resource::get_resource::<R::Res>().expect("Graph not found;");
                    start
                        .repartition(|id| Ok(id.get_id()))
                        .flat_map(move |id| Ok(one_hop(id, &*graph)))
                })?;
            } else {
                for _i in 0..k_hop {
                    let graph = pegasus::resource::get_resource::<R::Res>().expect("Graph not found;");
                    stream = stream
                        .repartition(|id| Ok(id.get_id()))
                        .flat_map(move |id| Ok(one_hop(id, &*graph)))?;
                }
            }
            stream
                .count()?
                .map(move |cnt| {
                    let x = start.elapsed();
                    Ok((id.get_id(), cnt, x))
                })?
                .sink_into(output)
        }
    })
    .expect("submit k_hop job failure;")
}
