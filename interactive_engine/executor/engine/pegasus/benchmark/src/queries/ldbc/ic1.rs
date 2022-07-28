use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pegasus::api::{Binary, Branch, IterCondition, Iteration, Map, Sink, Unary};
use pegasus::resource::PartitionedResource;
use pegasus::result::ResultStream;
use pegasus::tag::tools::map::TidyTagMap;
use pegasus::{JobConf};
use pegasus_graph::graph::Direction;

use crate::graph::{Graph, OrderBy};

// interactive complex query 1 :
// g.V().hasLabel('person').has('person_id', $id)
// .repeat(both('knows').has('firstName', $name).has('person_id', neq($id)).dedup()).emit().times(3)
// .dedup().limit(20)
// .project('dist', 'person').by(loops()).by(identity())
// .fold()
// .map{ 排序 }

pub fn ic1<G: Graph, R: PartitionedResource<Res = G>>(
    person_id: u64, first_name: String, conf: JobConf, graph: R,
) -> ResultStream<Vec<u8>> {
    // pegasus::run_with_resources(conf, graph, || {
    //     let first_name = first_name.clone();
    //     move |source, sink| {
    //         let stream = if source.get_worker_index() == 0 {
    //             source.input_from(vec![person_id.clone()])
    //         } else {
    //             source.input_from(vec![])
    //         }?;
    //
    //         let (emit, leave) = stream.iterate_emit(IterCondition::max_iters(3), |start| {
    //             let graph = pegasus::resource::get_resource::<R::Res>().unwrap();
    //             start
    //                 .repartition(|id| Ok(*id))
    //                 .flat_map(move |src_id| {
    //                     Ok(graph
    //                         .get_neighbor_ids(src_id, "person", "knows", Direction::Both)
    //                         .filter(move |id| *id != person_id))
    //                 })?
    //                 .unary("filter", |_| {
    //                     let graph = pegasus::resource::get_resource::<R::Res>().unwrap();
    //                     let mut vec = vec![];
    //                     move |input, output| {
    //                         input.for_each_batch(|batch| {
    //                             if !batch.is_empty() {
    //                                 vec.clear();
    //                                 for id in batch.drain() {
    //                                     vec.push(id);
    //                                 }
    //                                 let result = graph.filter_vertex("person", &vec, format!("p_firstname = '{}'", first_name));
    //                                 output
    //                                     .new_session(batch.tag())?
    //                                     .give_iterator(result.into_iter())?;
    //                             }
    //                             Ok(())
    //                         })
    //                     }
    //                 })?
    //                 .aggregate()
    //                 .branch("dedup_limit", |info| {
    //                     let mut g_dup = HashSet::new();
    //                     let mut emit = TidyTagMap::new(info.scope_level);
    //                     let mut emit_cnt = 0u32;
    //                     move |input, output1, output2| {
    //                         input.for_each_batch(|batch| {
    //                             if !batch.is_empty() {
    //                                 let limited =
    //                                     emit.get_mut_or_else(batch.tag(), || (Vec::new(), emit_cnt));
    //                                 if limited.1 < 20 {
    //                                     for item in batch.drain() {
    //                                         if !g_dup.contains(&item) {
    //                                             g_dup.insert(item.clone());
    //                                             limited.0.push(item);
    //                                             limited.1 += 1;
    //                                             if limited.1 >= 20 {
    //                                                 break;
    //                                             }
    //                                         }
    //                                     }
    //                                     if limited.1 >= 20 {
    //                                         let v = std::mem::replace(&mut limited.0, vec![]);
    //                                         let dist = batch.tag.current_uncheck();
    //                                         let size = v.len() as u32;
    //                                         if dist < 2 {
    //                                             output1
    //                                                 .new_session(batch.tag())?
    //                                                 .give_iterator(v.into_iter().map(move |i| (dist, i)))?;
    //                                         } else {
    //                                             output2
    //                                                 .new_session(batch.tag())?
    //                                                 .give_iterator(v.into_iter())?;
    //                                         }
    //                                         emit_cnt += size;
    //                                     }
    //                                     if !batch.is_empty() {
    //                                         batch.discard();
    //                                     }
    //                                 }
    //                             }
    //
    //                             if batch.is_last() {
    //                                 if let Some((vec, _)) = emit.remove(batch.tag()) {
    //                                     if !vec.is_empty() {
    //                                         let dist = batch.tag().current_uncheck();
    //                                         if dist < 2 {
    //                                             output1
    //                                                 .new_session(batch.tag())?
    //                                                 .give_iterator(
    //                                                     vec.clone().into_iter().map(move |i| (dist, i)),
    //                                                 )?;
    //                                             emit_cnt += vec.len() as u32;
    //                                         }
    //                                         output2
    //                                             .new_session(batch.tag())?
    //                                             .give_iterator(vec.into_iter())?;
    //                                     }
    //                                 }
    //                             }
    //                             Ok(())
    //                         })
    //                     }
    //                 })
    //         })?;
    //         emit.binary("merge_barrier", leave, |_| {
    //             let mut collect = HashMap::new();
    //             let mut binary_end = HashSet::new();
    //             let order_by = vec![OrderBy::asc_by("p_lastname"), OrderBy::asc_by("~id")];
    //             move |left, right, output| {
    //                 left.for_each_batch(|batch| {
    //                     for item in batch.drain() {
    //                         collect.insert(item.1, item.0);
    //                     }
    //                     if batch.is_last() {
    //                         if !binary_end.insert(batch.tag().clone()) {
    //                             let ids = collect.keys().copied().collect::<Vec<_>>();
    //                             let graph = pegasus::resource::get_resource::<Arc<G>>().unwrap();
    //                             let details = graph.get_vertices_by_ids("person", &ids);
    //                             let mut with_dist = Vec::with_capacity(details.len());
    //                             for v in details {
    //                                 let dist = collect.get(&v.id.vertex_id()).expect("dist lost");
    //                                 with_dist.push((*dist, v));
    //                             }
    //                             with_dist.sort_by(|a, b| {
    //                                 let ord = a.0.cmp(&b.0);
    //                                 if ord == Ordering::Equal {
    //                                     a.1.cmp_by(&b.1, &order_by)
    //                                 } else {
    //                                     ord
    //                                 }
    //                             });
    //                             let binary = encode_result(with_dist);
    //                             output.new_session(batch.tag())?.give(binary)?;
    //                         }
    //                     }
    //                     Ok(())
    //                 })?;
    //                 right.for_each_batch(|batch| {
    //                     for item in batch.drain() {
    //                         collect.insert(item, 3);
    //                     }
    //                     if batch.is_last() {
    //                         if !binary_end.insert(batch.tag().clone()) {
    //                             let ids = collect.keys().copied().collect::<Vec<_>>();
    //                             let graph = pegasus::resource::get_resource::<Arc<G>>().unwrap();
    //                             let details = graph.get_vertices_by_ids("person", &ids);
    //                             let mut with_dist = Vec::with_capacity(details.len());
    //                             for v in details {
    //                                 let dist = collect.get(&v.id.vertex_id()).unwrap();
    //                                 with_dist.push((*dist, v));
    //                             }
    //                             with_dist.sort_by(|a, b| {
    //                                 let ord = a.0.cmp(&b.0);
    //                                 if ord == Ordering::Equal {
    //                                     a.1.cmp_by(&b.1, &order_by)
    //                                 } else {
    //                                     ord
    //                                 }
    //                             });
    //                             let binary = encode_result(with_dist);
    //                             output.new_session(batch.tag())?.give(binary)?;
    //                         }
    //                     }
    //                     Ok(())
    //                 })
    //             }
    //         })?
    //         .sink_into(sink)
    //     }
    // })
    // .expect("submit ic1 job failure")
    todo!()
}

#[inline]
fn encode_result<T>(_result: T) -> Vec<u8> {
    unimplemented!()
}
