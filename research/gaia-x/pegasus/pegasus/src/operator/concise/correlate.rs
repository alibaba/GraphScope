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
//

use pegasus_common::buffer::{Buffer, BufferPool, MemBufAlloc};

use crate::api::{Binary, CorrelatedSubTask, Unary};
use crate::data::MicroBatch;
use crate::errors::IOError;
use crate::progress::{EndSignal, Weight};
use crate::stream::{SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data, Tag};

impl<D: Data> CorrelatedSubTask<D> for Stream<D> {
    fn apply<T, F>(self, func: F) -> Result<Stream<(D, T)>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<SingleItem<T>, BuildJobError>,
    {
        let entered = self.enter()?;
        let (to_sub, main) = entered.copied()?;
        let scope_capacity = to_sub.get_scope_capacity();
        let sub = to_sub.unary("fork_subtask", move |info| {
            assert!(info.scope_level > 0);
            let id = crate::worker_id::get_current_worker();
            let worker = id.index;
            let offset = id.total_peers();
            let index = worker + offset;
            let mut tumbling_scope = TidyTagMap::new(info.scope_level - 1);
            let mut buf_pool = BufferPool::new(1, scope_capacity as usize, MemBufAlloc::new());
            move |input, output| {
                input.for_each_batch(|dataset| {
                    let len = dataset.len();
                    let p = dataset.tag.to_parent_uncheck();
                    if len > 0 {
                        let seq = tumbling_scope.get_mut_or_else(&p, || index);
                        for _ in 0..len {
                            if let Some(mut buf) = buf_pool.fetch() {
                                if let Some(next) = dataset.next() {
                                    buf.push(next);
                                    let cur = *seq;
                                    let tag = Tag::inherit(&p, cur);
                                    *seq += offset;
                                    let i = (cur - worker) / offset;
                                    trace_worker!("fork {}th scope {:?} from {:?};", i, tag, p);
                                    let mut batch = new_batch(tag.clone(), worker, buf);
                                    let end = EndSignal::new(tag, Weight::single(worker));
                                    batch.set_end(end);
                                    output.push_batch(batch)?;
                                } else {
                                    //
                                    break;
                                }
                            } else {
                                let i = (*seq - worker) / offset - 1;
                                trace_worker!(
                                    "suspend fork new scope as capacity blocked, already forked {} scopes;",
                                    i
                                );
                                would_block!("no buffer available for new scope;")?
                            }
                        }
                    }

                    if let Some(_end) = dataset.take_end() {
                        let idx = tumbling_scope.remove(&p).unwrap_or(index);
                        let cnt = (idx - worker) / offset;
                        trace_worker!("totally fork {} scope for {:?}", cnt - 1, p);
                    }

                    Ok(())
                })
            }
        })?;

        let SingleItem { inner } = func(sub)?;
        main.binary("zip_subtask", inner, |info| {
            let mut parent_data = TidyTagMap::new(info.scope_level - 1);
            let peers = crate::worker_id::get_current_worker().total_peers();
            move |input_left, input_right, output| {
                input_left.for_each_batch(|dataset| {
                    let p_tag = dataset.tag.to_parent_uncheck();
                    let barrier = parent_data.get_mut_or_else(&p_tag, || (vec![], None, 0, 0));
                    for item in dataset.drain() {
                        barrier.0.push(Some(item));
                    }
                    if let Some(ref e) = dataset.end {
                        barrier.1 = Some(e.clone());
                        let size = barrier.0.len();
                        barrier.2 = size;
                        trace_worker!(
                            "{} subtasks of {:?} waiting finish;",
                            size,
                            e.tag.to_parent_uncheck()
                        );
                    }
                    Ok(())
                })?;

                input_right.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let p_tag = dataset.tag.to_parent_uncheck();
                        if let Some(parent) = parent_data.get_mut(&p_tag) {
                            if parent.1.is_some() {
                                let seq = dataset.tag.current_uncheck();
                                if seq > 0 {
                                    let p_tag = dataset.tag.to_parent_uncheck();
                                    let offset = (seq / peers) as usize - 1;
                                    trace_worker!("join result of {}th subtask {:?}", offset, dataset.tag);
                                    let tag = Tag::inherit(&p_tag, 0);
                                    let mut session = output.new_session(&tag)?;
                                    assert_eq!(dataset.len(), 1);
                                    let item = dataset.next().unwrap();
                                    if let Some(p) = parent.0[offset].take() {
                                        session.give((p, item.0))?;
                                        parent.3 += 1;
                                    } else {
                                        error_worker!(
                                            "{}th subtask in scope {:?} had been joined before;",
                                            offset,
                                            dataset.tag
                                        );
                                        panic!(
                                            "{}th subtask in scope {:?} had been joined;",
                                            offset, dataset.tag
                                        );
                                    }
                                    dataset.take_end();

                                    if parent.2 == parent.3 {
                                        // assert!(dataset.is_last());
                                        trace_worker!(
                                            "all {} results of subtask {:?} joined;",
                                            parent.2,
                                            p_tag
                                        );
                                        let end = parent.1.take().expect("parent not end;");
                                        dataset.set_end(end);
                                    }
                                } else {
                                    // seq = 0 is not a subtask; it should be empty;
                                    // but it is not empty now, may be because of some aggregation operations;
                                    warn_worker!("data of scope {:?};", dataset.tag);
                                    dataset.clear();
                                }
                            } else {
                                warn_worker!(
                                    "{:?} subtask waiting parent scope end {:?};",
                                    dataset.tag,
                                    p_tag
                                );
                                would_block!("subtask waiting parent;")?;
                            }
                        } else {
                            would_block!("subtask waiting parent;")?;
                        }
                    } else {
                        //warn_worker!("empty subtask result of {:?};", dataset.tag);
                        dataset.take_end();
                    }

                    // dataset.take_end();
                    Ok(())
                })
            }
        })?
        .leave()
    }
}

#[cfg(not(feature = "rob"))]
fn new_batch<D>(tag: Tag, worker: u32, buf: Buffer<D>) -> MicroBatch<D> {
    MicroBatch::new(tag.clone(), worker, 0, buf)
}

#[cfg(feature = "rob")]
fn new_batch<D>(tag: Tag, worker: u32, buf: Buffer<D>) -> MicroBatch<D> {
    MicroBatch::new(tag.clone(), worker, buf.into_read_only())
}
