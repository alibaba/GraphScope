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
use crate::data::{EndByScope, MicroBatch};
use crate::errors::{IOError, JobExecError};
use crate::progress::Weight;
use crate::stream::{SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data, Tag};
use std::collections::VecDeque;

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
                                    let end = EndByScope::new(tag, Weight::single(worker), 1);
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
                    let barrier = parent_data.get_mut_or_else(&p_tag, || (ZipSubtaskBuf::new(), None));
                    for item in dataset.drain() {
                        barrier.0.add_req(item);
                    }
                    if let Some(e) = dataset.get_end() {
                        barrier.1 = Some(e.clone());
                    }
                    Ok(())
                })?;

                input_right.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let p_tag = dataset.tag.to_parent_uncheck();
                        if let Some(parent) = parent_data.get_mut(&p_tag) {
                            let tag = Tag::inherit(&p_tag, 0);
                            let mut session = output.new_session(&tag)?;
                            let seq = dataset.tag.current_uncheck();
                            let res = dataset.next().unwrap();
                            assert!(seq > 0);
                            let offset = (seq / peers) as usize - 1;
                            match parent.0.take(offset) {
                                Ok(req) => {
                                    trace_worker!("join result of {}th subtask {:?}", offset, dataset.tag);
                                    session.give((req, res.0))?;
                                },
                                Err(TakeErr::AlreadyTake) => {
                                    Err(JobExecError::panic(format!("{}th subtask with scope {:?} had been joined;", offset, dataset.tag)))?;
                                },
                                Err(TakeErr::NotExist) => {
                                    Err(JobExecError::panic(format!("req data of {}th task not found;", offset)))?;
                                }
                            }

                            if parent.0.is_empty() {
                                if let Some(end) = parent.1.take() {
                                    trace_worker!("all subtask from {:?} are joined;", end.tag.to_parent_uncheck());
                                    session.notify_end(end)?;
                                }
                            }
                        } else {
                            Err(JobExecError::panic(format!("req scope {:?} not found;", dataset.tag)))?;
                        }
                    } else {
                        //warn_worker!("empty subtask result of {:?};", dataset.tag);
                    }
                    dataset.take_end();

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

struct ZipSubtaskBuf<D> {
    reqs : VecDeque<Option<D>>,
    head: usize,
    len : usize ,
}

impl<D> ZipSubtaskBuf<D> {
    pub fn new() -> Self {
        ZipSubtaskBuf {
            reqs: VecDeque::new(),
            head: 0,
            len: 0,
        }
    }

    pub fn add_req(&mut self, req: D) {
        self.len += 1;
        self.reqs.push_back(Some(req));
    }

    pub fn take(&mut self, offset: usize) -> Result<D, TakeErr> {
        if offset < self.head {
            Err(TakeErr::AlreadyTake)
        } else {
            let offset = offset - self.head;
            if offset == 0 {
                if let Some(opt) = self.reqs.pop_front() {
                    assert!(opt.is_some());
                    self.head += 1;
                    loop {
                        match self.reqs.pop_front() {
                            Some(Some(item)) => {
                                self.reqs.push_front(Some(item));
                                break;
                            },
                            Some(None) => {
                                self.head += 1;
                            },
                            None => {
                                break;
                            }
                        }
                    }
                    self.len -= 1;
                    Ok(opt.unwrap())
                } else {
                    Err(TakeErr::NotExist)
                }
            } else {
                if let Some(opt) = self.reqs.get_mut(offset) {
                    if let Some(item) = opt.take() {
                        self.len -= 1;
                        Ok(item)
                    } else {
                        Err(TakeErr::AlreadyTake)
                    }
                } else {
                    Err(TakeErr::NotExist)
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum TakeErr {
    NotExist,
    AlreadyTake,
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn zip_subtask_buf_test() {
        let mut buf = ZipSubtaskBuf::new();
        for i in 0..100 {
            buf.add_req(i);
        }

        for i in 0..100 {
            let item = buf.take(i).expect("");
            assert_eq!(item, i as u32);
        }

        assert_eq!(buf.take(99), Err(TakeErr::AlreadyTake));
        assert_eq!(buf.take(100), Err(TakeErr::NotExist));
        assert_eq!(buf.take(100), Err(TakeErr::NotExist));
        assert!(buf.is_empty());

        for i in 100..200 {
            buf.add_req(i);
        }

        for i in 100..200 {
            let item = buf.take(i).expect("");
            assert_eq!(item, i as u32);
        }

        assert_eq!(buf.take(100), Err(TakeErr::AlreadyTake));
        assert_eq!(buf.take(200), Err(TakeErr::NotExist));
        assert!(buf.is_empty());

        for i in 200..300 {
            buf.add_req(i);
        }

        for i in (200..300).rev() {
            let item = buf.take(i).expect("");
            assert_eq!(item, i as u32);
        }
        assert_eq!(buf.take(0), Err(TakeErr::AlreadyTake));
        assert_eq!(buf.take(100), Err(TakeErr::AlreadyTake));
        assert!(buf.is_empty());
    }
}