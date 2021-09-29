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

use std::collections::VecDeque;
use std::fmt::Debug;

use pegasus_common::buffer::{Buffer, BufferPool, MemBufAlloc};

use crate::api::notification::{Cancel, End};
use crate::api::CorrelatedSubTask;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::data::{EndByScope, MicroBatch};
use crate::errors::{IOError, JobExecError};
use crate::operator::{Notifiable, OperatorCore};
use crate::progress::Weight;
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data, Tag};

impl<D: Data> CorrelatedSubTask<D> for Stream<D> {
    fn apply<T, F>(self, func: F) -> Result<Stream<(D, T)>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<SingleItem<T>, BuildJobError>,
    {
        let entered = self.enter()?;
        let scope_capacity = entered.get_scope_capacity();
        let (main, sub): (Stream<D>, Stream<D>) = entered.binary_branch_notify("fork_subtask", |info| {
            ForkSubtaskOperator::<D>::new(scope_capacity, info.scope_level)
        })?;

        let SingleItem { inner } = func(sub)?;
        main.union_transform_notify("zip_subtasks", inner, |info| {
            ZipSubtaskOperator::<D, T>::new(info.scope_level)
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

struct ForkProgress<D> {
    next_seq: u32,
    current_input_seq: Option<u64>,
    buf: BufferPool<D, MemBufAlloc<D>>
}

impl<D> ForkProgress<D> {
    fn new(next_seq: u32, capacity: usize) -> Self {
        ForkProgress {
            next_seq,
            current_input_seq: None,
            buf: BufferPool::new(1, capacity, MemBufAlloc::new())
        }
    }

    fn update_input_seq(&mut self, seq: u64) -> bool {
        if let Some(ref mut pre) = self.current_input_seq {
            if *pre < seq {
                *pre = seq;
                true
            } else {
                false
            }
        } else {
            self.current_input_seq = Some(seq);
            true
        }
    }
}

struct ForkSubtaskOperator<D> {
    worker_index: u32,
    peers: u32,
    scope_capacity: u32,
    scope_level: u32,
    tumbling_scope: TidyTagMap<ForkProgress<D>>,
}

impl<D> ForkSubtaskOperator<D> {
    fn new(scope_capacity: u32, scope_level: u32) -> Self {
        let id = crate::worker_id::get_current_worker();
        ForkSubtaskOperator {
            worker_index: id.index,
            peers: id.total_peers(),
            scope_capacity,
            scope_level,
            tumbling_scope: TidyTagMap::new(scope_level - 1),
        }
    }

    fn get_buf_mut(&mut self, tag: &Tag) -> &mut ForkProgress<D> {
        let init = self.worker_index + self.peers;
        let capacity = self.scope_capacity as usize;
        self.tumbling_scope
            .get_mut_or_else(tag, || ForkProgress::new(init, capacity))
    }
}

impl<D: Data> OperatorCore for ForkSubtaskOperator<D> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0]);
        let output1 = new_output::<D>(&outputs[0]);
        let output2 = new_output::<D>(&outputs[1]);
        input.for_each_batch(|batch| {
            let len = batch.len();
            if len > 0 {
                let p = batch.tag().to_parent_uncheck();
                let offset = self.peers;
                let worker = self.worker_index;

                let fp = self.get_buf_mut(&p);
                if fp.update_input_seq(batch.get_seq()) {
                    let batch_cp = batch.share();
                    output1.push_batch(batch_cp)?;
                }
                batch.take_end();
                for _ in 0..len {
                    if let Some(mut buf) = fp.buf.fetch() {
                        if let Some(next) = batch.next() {
                            buf.push(next);
                            let cur = fp.next_seq;
                            let tag = Tag::inherit(&p, cur);
                            fp.next_seq += offset;
                            let i = (cur - worker) / offset;
                            trace_worker!("fork {}th scope {:?}", i, tag);
                            let mut sub_batch = new_batch(tag.clone(), worker, buf);
                            let end = EndByScope::new(tag, Weight::single(worker), 1);
                            sub_batch.set_end(end);
                            output2.push_batch(sub_batch)?;
                        } else {
                            //
                            break;
                        }
                    } else {
                        let i = (fp.next_seq - worker) / offset - 1;
                        trace_worker!(
                            "suspend fork new scope as capacity blocked, already forked {} scopes;",
                            i
                        );
                        would_block!("no buffer available for new scope;")?
                    }
                }
            } else {
                if let Some(end) = batch.take_end() {
                    output1.notify_end(end)?;
                }
            }
            Ok(())
        })
    }
}

impl<D: Data> Notifiable for ForkSubtaskOperator<D> {
    fn on_end(&mut self, n: End, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        let level = n.tag().len() as u32;
        assert!(level < self.scope_level);
        let end = n.take();
        if level + 1 == self.scope_level {
            if let Some(fp) = self.tumbling_scope.remove(&end.tag) {
                let cnt = (fp.next_seq - self.worker_index) / self.peers;
                trace_worker!("totally fork {} scope for {:?}", cnt - 1, &end.tag);
            }
            outputs[1].notify_end(end.clone())?;
        } else if end.tag.is_root() {
            outputs[1].notify_end(end.clone())?;
        } else {
            // ignore;
        }
        outputs[0].notify_end(end)?;
        Ok(())
    }

    fn on_cancel(&mut self, n: Cancel, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        let level = n.tag().len() as u32;
        if n.port() == 0 {
            assert!(level < self.scope_level);
            if level + 1 == self.scope_level {
                let tag = Tag::inherit(n.tag(), 0);
                inputs[0].cancel_scope(&tag);
            }
            inputs[0].cancel_scope(n.tag());
        } else {
            assert_eq!(n.port(), 1);
            //ignore;
        }
        Ok(())
    }
}

struct ZipSubtaskOperator<P, S> {
    peers: u32,
    scope_level: u32,
    parent: TidyTagMap<ZipSubtaskBuf<P>>,
    parent_parent_ends: Vec<Vec<EndByScope>>,
    _ph: std::marker::PhantomData<S>,
}

impl<P: Data, S: Send> ZipSubtaskOperator<P, S> {
    fn new(scope_level: u32) -> Self {
        let peers = crate::worker_id::get_current_worker().total_peers();
        let mut parent_parent_ends = Vec::with_capacity(scope_level as usize - 1);
        for _ in 0..scope_level - 1 {
            parent_parent_ends.push(vec![]);
        }
        ZipSubtaskOperator {
            peers,
            scope_level,
            parent: TidyTagMap::new(scope_level - 1),
            parent_parent_ends,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<P: Data, S: Data> OperatorCore for ZipSubtaskOperator<P, S> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input_left = new_input_session::<P>(&inputs[0]);
        let mut input_right = new_input_session::<Single<S>>(&inputs[1]);
        let output = new_output::<(P, S)>(&outputs[0]);
        input_left.for_each_batch(|batch| {
            let p_tag = batch.tag.to_parent_uncheck();
            let barrier = self
                .parent
                .get_mut_or_else(&p_tag, || ZipSubtaskBuf::new());
            for item in batch.drain() {
                barrier.add(item);
            }
            if let Some(end) = batch.take_end() {
                barrier.cur_end(end);
            }
            Ok(())
        })?;

        input_right.for_each_batch(|batch| {
            if !batch.is_empty() {
                assert_eq!(batch.len(), 1);
                let p_tag = batch.tag.to_parent_uncheck();
                let peers = self.peers;
                if let Some(parent) = self.parent.get_mut(&p_tag) {
                    let tag = Tag::inherit(&p_tag, 0);
                    let mut session = output.new_session(&tag)?;
                    let seq = batch.tag.current_uncheck();
                    let res = batch.next().unwrap();
                    assert!(seq > 0, "unrecognized sequence {}", seq);
                    let offset = (seq / peers) as usize - 1;
                    match parent.take(offset) {
                        Ok(req) => {
                            trace_worker!("join result of {}th subtask {:?}", offset, batch.tag);
                            session.give((req, res.0))?;
                        }
                        Err(TakeErr::AlreadyTake) => {
                            Err(JobExecError::panic(format!(
                                "{}th subtask with scope {:?} had been joined;",
                                offset, batch.tag
                            )))?;
                        }
                        Err(TakeErr::NotExist) => {
                            Err(JobExecError::panic(format!("req data of {}th task not found;", offset)))?;
                        }
                    }

                    if parent.is_empty() {
                        if let Some(end) = parent.cur_end.take() {
                            trace_worker!("all subtasks of {:?} is joined;", p_tag);
                            session.notify_end(end)?;
                        }
                    }
                } else {
                    Err(JobExecError::panic(format!("req scope {:?} not found;", batch.tag)))?;
                }
            } else {
                //warn_worker!("empty subtask result of {:?};", dataset.tag);
            }
            batch.take_end();
            Ok(())
        })
    }
}

impl<P: Data, S: Data> Notifiable for ZipSubtaskOperator<P, S> {
    fn on_end(&mut self, n: End, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        let level = n.tag().len() as u32;
        assert!(level < self.scope_level);
        if n.port == 0 {
            if level + 1 == self.scope_level {
                if let Some(tasks) = self.parent.get_mut(n.tag()) {
                    if tasks.is_empty() {
                        if let Some(end) = tasks.cur_end.take() {
                            outputs[0].notify_end(end)?;
                        }
                        outputs[0].notify_end(n.take())?;
                    } else {
                        tasks.end(n.take());
                    }
                } else {
                    outputs[0].notify_end(n.take())?;
                }
            } else {
                let offset = level as usize;
                assert!(offset < self.parent_parent_ends.len());
                if self.parent.is_empty() {
                    outputs[0].notify_end(n.take())?;
                } else {
                    self.parent_parent_ends[offset].push(n.take());
                }
            }
        } else {
            assert_eq!(n.port, 1);
            if level + 1 == self.scope_level {
                trace_worker!("all subtasks of {:?} is finished;", n.tag());
                if let Some(mut tasks) = self.parent.remove(n.tag()) {
                    if tasks.is_empty() {
                        trace_worker!("all subtasks of {:?} is joined;", n.tag());
                    } else {
                        trace_worker!("{} subtasks of {:?} have no results;", tasks.len, n.tag());
                    }
                    if let Some(end) = tasks.cur_end.take() {
                        outputs[0].notify_end(end)?;
                    }

                    if let Some(end) = tasks.end.take() {
                        outputs[0].notify_end(end)?;
                    }
                    if self.parent.is_empty() {
                        for i in (0..self.parent_parent_ends.len()).rev() {
                            for end in self.parent_parent_ends[i].drain(..) {
                                outputs[0].notify_end(end)?;
                            }
                        }
                    }
                } else {
                    error_worker!("parent scope of {:?} not found;", n.tag());
                }
            } else {
                outputs[0].notify_end(n.take())?;
            }
        }

        Ok(())
    }

    fn on_cancel(&mut self, n: Cancel, inputs: &[Box<dyn InputProxy>]) -> Result<(), JobExecError> {
        trace_worker!("accept cancel of {:?}", n.tag());
        let level = n.tag().len() as u32;
        assert!(level < self.scope_level);
        if level + 1 == self.scope_level {
            if let Some(tasks) = self.parent.get_mut(n.tag()) {
                let len = tasks.len;
                tasks.cancel();
                inputs[1].cancel_scope(n.tag());
                if tasks.end.is_none() {
                    inputs[0].cancel_scope(n.tag());
                    trace_worker!("cancel join {} and flowing subtasks of {:?}", len, n.tag());
                } else {
                    trace_worker!("cancel join {} subtasks of {:?}", len, n.tag());
                }
            } else {
                // already end; ignore;
            }
        } else {
            for (tag, tasks) in self.parent.iter_mut() {
                if n.tag().is_parent_of(&*tag) {
                    let len = tasks.len;
                    tasks.cancel();
                    inputs[1].cancel_scope(&*tag);
                    if tasks.end.is_none() {
                        inputs[0].cancel_scope(&*tag);
                        trace_worker!("cancel join {} and flowing subtasks of {:?}", len, &*tag);
                    } else {
                        trace_worker!("cancel join {} subtasks of {:?}", len, &*tag);
                    }
                }
            }
            inputs[0].cancel_scope(n.tag());
            inputs[1].cancel_scope(n.tag());
        }
        Ok(())
    }
}

struct ZipSubtaskBuf<D> {
    reqs: VecDeque<Option<D>>,
    head: usize,
    len: usize,
    is_canceled: bool,
    cur_end: Option<EndByScope>,
    end: Option<EndByScope>,
}

impl<D> ZipSubtaskBuf<D> {
    fn new() -> Self {
        ZipSubtaskBuf { reqs: VecDeque::new(), head: 0, len: 0, is_canceled: false, cur_end: None, end: None }
    }

    fn cur_end(&mut self, end:EndByScope) {
        self.cur_end = Some(end);
    }

    fn end(&mut self, end: EndByScope) {
        self.end = Some(end);
    }

    fn add(&mut self, req: D) {
        self.len += 1;
        self.reqs.push_back(Some(req));
    }

    fn take(&mut self, offset: usize) -> Result<D, TakeErr> {
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
                            }
                            Some(None) => {
                                self.head += 1;
                            }
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

    fn cancel(&mut self) {
        self.reqs.clear();
        self.head = 0;
        self.len = 0;
        self.is_canceled = true;
    }

    fn is_empty(&self) -> bool {
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
            buf.add(i);
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
            buf.add(i);
        }

        for i in 100..200 {
            let item = buf.take(i).expect("");
            assert_eq!(item, i as u32);
        }

        assert_eq!(buf.take(100), Err(TakeErr::AlreadyTake));
        assert_eq!(buf.take(200), Err(TakeErr::NotExist));
        assert!(buf.is_empty());

        for i in 200..300 {
            buf.add(i);
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
