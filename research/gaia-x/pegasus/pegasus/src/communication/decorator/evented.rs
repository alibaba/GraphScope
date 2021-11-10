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
use crate::channel_id::ChannelInfo;
use crate::communication::IOResult;
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPush, Push};
use crate::event::emitter::EventEmitter;
use crate::event::{Event, EventKind};
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

#[allow(dead_code)]
pub struct EventEmitPush<T: Data> {
    pub ch_info: ChannelInfo,
    pub source_worker: u32,
    pub target_worker: u32,
    inner: GeneralPush<MicroBatch<T>>,
    event_emitter: EventEmitter,
    // scope -> (sequence, counts)
    push_monitor: TidyTagMap<(usize, usize, usize)>,
}

#[allow(dead_code)]
impl<T: Data> EventEmitPush<T> {
    pub fn new(
        info: ChannelInfo, source_worker: u32, target_worker: u32, push: GeneralPush<MicroBatch<T>>,
        emitter: EventEmitter,
    ) -> Self {
        let push_counts = TidyTagMap::new(info.scope_level);
        EventEmitPush {
            ch_info: info,
            source_worker,
            target_worker,
            inner: push,
            event_emitter: emitter,
            push_monitor: push_counts,
        }
    }

    pub fn get_push_count(&self, tag: &Tag) -> Option<usize> {
        self.push_monitor.get(tag).map(|(_, _, x)| *x)
    }

    pub fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
        assert!(end.sources() > 1);
        if end.tag().len() == self.push_monitor.scope_level as usize {
            let size = self
                .push_monitor
                .remove(end.tag())
                .unwrap_or((0, 0, 0));
            end.set_push_count(size.2 as u64);
            trace_worker!(
                "output[{:?}]: notify end of {:?} to channel[{}] to worker {}, total pushed {};",
                self.ch_info.source_port,
                end.tag(),
                self.ch_info.id.index,
                self.target_worker,
                size.2
            );
        } else {
            trace_worker!(
                "output[{:?}]: send end event of {:?} of channel[{}] to worker {} to port {:?};",
                self.ch_info.source_port,
                end.tag(),
                self.ch_info.id.index,
                self.target_worker,
                self.ch_info.target_port
            );
        }
        let event = Event::new(self.source_worker, self.ch_info.target_port, EventKind::End(end));
        self.event_emitter
            .send(self.target_worker, event)
    }
}

impl<D: Data> Push<MicroBatch<D>> for EventEmitPush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
        let len = batch.len();
        if let Some(mut end) = batch.take_end() {
            let (seq, _cnt, mut total) = self
                .push_monitor
                .remove(&batch.tag)
                .unwrap_or((0, 0, 0));
            total += len;
            end.total_send = total as u64;
            batch.set_end(end);
            batch.set_seq(seq as u64);
            trace_worker!(
                    "output[{:?}] push last batch(len={}) of {:?} to channel[{}] to worker {}, total pushed {} ;",
                    self.ch_info.source_port,
                    len,
                    batch.tag,
                    self.ch_info.id.index,
                    self.target_worker,
                    total
                );
        } else {
            assert!(len > 0, "push batch size = 0;");
            let (seq, cnt, total) = self.push_monitor.get_mut_or_insert(&batch.tag);
            *cnt += len;
            *total += len;
            batch.set_seq(*seq as u64);
            *seq += 1;
        }
        self.inner.push(batch)
    }

    fn flush(&mut self) -> IOResult<()> {
        let index = self.ch_info.index();
        let target = self.target_worker;
        // trace_worker!(
        //     "output[{:?}] try to flush channel[{}] to worker {};",
        //     self.ch_info.source_port,
        //     self.ch_info.index(),
        //     target
        // );
        for (t, (_, b, _)) in self.push_monitor.iter_mut() {
            if *b > 0 {
                trace_worker!(
                    "output[{:?}] flushed {} data of {:?} to channel[{}] to worker {} ;",
                    self.ch_info.source_port,
                    *b,
                    t,
                    index,
                    target
                );
                *b = 0;
            }
        }
        self.inner.flush()
    }

    fn close(&mut self) -> IOResult<()> {
        self.inner.close()
    }
}
