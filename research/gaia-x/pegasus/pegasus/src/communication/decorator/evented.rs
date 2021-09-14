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

pub use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::ScopeStreamPush;
    use crate::data::MicroBatch;
    use crate::data_plane::{GeneralPush, Push};
    use crate::errors::IOResult;
    use crate::event::emitter::EventEmitter;
    use crate::event::{Event, EventKind};
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::tag::tools::map::TidyTagMap;
    use crate::{Data, Tag};

    pub struct EventEmitPush<T: Data> {
        pub ch_info: ChannelInfo,
        pub source_worker: u32,
        pub target_worker: u32,
        inner: GeneralPush<MicroBatch<T>>,
        has_cycles: Arc<AtomicBool>,
        event_emitter: EventEmitter,
        push_counts: TidyTagMap<(usize, usize)>,
    }

    impl<T: Data> EventEmitPush<T> {
        pub fn new(
            info: ChannelInfo, source_worker: u32, target_worker: u32, has_cycles: Arc<AtomicBool>,
            push: GeneralPush<MicroBatch<T>>, emitter: EventEmitter,
        ) -> Self {
            let push_counts = TidyTagMap::new(info.scope_level);
            EventEmitPush {
                ch_info: info,
                source_worker,
                target_worker,
                inner: push,
                has_cycles,
                event_emitter: emitter,
                push_counts,
            }
        }
    }

    impl<T: Data> ScopeStreamPush<MicroBatch<T>> for EventEmitPush<T> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, _: &Tag, msg: MicroBatch<T>) -> IOResult<()> {
            if log_enabled!(log::Level::Trace) {
                let c = self.push_counts.get_mut_or_insert(&msg.tag);
                c.0 += msg.len();
            }
            self.inner.push(msg)
        }

        fn push_last(&mut self, mut msg: MicroBatch<T>, mut end: EndSignal) -> IOResult<()> {
            assert!(!msg.is_empty());
            if log_enabled!(log::Level::Trace) {
                let c = self
                    .push_counts
                    .remove(&end.tag)
                    .unwrap_or((0, 0));
                let size = c.0 + c.1 + msg.len();
                if size > 0 {
                    trace_worker!(
                        "output[{:?}]: push last data of {:?}, total pushed {} into channel[{}] to {};",
                        self.port(),
                        msg.tag,
                        size,
                        self.ch_info.index(),
                        self.target_worker
                    );
                }
            }
            end.seq = msg.seq + 1;

            if end.source_weight.value() == 1 {
                msg.set_end(end);
                self.inner.push(msg)?;
                // self.flush()?;
            } else {
                self.inner.push(msg)?;
                // there need to force flush data stream, because it need to make sure that the events won't in front
                // of the data; It must guarantee that if the event reached, then all data were also reached;
                self.flush()?;
                trace_worker!(
                    "send end event of {:?} on to [worker_{}]: port: {:?} ;",
                    end.tag,
                    self.target_worker,
                    self.ch_info.target_port
                );
                let event = Event::new(self.source_worker, self.ch_info.target_port, EventKind::End(end));
                self.event_emitter
                    .send(self.target_worker, event)?;
            }
            Ok(())
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if end.source_weight.value() == 1 {
                if end.seq > 0 || self.has_cycles.load(Ordering::SeqCst) {
                    if log_enabled!(log::Level::Trace) {
                        let c = self
                            .push_counts
                            .remove(&end.tag)
                            .unwrap_or((0, 0));
                        let size = c.0 + c.1;
                        trace_worker!(
                            "output[{:?}]: notify end of {:?}, total pushed {} into channel[{}] to {};",
                            self.port(),
                            end.tag,
                            size,
                            self.ch_info.index(),
                            self.target_worker
                        );
                    }

                    let mut d = MicroBatch::empty();
                    d.tag = end.tag.clone();
                    d.end = Some(end);
                    self.inner.push(d)?;
                }
                // self.flush()?;
            } else {
                if log_enabled!(log::Level::Trace) {
                    if end.tag.len() == self.push_counts.scope_level as usize {
                        let c = self
                            .push_counts
                            .remove(&end.tag)
                            .unwrap_or((0, 0));
                        let size = c.0 + c.1;
                        trace_worker!(
                            "output[{:?}]: notify end of {:?}, total pushed {} into channel[{}] to {};",
                            self.port(),
                            end.tag,
                            size,
                            self.ch_info.index(),
                            self.target_worker
                        );
                    } else {
                        trace_worker!(
                            "output[{:?}] notify end of {:?} of channel[{}]",
                            self.port(),
                            end.tag,
                            self.ch_info.index()
                        );
                    }
                }

                self.flush()?;
                trace_worker!(
                    "send end event of {:?} on to {}: port: {:?} ;",
                    end.tag,
                    self.target_worker,
                    self.ch_info.target_port
                );
                let event = Event::new(self.source_worker, self.ch_info.target_port, EventKind::End(end));
                self.event_emitter
                    .send(self.target_worker, event)?;
            }
            Ok(())
        }

        fn flush(&mut self) -> IOResult<()> {
            if log_enabled!(log::Level::Trace) {
                let port = self.port();
                trace_worker!(
                    "output[{:?}] flush channel[{}] to {};",
                    port,
                    self.ch_info.index(),
                    self.target_worker
                );
                for (a, b) in self.push_counts.iter_mut() {
                    let cnt = b.0;
                    if cnt > 0 {
                        b.1 += cnt;
                        b.0 = 0;
                        trace_worker!(
                            "output[{:?}] flush {} data of {:?} into channel[{}] to {};",
                            port,
                            cnt,
                            a,
                            self.ch_info.index(),
                            self.target_worker
                        );
                    }
                }
            }
            self.inner.flush()?;
            Ok(())
        }

        fn close(&mut self) -> IOResult<()> {
            self.flush()?;
            self.inner.close()
        }
    }
}

////////////////////////////////////////////////////////
#[cfg(feature = "rob")]
mod rob {
    use pegasus_common::buffer::ReadBuffer;

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
            if end.tag.len() == self.push_monitor.scope_level as usize {
                let size = self
                    .push_monitor
                    .remove(&end.tag)
                    .unwrap_or((0, 0, 0));
                end.seq = size.0 as u64;

                if size.2 == 0
                    && !end.tag.is_root()
                    && !end
                        .update_weight
                        .as_ref()
                        .map(|w| w.contains_source(self.target_worker))
                        .unwrap_or(true)
                {
                    trace_worker!(
                        "output[{:?}] ignore end of {:?} to channel[{}] to worker {};",
                        self.ch_info.source_port,
                        end.tag,
                        self.ch_info.index(),
                        self.target_worker
                    );
                    return Ok(());
                }

                trace_worker!(
                    "output[{:?}]: notify end of {:?} to channel[{}] to worker {}, total pushed {};",
                    self.ch_info.source_port,
                    end.tag,
                    self.ch_info.id.index,
                    self.target_worker,
                    size.2
                );
            }

            if end.source_weight.value() == 1 {
                let mut last = MicroBatch::new(end.tag.clone(), self.source_worker, ReadBuffer::new());
                let seq = end.seq;
                last.set_end(end);
                last.set_seq(seq);
                self.inner.push(last)
            } else {
                trace_worker!(
                    "output[{:?}]: send end event of {:?} of channel[{}] to worker {} to port {:?};",
                    self.ch_info.source_port,
                    end.tag,
                    self.ch_info.id.index,
                    self.target_worker,
                    self.ch_info.target_port
                );
                let event = Event::new(self.source_worker, self.ch_info.target_port, EventKind::End(end));
                self.event_emitter
                    .send(self.target_worker, event)
            }
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for EventEmitPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
            let len = batch.len();
            if batch.is_last() {
                let (seq, _cnt, mut total) = self
                    .push_monitor
                    .remove(&batch.tag)
                    .unwrap_or((0, 0, 0));
                batch.set_seq(seq as u64);
                total += len;
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
            trace_worker!(
                "output[{:?}] flush channel[{}] to worker {};",
                self.ch_info.source_port,
                self.ch_info.index(),
                target
            );
            for (t, (_, b, _)) in self.push_monitor.iter_mut() {
                if *b > 0 {
                    trace_worker!(
                        "output[{:?}] flush {} data of {:?} to channel[{}] to worker {} ;",
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
}
