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
use crate::communication::decorator::ScopeStreamPush;
use crate::data::DataSet;
use crate::data_plane::{GeneralPush, Push};
use crate::errors::IOResult;
use crate::event::emitter::EventEmitter;
use crate::event::{Event, Signal};
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct ControlPush<T: Data> {
    pub ch_info: ChannelInfo,
    pub source_worker: u32,
    pub target_worker: u32,
    inner: GeneralPush<DataSet<T>>,
    has_cycles: Arc<AtomicBool>,
    event_emitter: EventEmitter,
    push_counts: TidyTagMap<(usize, usize)>,
}

impl<T: Data> ControlPush<T> {
    pub fn new(
        info: ChannelInfo, source_worker: u32, target_worker: u32, has_cycles: Arc<AtomicBool>,
        push: GeneralPush<DataSet<T>>, emitter: EventEmitter,
    ) -> Self {
        let push_counts = TidyTagMap::new(info.scope_level);
        ControlPush {
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

impl<T: Data> ScopeStreamPush<DataSet<T>> for ControlPush<T> {
    fn port(&self) -> Port {
        self.ch_info.source_port
    }

    fn push(&mut self, _: &Tag, msg: DataSet<T>) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let c = self.push_counts.get_mut_or_insert(&msg.tag);
            c.0 += msg.len();
        }
        self.inner.push(msg)
    }

    fn push_last(&mut self, mut msg: DataSet<T>, mut end: EndSignal) -> IOResult<()> {
        assert!(!msg.is_empty());
        if log_enabled!(log::Level::Trace) {
            let c = self
                .push_counts
                .remove(&end.tag)
                .unwrap_or((0, 0));
            let size = c.0 + c.1 + msg.len();
            if size > 0 {
                trace_worker!(
                    "output[{:?}]: push last data of {:?}, total pushed {} to {};",
                    self.port(),
                    msg.tag,
                    size,
                    self.target_worker
                );
            }
        }
        end.seq = msg.seq + 1;

        if end.source_weight.value() == 1 {
            msg.set_last(end);
            self.inner.push(msg)?;
            // self.flush()?;
        } else {
            self.inner.push(msg)?;
            // there need to force flush data stream, because it need to make sure that the events won't in front
            // of the data; It must guarantee that if the event reached, then all data were also reached;
            self.flush()?;
            trace_worker!(
                "send end event of {:?} on to [worker {}: port: {:?} ;",
                end.tag,
                self.target_worker,
                self.ch_info.target_port
            );
            let event = Event::new(self.source_worker, self.ch_info.target_port, Signal::EndSignal(end));
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
                        "output[{:?}]: notify end of {:?}, total pushed {} to {};",
                        self.port(),
                        end.tag,
                        size,
                        self.target_worker
                    );
                }

                let mut d = DataSet::empty();
                d.tag = end.tag.clone();
                d.end = Some(end);
                self.inner.push(d)?;
            }
            // self.flush()?;
        } else {
            if log_enabled!(log::Level::Trace) {
                if end.tag.len() == self.push_counts.scope_level {
                    let c = self
                        .push_counts
                        .remove(&end.tag)
                        .unwrap_or((0, 0));
                    let size = c.0 + c.1;
                    trace_worker!(
                        "output[{:?}]: notify end of {:?}, total pushed {} to {};",
                        self.port(),
                        end.tag,
                        size,
                        self.target_worker
                    );
                } else {
                    trace_worker!("output[{:?}] notify end of {:?}", self.port(), end.tag);
                }
            }

            self.flush()?;
            trace_worker!(
                "send end event of {:?} on to [worker {}]: port: {:?} ;",
                end.tag,
                self.target_worker,
                self.ch_info.target_port
            );
            let event = Event::new(self.source_worker, self.ch_info.target_port, Signal::EndSignal(end));
            self.event_emitter
                .send(self.target_worker, event)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let port = self.port();
            trace_worker!("output[{:?}] flush;", port);
            for (a, b) in self.push_counts.iter_mut() {
                let cnt = b.0;
                if cnt > 0 {
                    b.1 += cnt;
                    b.0 = 0;
                    trace_worker!(
                        "output[{:?}] flush {} data of {:?} to {};",
                        port,
                        cnt,
                        a,
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
