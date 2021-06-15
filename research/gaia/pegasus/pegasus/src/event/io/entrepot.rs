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

use crate::data_plane::{GeneralPull, GeneralPush, Pull, Push};
use crate::errors::{BuildJobError, IOResult};
use crate::event::io::{EventBatch, EventBus, Events};
use crate::event::{Event, EventKind};
use crate::{JobConf, WorkerId};
use crossbeam_channel::{Receiver, TryRecvError};
use pegasus_common::rc::RcPointer;
use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::sync::Arc;

#[allow(dead_code)]
pub struct EventPush {
    pub(crate) source: WorkerId,
    pub(crate) target: WorkerId,
    inner: GeneralPush<Events>,
    buffer: Option<EventBatch>,
}

impl EventPush {
    pub fn new(source: WorkerId, target: WorkerId, push: GeneralPush<Events>) -> Self {
        if push.is_local() {
            EventPush { source, target, inner: push, buffer: None }
        } else {
            EventPush { source, target, inner: push, buffer: Some(EventBatch::new()) }
        }
    }
}

impl Push<Event> for EventPush {
    #[inline]
    fn push(&mut self, msg: Event) -> IOResult<()> {
        match msg.kind {
            EventKind::Discard(_) => {
                if let Err(e) = self.inner.push(Events::Single(msg)) {
                    // ignore the error of pushing discard events
                    info_worker!("Ignore pushing event failure {:?}, because the target worker has already exited", e);
                }
            }
            _ => {
                if let Some(mut buffer) = self.buffer.take() {
                    buffer.push(msg);
                    if buffer.len() == 64 {
                        let batch = std::mem::replace(&mut buffer, EventBatch::new());
                        self.inner.push(Events::Batched(batch))?;
                    }
                    self.buffer.replace(buffer);
                } else {
                    self.inner.push(Events::Single(msg))?;
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        if let Some(mut buffer) = self.buffer.take() {
            if !buffer.is_empty() {
                let batch = std::mem::replace(&mut buffer, EventBatch::new());
                self.inner.push(Events::Batched(batch))?;
            }
            self.buffer = Some(buffer);
        }
        self.inner.flush()
    }

    #[inline]
    fn close(&mut self) -> IOResult<()> {
        self.flush()?;
        self.inner.close().ok();
        Ok(())
    }
}

pub struct EventEntrepot {
    pub worker_id: WorkerId,
    recv: Receiver<(WorkerId, Event)>,
    pushes: Vec<EventPush>,
    pull: GeneralPull<Events>,
    received: RcPointer<RefCell<VecDeque<Event>>>,
}

impl EventEntrepot {
    pub fn new(
        event_bus: EventBus, recv: Receiver<(WorkerId, Event)>, conf: &Arc<JobConf>,
    ) -> Result<Self, BuildJobError> {
        let mut event_pushes = Vec::with_capacity(conf.total_workers());
        let EventBus { worker_id, tx: _, internal } = event_bus;
        let ch_res = crate::communication::build_channel::<Events>(0, conf)?;
        let (pushes, pull) = ch_res.take();
        for (push, target) in pushes.into_iter().zip(worker_id.all_peers()) {
            event_pushes.push(EventPush::new(worker_id, target, push));
        }
        Ok(EventEntrepot { worker_id, recv, pushes: event_pushes, pull, received: internal })
    }

    pub fn classify(&mut self) -> IOResult<()> {
        let mut received = self.received.borrow_mut();
        loop {
            match self.recv.try_recv() {
                Ok((id, event)) => {
                    if self.worker_id.index == id.index {
                        received.push_back(event);
                    } else {
                        self.pushes[id.index as usize].push(event)?;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    warn_worker!("event entrepot disconnected;");
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        for i in 0..self.pushes.len() {
            self.pushes[i].flush()?;
        }
        Ok(())
    }

    pub fn pull(&mut self) -> IOResult<RefMut<VecDeque<Event>>> {
        let mut received = self.received.borrow_mut();
        loop {
            match self.pull.pull() {
                Ok(Some(Events::Single(e))) => {
                    received.push_back(e);
                }
                Ok(Some(Events::Batched(mut ec))) => {
                    for e in ec.drain(..) {
                        received.push_back(e);
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    if err.is_broken_pipe() {
                        warn_worker!("Event exhausted;");
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        Ok(received)
    }

    pub fn close(&mut self) -> IOResult<()> {
        for pusher in self.pushes.iter_mut() {
            if let Err(err) = pusher.close() {
                warn_worker!("EventPush close failure: {:?}", err);
            }
        }
        Ok(())
    }
}
