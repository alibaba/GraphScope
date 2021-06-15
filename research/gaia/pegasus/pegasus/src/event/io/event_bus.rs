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

use crate::errors::{IOError, IOResult};
use crate::event::Event;
use crate::WorkerId;
use crossbeam_channel::Sender;
use pegasus_common::rc::RcPointer;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::ErrorKind;

/// Shared inter-worker(Process) event-bus;
pub struct EventBus {
    pub worker_id: WorkerId,
    pub(crate) tx: Sender<(WorkerId, Event)>,
    pub(crate) internal: RcPointer<RefCell<VecDeque<Event>>>,
}

impl EventBus {
    pub fn new(worker_id: WorkerId, sender: Sender<(WorkerId, Event)>) -> Self {
        EventBus { worker_id, tx: sender, internal: RcPointer::new(RefCell::new(VecDeque::new())) }
    }

    #[inline]
    pub fn send_self(&self, event: Event) -> IOResult<()> {
        self.internal.borrow_mut().push_back(event);
        Ok(())
    }

    #[inline]
    pub fn send_to(&self, worker_id: WorkerId, event: Event) -> IOResult<()> {
        self.tx.send((worker_id, event)).map_err(|_| {
            error_worker!("EventBus#send event failure as broken pipe;");
            let id = (self.worker_id, 0u32).into();
            throw_io_error!(ErrorKind::BrokenPipe, id)
        })
    }

    pub fn broadcast(&self, event: Event) -> IOResult<()> {
        let result = self.broadcast_exclude(self.worker_id, &event);
        self.internal.borrow_mut().push_back(event);
        result
    }

    #[inline]
    fn broadcast_exclude(&self, worker_id: WorkerId, event: &Event) -> IOResult<()> {
        let mut errors = None;
        for peer in self.worker_id.all_peers() {
            if peer != worker_id {
                if let Err(err) = self.send_to(peer, event.clone()) {
                    errors = Some(err)
                }
            }
        }

        errors
            .map(|e| {
                error_worker!("EventBus#broadcast event failure as {:?};", e);
                Err(e)
            })
            .unwrap_or(Ok(()))
    }
}

impl Clone for EventBus {
    fn clone(&self) -> Self {
        EventBus { worker_id: self.worker_id, tx: self.tx.clone(), internal: self.internal.clone() }
    }
}
