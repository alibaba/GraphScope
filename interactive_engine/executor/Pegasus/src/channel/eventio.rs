//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::vec_deque::VecDeque;
use super::*;
use crate::event::Event;
use crossbeam_channel::{Receiver, TryRecvError, SendError};

/// Shared inter-worker(Process) event buffer;
pub struct EventsBuffer {
    peers: usize,
    worker_id: WorkerId,
    buffer: Sender<(WorkerId, Event)>
}

impl EventsBuffer {
    pub fn new(peers: usize, worker_id: WorkerId, sender: Sender<(WorkerId, Event)>) -> Self {
        EventsBuffer {
            peers,
            worker_id,
            buffer: sender
        }
    }

    #[inline]
    pub fn push(&self, worker_id: WorkerId, event: Event) -> IOResult<()> {
        if let Err(SendError(e)) = self.buffer.send((worker_id, event)) {
            let msg = format!("EventsBuffer with worker id {} send event {:?} to {} failure;", self.worker_id, e.1, e.0);
            error!("push event error: caused by {}", msg);
            match e.1 {
                Event::LowWaterMark(_, _, _) => Ok(()),
                Event::HighWaterMark(_, _, _) => Ok(()),
                Event::Iterations(_, _, _) => Ok(()),
                _ => Err(IOError::BrokenPipe(msg))
            }
        } else { Ok(()) }
    }

    pub fn broadcast(&self, event: Event) -> IOResult<()> {
        for i in 1..self.peers {
            self.push(WorkerId(self.worker_id.0, i), event.clone())?;
        }
        self.push(WorkerId(self.worker_id.0, 0), event)
    }

    pub fn broadcast_exclude(&self, worker_id: WorkerId, event: Event) -> IOResult<()> {
        debug_assert_eq!(self.worker_id.0, worker_id.0);
        for i in 0..self.peers {
            if i != worker_id.1 {
                self.push(WorkerId(self.worker_id.0, i), event.clone())?;
            }
        }
        Ok(())
    }
}

impl Clone for EventsBuffer {
    fn clone(&self) -> Self {
        EventsBuffer {
            peers: self.peers,
            worker_id: self.worker_id,
            buffer: self.buffer.clone()
        }
    }
}

#[allow(dead_code)]
pub struct EventPush {
    pub(crate) ch_id: ChannelId,
    pub(crate) target: WorkerId,
    inner: Box<dyn Push<Vec<Event>>>,
    buffer: Option<Vec<Event>>
}


impl EventPush {
    pub fn new(ch: ChannelId, source: WorkerId, target: WorkerId,
               parallel: ParallelConfig,
               push: Box<dyn Push<Vec<Event>>>) -> Self {
        if  parallel.processes == 1 || parallel.is_in_local(source.1, target.1) {
            EventPush {
                ch_id: ch,
                target,
                inner: push,
                buffer: None
            }
        } else {
            EventPush {
                ch_id: ch,
                target,
                inner: push,
                buffer: Some(Vec::new()),
            }
        }
    }
}

impl Push<Event> for EventPush {
    #[inline]
    fn push(&mut self, msg: Event) -> IOResult<()> {
        if let Some(mut buffer) = self.buffer.take() {
            buffer.push(msg);
            if buffer.len() == 64 {
                let flush = ::std::mem::replace(&mut buffer, Vec::with_capacity(64));
                self.inner.push(flush)?;
            }
            self.buffer.replace(buffer);
            Ok(())
        } else {
            self.inner.push(vec![msg])
        }
    }

    fn flush(&mut self) -> IOResult<()> {
        if let Some(mut buffer) = self.buffer.take() {
            if !buffer.is_empty() {
                let tmp = ::std::mem::replace(&mut buffer, Vec::with_capacity(64));
                self.inner.push(tmp)?;
            }
            self.buffer.replace(buffer);
        }
        Ok(())
    }

    #[inline]
    fn close(&mut self) -> IOResult<()> {
        self.inner.close()
    }
}

#[allow(dead_code)]
pub struct EventPull {
    pub(crate) ch_id: ChannelId,
    inner: Box<dyn Pull<Vec<Event>>>,
}

impl EventPull {
    pub fn new(ch: ChannelId, pull: Box<dyn Pull<Vec<Event>>>) -> Self {
        EventPull {
            ch_id: ch,
            inner: Box::new(pull)
        }
    }
}

impl Pull<Vec<Event>> for EventPull {
    #[inline]
    fn pull(&mut self) -> IOResult<Option<Vec<Event>>> {
        self.inner.pull()
    }

    #[inline]
    fn try_pull(&mut self, timeout: usize) -> IOResult<Option<Vec<Event>>> {
        self.inner.try_pull(timeout)
    }
}

#[allow(dead_code)]
pub struct EventCaster {
    worker: WorkerId,
    recv: Receiver<(WorkerId, Event)>,
    pushes: Vec<EventPush>,
    pull: EventPull,
    received_events: VecDeque<Event>,
}

impl EventCaster {
    pub fn new(worker: WorkerId, ch_id: ChannelId,
               recv: Receiver<(WorkerId, Event)>,
               parallel: ParallelConfig,
               pushes: Vec<Box<dyn Push<Vec<Event>>>>, pull: Box<dyn Pull<Vec<Event>>>) -> Self {

        let pushes = pushes.into_iter().enumerate()
            .map(|(target, push)| {
                EventPush::new(ch_id, worker, WorkerId(worker.0, target), parallel, push)
            }).collect::<Vec<_>>();

        let pull = EventPull::new(ch_id, pull);

        EventCaster {
            worker,
            recv,
            pushes,
            pull,
            received_events: VecDeque::new(),
        }
    }

    pub fn collect(&mut self) -> IOResult<()> {
        loop {
            match self.recv.try_recv() {
                Ok((id, event)) => {
                    debug_assert_eq!(self.worker.0, id.0);
                    if self.worker.1 == id.1 {
                        self.received_events.push_back(event);
                    } else {
                        self.pushes[id.1].push(event)?;
                    }
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    warn!("Inter-Process event channel disconnected");
                    break
                }
            }
        }
        Ok(())
    }

    pub fn push(&mut self) -> IOResult<()> {
        self.collect()?;
        for i in 0..self.pushes.len() {
            self.pushes[i].flush()?;
        }
        Ok(())
    }

    pub fn pull(&mut self) -> IOResult<&mut VecDeque<Event>> {
        while let Some(events) = self.pull.pull()? {
           self.received_events.extend(events);
        }

        Ok(&mut self.received_events)
    }
}




