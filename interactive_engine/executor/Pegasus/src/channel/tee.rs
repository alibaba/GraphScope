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


use super::*;
use crate::channel::eventio::EventsBuffer;
use crate::event::Event;

pub struct Tee<D> {
    shared: Vec<Box<dyn Push<D>>>
}

impl<D: Data> Tee<D> {
    pub fn new() -> Self {
        Tee { shared: Vec::new() }
    }

    pub fn from(shared: Vec<Box<dyn Push<D>>>) -> Self {
        Tee { shared }
    }

    pub fn add_push<P: Push<D> + 'static>(&mut self, p: P) {
        self.shared.push(Box::new(p));
    }
}

impl<D: Data> Push<D> for Tee<D> {
    fn push(&mut self, msg: D) -> IOResult<()> {
        let len = self.shared.len();
        for i in 1..len {
            let copy = msg.clone();
            self.shared[i].push(copy)?;
        }

        if len >= 1 {
            self.shared[0].push(msg)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        for p in self.shared.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for p in self.shared.iter_mut() {
           p.close()?;
        }
        Ok(())
    }
}

pub struct WrappedTee<D> {
    pub worker: WorkerId,
    tee: Tee<D>,
    ch_ids: Vec<(ChannelId, bool)>,
    events_buf: EventsBuffer,
}

impl<D: Data> WrappedTee<D> {

    pub fn new(worker: WorkerId, tee: Tee<D>, ch_ids: Vec<(ChannelId, bool)>, events_buf: &EventsBuffer) -> Self {
        WrappedTee {
            worker,
            tee,
            ch_ids,
            events_buf: events_buf.clone()
        }
    }

    pub fn transmit_end(&mut self, tag: Tag) -> IOResult<()> {
        self.tee.flush()?;
        for (ch_id, local) in self.ch_ids.iter() {
            let event = Event::END(tag.clone(), self.worker, *ch_id);
            if *local {
                self.events_buf.push(self.worker, event)?;
            } else {
                self.events_buf.broadcast(event)?;
            }
        }
        Ok(())
    }
}

impl<D: Data> Push<D> for WrappedTee<D> {
    #[inline]
    fn push(&mut self, msg: D) -> Result<(), IOError> {
        self.tee.push(msg)
    }

    #[inline]
    fn flush(&mut self) -> Result<(), IOError> {
        self.tee.flush()
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.tee.close()?;
        for (ch_id, local) in self.ch_ids.iter() {
            //trace!("Worker[{}]: closing channel {}", self.worker, ch_id.0);
            let event = Event::EOS(self.worker, *ch_id);
            if *local {
                self.events_buf.push(self.worker, event)?;
            } else {
                self.events_buf.broadcast(event)?;
            }
        }
        Ok(())
    }
}
