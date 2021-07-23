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

use super::Event;
use crossbeam_queue::SegQueue;
use pegasus_common::codec::*;
use std::sync::Arc;

lazy_static! {
    static ref EVENT_BATCH_RECYCLE: Arc<SegQueue<Vec<Event>>> = Arc::new(SegQueue::new());
}

#[derive(Clone, Debug)]
pub struct EventBatch {
    batch: Vec<Event>,
}

impl Encode for EventBatch {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.batch.len() as u32)?;
        for e in self.batch.iter() {
            e.write_to(writer)?;
        }
        Ok(())
    }
}

impl Decode for EventBatch {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut batch: Vec<Event> =
            EVENT_BATCH_RECYCLE.pop().unwrap_or_else(|_| Vec::with_capacity(len));
        for _ in 0..len {
            let event = Event::read_from(reader)?;
            batch.push(event);
        }
        Ok(EventBatch { batch })
    }
}

impl EventBatch {
    #[inline]
    pub fn new() -> Self {
        let batch = EVENT_BATCH_RECYCLE.pop().unwrap_or_else(|_| Vec::with_capacity(64));
        EventBatch { batch }
    }
}

impl std::ops::Deref for EventBatch {
    type Target = Vec<Event>;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

impl std::ops::DerefMut for EventBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.batch
    }
}

impl Drop for EventBatch {
    fn drop(&mut self) {
        let mut batch = ::std::mem::replace(&mut self.batch, vec![]);
        batch.clear();
        if batch.capacity() > 0 {
            EVENT_BATCH_RECYCLE.push(batch);
        }
    }
}

#[derive(Clone, Debug)]
pub enum Events {
    Single(Event),
    Batched(EventBatch),
}

impl Encode for Events {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> ::std::io::Result<()> {
        match self {
            &Events::Batched(ref events) => {
                writer.write_u8(0)?;
                events.write_to(writer)
            },
            &Events::Single(ref event) => {
                writer.write_u8(1)?;
                event.write_to(writer)
            }
        }
    }
}

impl Decode for Events {
    fn read_from<R: ReadExt>(reader: &mut R) -> ::std::io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let batched = EventBatch::read_from(reader)?;
                Ok(Events::Batched(batched))
            }
            1 => {
                let event = Event::read_from(reader)?;
                Ok(Events::Single(event))
            }
            _ => Err(::std::io::Error::new(::std::io::ErrorKind::Other, "unreachable"))
        }
    }
}

mod entrepot;
mod event_bus;
pub use entrepot::EventEntrepot;
pub use event_bus::EventBus;
