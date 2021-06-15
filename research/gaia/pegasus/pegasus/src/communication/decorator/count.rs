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

use crate::channel_id::SubChannelId;
use crate::data::DataSet;
use crate::data_plane::{GeneralPush, Push};
use crate::errors::IOResult;
use crate::event::{Event, EventBus, EventKind};
use crate::{Data, Tag, WorkerId};
use std::marker::PhantomData;

pub struct CountedPush<T: Data> {
    pub ch_id: SubChannelId,
    pub source: WorkerId,
    pub target: WorkerId,
    inner: GeneralPush<DataSet<T>>,
    event_bus: EventBus,
    current: Option<(Tag, usize)>,
    _ph: PhantomData<T>,
}

impl<T: Data> CountedPush<T> {
    pub fn new(
        ch_id: SubChannelId, source: WorkerId, target: WorkerId, push: GeneralPush<DataSet<T>>,
        event_bus: &EventBus,
    ) -> Self {
        CountedPush {
            ch_id,
            source,
            target,
            inner: push,
            event_bus: event_bus.clone(),
            current: None,
            _ph: Default::default(),
        }
    }

    #[inline(always)]
    fn send_event(&mut self, t: Tag, size: usize) -> IOResult<()> {
        self.inner.flush()?;
        let event = Event::new(t, self.ch_id.index(), EventKind::Pushed(size));
        self.event_bus.send_to(self.target, event)
    }
}

impl<T: Data> Push<DataSet<T>> for CountedPush<T> {
    fn push(&mut self, msg: DataSet<T>) -> IOResult<()> {
        if let Some((t, mut size)) = self.current.take() {
            if t.eq(&msg.tag) {
                size += msg.len();
                self.current.replace((t, size));
            } else {
                if size > 0 {
                    self.send_event(t, size)?;
                }
                self.current.replace((msg.tag.clone(), msg.len()));
            }
        } else {
            self.current.replace((msg.tag.clone(), msg.len()));
        }
        self.inner.push(msg)
    }

    fn flush(&mut self) -> IOResult<()> {
        if let Some((t, size)) = self.current.take() {
            if size > 0 {
                self.send_event(t, size)?;
            }
        }
        self.inner.flush()
    }

    fn close(&mut self) -> IOResult<()> {
        self.flush()?;
        self.inner.close()
    }
}
