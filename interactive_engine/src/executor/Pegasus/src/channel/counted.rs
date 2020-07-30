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

use std::marker::PhantomData;
use super::*;
use crate::channel::eventio::EventsBuffer;
use crate::event::Event;
use std::collections::{HashMap, HashSet};
use std::collections::vec_deque::VecDeque;

#[allow(dead_code)]
pub struct CountedPush<D, P> {
    ch_id: ChannelId,
    worker: WorkerId,
    target: WorkerId,
    inner: P,
    events_buf: EventsBuffer,
    _ph: PhantomData<D>,
}

impl<D: Data, P: Push<DataSet<D>>> CountedPush<D, P> {
    pub fn new(ch_id: ChannelId, worker: WorkerId, target: WorkerId, push: P,
               events_buf: &EventsBuffer) -> Self{
        CountedPush {
            ch_id,
            worker,
            target,
            inner: push,
            events_buf: events_buf.clone(),
            _ph: Default::default(),
        }
    }
}

impl<D: Data, P: Push<DataSet<D>>> Push<DataSet<D>> for CountedPush<D, P> {

    fn push(&mut self, msg: DataSet<D>) -> Result<(), IOError> {
        let tag = msg.tag().clone();
        let count = msg.len();
        self.inner.push(msg)?;
        self.events_buf.push(self.target, Event::Pushed(tag, count, self.ch_id))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.inner.close()
    }
}

pub struct TagAwarePull<D: Data, P> {
    stashes: HashMap<Tag, Vec<DataSet<D>>>,
    stash_popped: VecDeque<DataSet<D>>,
    inner: P,
}

impl<D: Data, P: Pull<DataSet<D>>> TagAwarePull<D, P> {

    pub fn new(inner: P) -> Self {
        TagAwarePull {
            stashes: HashMap::new(),
            stash_popped: VecDeque::new(),
            inner
        }
    }

    fn stash_pull<F>(&mut self, mut pull: F) -> IOResult<Option<DataSet<D>>>
        where F: FnMut(&mut P) -> IOResult<Option<DataSet<D>>>
    {
        while let Some(popped) = self.stash_popped.pop_front() {
            if let Some(stash) = self.stashes.get_mut(popped.tag()) {
                stash.push(popped);
            } else {
                return Ok(Some(popped));
            }
        }

        loop {
            if let Some(msg) = pull(&mut self.inner)? {
                if let Some(stash) = self.stashes.get_mut(msg.tag()) {
                    stash.push(msg);
                } else {
                    return Ok(Some(msg));
                }
            } else {
                return Ok(None);
            }
        }
    }
}

impl<D: Data, P: Pull<DataSet<D>>> Pull<DataSet<D>> for TagAwarePull<D, P> {

    fn pull(&mut self) -> IOResult<Option<DataSet<D>>> {
        self.stash_pull(|puller| puller.pull())
    }

    fn try_pull(&mut self, timeout: usize) -> IOResult<Option<DataSet<D>>> {
        self.stash_pull(|puller| puller.try_pull(timeout))
    }
}

impl<D: Data, P: Pull<DataSet<D>>> Stash for TagAwarePull<D, P> {
    fn stash(&mut self, tags: &[Tag]) {
        for t in tags {
            if !self.stashes.contains_key(t) {
                self.stashes.insert(t.clone(), Vec::new());
            }
        }
    }

    fn stash_pop(&mut self, tags: &[Tag]) {
        for t in tags {
            if let Some(stashed) = self.stashes.remove(t) {
                self.stash_popped.extend(stashed);
            }
        }
    }

    fn get_stashed(&self) -> HashSet<&Tag> {
        self.stashes.keys().into_iter().collect::<HashSet<_>>()
    }
}

pub struct CountedPull<D, P> {
    ch_id: ChannelId,
    worker: WorkerId,
    inner: P,
    events_buf: EventsBuffer,
    _ph: PhantomData<D>,
}

impl<D: Data, P: Pull<DataSet<D>> + Stash + 'static> CountedPull<D, P> {

    pub fn new(ch: ChannelId, worker: WorkerId, pull: P, events_buf: &EventsBuffer) -> Self {
        CountedPull {
            ch_id: ch,
            worker,
            inner: pull,
            events_buf: events_buf.clone(),
            _ph: Default::default()
        }
    }
}

impl<D: Data, P: Stash + 'static> Stash for CountedPull<D, P> {

    #[inline]
    fn stash(&mut self, tags: &[Tag]) {
        self.inner.stash(tags);
    }

    #[inline]
    fn stash_pop(&mut self, tags: &[Tag]) {
        self.inner.stash_pop(tags);
    }

    #[inline]
    fn get_stashed(&self) -> HashSet<&Tag> {
        self.inner.get_stashed()
    }
}


impl<D: Data, P: Pull<DataSet<D>> + 'static> Pull<DataSet<D>> for CountedPull<D, P> {

    fn pull(&mut self) -> IOResult<Option<DataSet<D>>> {
        if let Some(msg) = self.inner.pull()? {
            let event = Event::Pulled(msg.tag().clone(), msg.len(), self.ch_id);
            self.events_buf.push(self.worker, event)?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    fn try_pull(&mut self, timeout: usize) -> IOResult<Option<DataSet<D>>> {
        if let Some(msg) = self.inner.try_pull(timeout)? {
            let event = Event::Pulled(msg.tag().clone(), msg.len(), self.ch_id);
            self.events_buf.push(self.worker, event)?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }
}
