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

pub trait TaggedInput: AsAny + Stash + Send {

    fn exhausted(&mut self);

    fn is_exhausted(&self) -> bool;

    fn channel(&self) -> ChannelId;
}

#[allow(dead_code)]
pub struct InputHandle<D: Data> {
    pub worker: WorkerId,
    pub ch_id: ChannelId,
    pull: Box<dyn StashedPull<DataSet<D>>>,
    exhausted: bool,
    event_buf: EventsBuffer,
    batch_size: usize
}

impl<D: Data> AsAny for InputHandle<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> Stash for InputHandle<D> {
    fn stash(&mut self, tags: &[Tag]) {
        self.pull.stash(tags)
    }

    fn stash_pop(&mut self, tags: &[Tag]) {
        self.pull.stash_pop(tags)
    }

    fn get_stashed(&self) -> HashSet<&Tag> {
        self.pull.get_stashed()
    }
}

impl<D: Data> TaggedInput for InputHandle<D> {
    fn exhausted(&mut self) {
        self.exhausted = true;
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    fn channel(&self) -> ChannelId {
        self.ch_id
    }
}

impl<D: Data> InputHandle<D> {

    pub fn new<P>(worker: WorkerId, ch: ChannelId, input: P, batch: usize, events_buf: &EventsBuffer) -> Self
        where P: StashedPull<DataSet<D>> + 'static
    {
        InputHandle {
            worker,
            ch_id: ch,
            pull: Box::new(input),
            exhausted: false,
            event_buf: events_buf.clone(),
            batch_size: batch
        }
    }

    #[inline]
    pub fn downcast(origin: &mut Box<dyn TaggedInput>) -> &mut Self {
        // TODO: Is it needed to handle downcast exception ?;
        origin.as_any_mut().downcast_mut::<Self>().expect("downcast InputHandle failure")
    }

    #[inline]
    pub fn next(&mut self) -> IOResult<Option<DataSet<D>>> {
        self.pull.pull()
    }

    pub fn for_each_batch<F>(&mut self, mut logic: F) -> IOResult<usize>
        where F: FnMut(DataSet<D>) -> IOResult<bool>
    {
        if self.exhausted {
            Ok(0)
        } else {
            let mut pulled = 0;
            while let Some(msg) = self.pull.pull()? {
                pulled += msg.len();
                if !logic(msg)? { break }
            }
            Ok(pulled)
        }
    }
}



