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

use crate::api::function::{MultiRouteFunction, RouteFunction};
use crate::channel_id::SubChannelId;
use crate::communication::decorator::count::CountedPush;
use crate::data::DataSet;
use crate::data_plane::Push;
use crate::errors::{IOError, IOResult};
use crate::{Data, Tag};
use crossbeam_channel::{Receiver, Sender};

struct BufferRecycle<D> {
    recycle_hook: Sender<Vec<D>>,
    recycle_bucket: Receiver<Vec<D>>,
    statistics: (usize, usize),
}

impl<D> BufferRecycle<D> {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam_channel::bounded(64);
        BufferRecycle { recycle_hook: tx, recycle_bucket: rx, statistics: (0, 0) }
    }

    pub fn get_recycle_hook(&self) -> &Sender<Vec<D>> {
        &self.recycle_hook
    }

    #[inline]
    pub fn try_recycle(&mut self) -> Option<Vec<D>> {
        if let Ok(v) = self.recycle_bucket.try_recv() {
            self.statistics.0 += 1;
            Some(v)
        } else {
            self.statistics.1 += 1;
            None
        }
    }
}

impl<D> Clone for BufferRecycle<D> {
    fn clone(&self) -> Self {
        BufferRecycle {
            recycle_hook: self.recycle_hook.clone(),
            recycle_bucket: self.recycle_bucket.clone(),
            statistics: (0, 0),
        }
    }
}

struct BufferedPush<D: Data> {
    batch_size: usize,
    push: CountedPush<D>,
    buffer: Vec<D>,
    recycle: BufferRecycle<D>,
}

impl<D: Data> BufferedPush<D> {
    fn push_batch(&mut self, msg: DataSet<D>) -> IOResult<()> {
        self.push.push(msg)
    }

    fn push(&mut self, msg: D) -> bool {
        self.buffer.push(msg);
        self.buffer.len() == self.batch_size
    }

    fn flush(&mut self, tag: Tag) -> IOResult<()> {
        let new_buffer =
            self.recycle.try_recycle().unwrap_or_else(|| Vec::with_capacity(self.batch_size));
        let buffer = std::mem::replace(&mut self.buffer, new_buffer);
        let batch = DataSet::with_hook(tag, buffer, self.recycle.get_recycle_hook());
        self.push.push(batch)
    }

    #[inline]
    fn flush_push(&mut self) -> IOResult<()> {
        self.push.flush()
    }

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn close(&mut self) -> IOResult<()> {
        self.push.close()
    }
}

enum RoutingRule<D: Data> {
    ToOne(Box<dyn RouteFunction<D>>),
    ToSome(Box<dyn MultiRouteFunction<D>>),
    ToAll,
}

pub struct ExchangePush<D: Data> {
    pub batch_size: usize,
    ch_id: SubChannelId,
    pushes: Vec<BufferedPush<D>>,
    current: Option<Tag>,
    routing: RoutingRule<D>,
    mask: Option<u64>,
}

impl<D: Data> ExchangePush<D> {
    fn new(
        batch_size: usize, ch_id: SubChannelId, pushes: Vec<CountedPush<D>>,
        routing: RoutingRule<D>,
    ) -> Self {
        let mask = if (pushes.len() & (pushes.len() - 1)) == 0 {
            let mask = (pushes.len() - 1) as u64;
            Some(mask)
        } else {
            None
        };

        let mut buffer_pushes = Vec::with_capacity(pushes.len());
        let recycle = BufferRecycle::new();
        for push in pushes {
            buffer_pushes.push(BufferedPush {
                batch_size,
                push,
                buffer: vec![],
                recycle: recycle.clone(),
            });
        }

        ExchangePush { batch_size, pushes: buffer_pushes, ch_id, current: None, routing, mask }
    }

    pub fn exchange_to_one(
        batch_size: usize, ch_id: SubChannelId, pushes: Vec<CountedPush<D>>,
        routing: Box<dyn RouteFunction<D>>,
    ) -> Self {
        let routing = RoutingRule::ToOne(routing);
        ExchangePush::new(batch_size, ch_id, pushes, routing)
    }

    pub fn exchange_to_some(
        batch_size: usize, ch_id: SubChannelId, pushes: Vec<CountedPush<D>>,
        routing: Box<dyn MultiRouteFunction<D>>,
    ) -> Self {
        let routing = RoutingRule::ToSome(routing);
        ExchangePush::new(batch_size, ch_id, pushes, routing)
    }

    pub fn broadcast(batch_size: usize, ch_id: SubChannelId, pushes: Vec<CountedPush<D>>) -> Self {
        let routing = RoutingRule::ToAll;
        ExchangePush::new(batch_size, ch_id, pushes, routing)
    }

    #[inline]
    fn push_to(&mut self, data: D, index: usize) -> IOResult<()> {
        if self.pushes[index].push(data) {
            self.flush_buffer(index)?;
        }
        Ok(())
    }

    #[inline]
    fn flush_buffer(&mut self, index: usize) -> IOResult<()> {
        if let Some(tag) = self.current.as_ref() {
            self.pushes[index].flush(tag.clone())
        } else {
            Ok(())
        }
    }

    #[inline]
    fn apply_router(&mut self, mut msg: DataSet<D>, routing: &RoutingRule<D>) -> IOResult<()> {
        match routing {
            RoutingRule::ToOne(r) => {
                if let Some(mask) = self.mask {
                    for data in msg.drain(..) {
                        match r.route(&data) {
                            Ok(index) => {
                                let index = (index & mask) as usize;
                                self.push_to(data, index)?;
                            }
                            Err(e) => {
                                let mut err = throw_io_error!();
                                err.set_cause(e);
                                return Err(err);
                            }
                        }
                    }
                } else {
                    let mask = self.pushes.len() as u64;
                    for data in msg.drain(..) {
                        match r.route(&data) {
                            Ok(index) => {
                                let index = (index % mask) as usize;
                                self.push_to(data, index)?;
                            }
                            Err(e) => {
                                let mut err = throw_io_error!();
                                err.set_cause(e);
                                return Err(err);
                            }
                        }
                    }
                }
            }
            RoutingRule::ToSome(r) => {
                if let Some(mask) = self.mask {
                    for data in msg.drain(..) {
                        let targets = match r.route(&data) {
                            Ok(targets) => targets,
                            Err(e) => {
                                let mut err = throw_io_error!();
                                err.set_cause(e);
                                return Err(err);
                            }
                        };
                        if !targets.is_empty() {
                            if targets.len() > 1 {
                                for t in &targets[1..] {
                                    let route_index = (*t & mask) as usize;
                                    self.push_to(data.clone(), route_index)?;
                                }
                            }
                            let route_index = (targets[0] & mask) as usize;
                            self.push_to(data, route_index)?;
                        }
                    }
                } else {
                    let mask = self.pushes.len() as u64;
                    for data in msg.drain(..) {
                        let targets = match r.route(&data) {
                            Ok(targets) => targets,
                            Err(e) => {
                                let mut err = throw_io_error!();
                                err.set_cause(e);
                                return Err(err);
                            }
                        };
                        if !targets.is_empty() {
                            if targets.len() > 1 {
                                for t in &targets[1..] {
                                    let route_index = (*t % mask) as usize;
                                    self.push_to(data.clone(), route_index)?;
                                }
                            }
                            let route_index = (targets[0] % mask) as usize;
                            self.push_to(data, route_index)?;
                        }
                    }
                }
            }
            RoutingRule::ToAll => {
                for i in 1..self.pushes.len() {
                    for data in msg.iter() {
                        self.push_to(data.clone(), i)?;
                    }
                }
                self.pushes[0].push_batch(msg)?;
            }
        }
        Ok(())
    }
}

impl<D: Data> Push<DataSet<D>> for ExchangePush<D> {
    fn push(&mut self, msg: DataSet<D>) -> Result<(), IOError> {
        if msg.is_empty() {
            return Ok(());
        }

        if self.pushes.len() == 1 {
            self.pushes[0].push_batch(msg)?;
            return Ok(());
        }

        if let Some(tag) = self.current.as_mut() {
            if tag != &msg.tag {
                *tag = msg.tag.clone();
                std::mem::drop(tag);
                self.flush()?;
            }
        } else {
            self.current = Some(msg.tag.clone());
        }

        let routing = std::mem::replace(&mut self.routing, RoutingRule::ToAll);
        let result = self.apply_router(msg, &routing);
        self.routing = routing;
        result
    }

    fn check_failure(&mut self) -> Option<DataSet<D>> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<(), IOError> {
        for i in 0..self.pushes.len() {
            if self.pushes[i].len() > 0 {
                self.flush_buffer(i)?;
            }
            self.pushes[i].flush_push()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.flush()?;
        let mut st = (0, 0);
        for p in self.pushes.iter_mut() {
            p.close()?;
            let st_tmp = p.recycle.statistics;
            st.0 += st_tmp.0;
            st.1 += st_tmp.1;
        }

        if crate::worker_id::is_in_trace() {
            info_worker!("channel {:?} exchange buffer reuse: [{} / {}]", self.ch_id, st.0, st.1);
        }

        Ok(())
    }
}
