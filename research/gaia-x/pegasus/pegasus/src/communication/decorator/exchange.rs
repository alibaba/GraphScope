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

use crate::api::function::{RouteFunction, FnResult};
use crate::channel_id::ChannelInfo;
use crate::communication::decorator::buffered::BufferedPush;
use crate::communication::decorator::evented::ControlPush;
use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
use crate::data::DataSet;
use crate::data_plane::GeneralPush;
use crate::errors::IOResult;
use crate::event::emitter::EventEmitter;
use crate::graph::Port;
use crate::progress::{EndSignal, Weight};
use crate::{Data, Tag};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::rc::Rc;

pub struct ExchangeMiniBatchPush<D: Data> {
    pub ch_info: ChannelInfo,
    pushes: Vec<BufferedPush<D, ControlPush<D>>>,
    routing: Box<dyn RouteFunction<D>>,
    magic: Magic,
}

#[derive(Copy, Clone)]
enum Magic {
    Modulo(u64),
    And(u64),
}

impl Magic {
    #[inline(always)]
    pub fn exec(&self, id: u64) -> u64 {
        match self {
            Magic::Modulo(x) => id % *x,
            Magic::And(x) => id & *x,
        }
    }
}

impl<D: Data> ExchangeMiniBatchPush<D> {
    pub fn new(
        ch_info: ChannelInfo, pushes: Vec<BufferedPush<D, ControlPush<D>>>,
        routing: Box<dyn RouteFunction<D>>,
    ) -> Self {
        let len = pushes.len();
        let magic =
            if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
        ExchangeMiniBatchPush { ch_info, pushes, routing, magic }
    }

    fn update_weight(&self, end: &mut EndSignal, target: Option<usize>) {
        // let total = crate::worker_id::get_current_worker_uncheck().peers;
        if end.source_weight.value() < self.pushes.len() {
            let mut weight = Weight::partial_empty();
            if let Some(t) = target {
                weight.add_source(t as u32);
            }
            for (index, p) in self.pushes.iter().enumerate() {
                if Some(index) != target && p.has_buffer(&end.tag) {
                    weight.add_source(index as u32);
                }
            }
            if weight.value() > 1 {
                trace_worker!("try to update eos weight to {:?} of scope {:?}", weight, end.tag);
                end.update_to(weight);
            }
        }
    }
}

impl<D: Data> ScopeStreamBuffer for ExchangeMiniBatchPush<D> {
    fn scope_size(&self) -> usize {
        let len = self.pushes.len();
        if len == 0 {
            0
        } else {
            let mut size = !0u32 as usize;
            for i in 0..len {
                size = std::cmp::min(self.pushes[i].scope_size(), size);
            }
            size
        }
    }

    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
        for push in &mut self.pushes[1..] {
            push.ensure_capacity(tag)?;
        }
        self.pushes[0].ensure_capacity(tag)
    }

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
        for push in self.pushes.iter_mut() {
            push.flush_scope(tag)?;
        }
        Ok(())
    }
}

impl<D: Data> ScopeStreamPush<D> for ExchangeMiniBatchPush<D> {
    fn port(&self) -> Port {
        self.ch_info.source_port
    }

    fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
        let len = self.pushes.len();
        if len == 1 {
            return self.pushes[0].push(tag, msg);
        }

        let target = self.magic.exec(self.routing.route(&msg)?) as usize;
        self.pushes[target].push(tag, msg)
    }

    fn push_last(&mut self, msg: D, mut end: EndSignal) -> IOResult<()> {
        let len = self.pushes.len();
        if len == 1 {
            return self.pushes[0].push_last(msg, end);
        }

        let target = self.magic.exec(self.routing.route(&msg)?) as usize;
        self.update_weight(&mut end, Some(target));

        for (i, p) in self.pushes.iter_mut().enumerate() {
            if i != target {
                p.notify_end(end.clone())?;
            }
        }

        self.pushes[target].push_last(msg, end)
    }

    fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        if self.pushes.len() == 1 {
            self.pushes[0].push_iter(tag, iter)
        } else {
            match self.magic {
                Magic::Modulo(x) => {
                    for next in iter {
                        let idx = self.routing.route(&next)? % x;
                        self.pushes[idx as usize].push(tag, next)?;
                    }
                }
                Magic::And(x) => {
                    for next in iter {
                        let idx = self.routing.route(&next)? & x;
                        self.pushes[idx as usize].push(tag, next)?;
                    }
                }
            }
            Ok(())
        }
    }

    fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
        self.update_weight(&mut end, None);
        for p in &mut self.pushes[1..] {
            p.notify_end(end.clone())?;
        }
        self.pushes[0].notify_end(end)
    }

    fn flush(&mut self) -> IOResult<()> {
        for push in self.pushes.iter_mut() {
            push.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        self.flush()?;
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

pub struct ExchangeByScopePush<D: Data> {
    pub ch_info: ChannelInfo,
    pushes: Vec<ControlPush<D>>,
    magic: Magic,
    has_cycles: Arc<AtomicBool>,
}

impl<D: Data> ExchangeByScopePush<D> {
    pub fn new(
        ch_info: ChannelInfo, has_cycles: Arc<AtomicBool>, pushes: Vec<GeneralPush<DataSet<D>>>,
        event_emitter: &EventEmitter,
    ) -> Self {
        assert!(ch_info.scope_level > 0);
        let mut decorated = Vec::with_capacity(pushes.len());
        let source = crate::worker_id::get_current_worker().index;
        let len = pushes.len();
        for (i, p) in pushes.into_iter().enumerate() {
            let has_cycles = Arc::new(AtomicBool::new(true));
            let p = ControlPush::new(ch_info, source, i as u32, has_cycles, p, event_emitter.clone());
            decorated.push(p);
        }
        let magic =
            if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
        ExchangeByScopePush { ch_info, pushes: decorated, magic, has_cycles }
    }
}

impl<D: Data> ScopeStreamPush<DataSet<D>> for ExchangeByScopePush<D> {
    fn port(&self) -> Port {
        self.ch_info.source_port
    }

    fn push(&mut self, tag: &Tag, msg: DataSet<D>) -> IOResult<()> {
        let idx = tag.current_uncheck() as u64;
        let offset = self.magic.exec(idx) as usize;
        self.pushes[offset].push(tag, msg)
    }

    fn push_last(&mut self, msg: DataSet<D>, mut end: EndSignal) -> IOResult<()> {
        let idx = end.tag.current_uncheck() as u64;
        let offset = self.magic.exec(idx) as usize;
        if self.has_cycles.load(Ordering::SeqCst) {
            for (i, p) in self.pushes.iter_mut().enumerate() {
                if i != offset {
                    p.notify_end(end.clone())?;
                }
            }
        } else {
            // end is only send to aggregate target,
            end.update_weight = Some(Weight::single());
        }
        self.pushes[offset].push_last(msg, end)
    }

    fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
        if end.tag.len() < self.ch_info.scope_level || self.has_cycles.load(Ordering::SeqCst) {
            for p in self.pushes.iter_mut() {
                p.notify_end(end.clone())?;
            }
        } else {
            let idx = end.tag.current_uncheck() as u64;
            let offset = self.magic.exec(idx) as usize;
            self.pushes[offset].notify_end(end)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////
#[allow(dead_code)]
struct Exchange<D> {
    magic: Magic,
    targets: u64,
    router: Rc<dyn RouteFunction<D>>,
}

/// Won't be used by more than one thread at same time;
/// It is safe to send to another thread with all rc points moved together.
unsafe impl<D: Send> Send for Exchange<D> { }

#[allow(dead_code)]
impl<D: Data> Exchange<D> {
    #[inline]
    fn route(&self, item: &D) -> FnResult<u64> {
        let par_key = self.router.route(item)?;
        Ok(self.magic.exec(par_key))
    }

    fn share(&self) -> Exchange<D> {
        Exchange {
            magic: self.magic,
            targets: self.targets,
            router: self.router.clone(),
        }
    }
}