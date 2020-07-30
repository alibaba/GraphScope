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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use crate::{WorkerId, ChannelId, Tag};
use crate::dataflow::Dataflow;
use crate::common::Port;
use crate::operator::{Operator, Notification};
use crate::channel::eventio::{EventsBuffer, EventCaster};
use crate::channel::{Edge, IOResult};
use crate::worker::Strategy;
use crate::channel::output::OutputDelta;

#[derive(Debug, Clone, Abomonation, Eq, PartialEq, Serialize, Deserialize)]
pub enum Event {
    ////////////////////// data events //////////////////////

    /// Pushed `n` messages to channel `i` with specific `Tag`;
    Pushed(Tag, usize, ChannelId),
    /// Pulled `n` messages to channel `i` with specific `Tag`;
    Pulled(Tag, usize, ChannelId),
    /// The signal which inspect the worker `i` won't produce any message with the `Tag`;
    END(Tag, WorkerId, ChannelId),
    /// The signal which inspect the worker `i` won't produce any message into the channel;
    EOS(WorkerId, ChannelId),

    ////////////////////// loop status events //////////////////////

    /// Inspect that there are some state sync messages in the channel;
    Iterations(Tag, WorkerId, ChannelId),

    ////////////////////// water mark events //////////////////////

    /// A channel in a worker go up above high water mark.
    HighWaterMark(WorkerId, ChannelId, Tag),
    /// A channel in a worker fall down below high water mark.
    LowWaterMark(WorkerId, ChannelId, Tag),

    // Worker of 'WorkerId' cancel all data with 'Tag' from source to channel of 'ChannelId';
    //  Cancel(Vec<Tag>, WorkerId, ChannelId)
}

impl Event {
    #[inline]
    pub fn channel(&self) -> ChannelId {
        match self {
            Event::Pushed(_, _, channel) => *channel,
            Event::Pulled(_, _, channel) => *channel,
            Event::END(_, _, channel) => *channel,
            Event::EOS(_, channel) => *channel,
            Event::Iterations(_, _, channel) => *channel,
            Event::HighWaterMark(_, channel, _) => *channel,
            Event::LowWaterMark(_, channel, _) => *channel,
            //Event::Cancel(_, _, channel) => *channel,
        }
    }

    #[inline]
    pub fn index(&self) -> u8 {
        match self {
            Event::Pushed(_, _, _) => 0,
            Event::Pulled(_, _, _) => 1,
            Event::END(_, _, _) => 2,
            Event::EOS(_, _) => 3,
            Event::Iterations(_, _, _) => 4,
            Event::HighWaterMark(_, _, _) => 5,
            Event::LowWaterMark(_, _, _) => 6,
            //Event::Cancel(_, _, _) => 7,
        }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Event::Pushed(_, _, _) => "Pushed",
            Event::Pulled(_, _, _) => "Pulled",
            Event::END(_, _, _) => "END",
            Event::EOS(_, _) => "EOS",
            Event::Iterations(_, _, _) => "Iterations",
            Event::HighWaterMark(_, _, _) => "HighWaterMark",
            Event::LowWaterMark(_, _, _) => "LowWaterMark",
            //Event::Cancel(_, _, _) => "Cancel",
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct WaterMark(pub usize, pub usize);

#[derive(Debug)]
pub struct EventDriver {
    outstanding: i64,
    pending_ends: HashSet<WorkerId>,
}

impl EventDriver {
    pub fn new() -> Self {
        EventDriver {
            outstanding: 0,
            pending_ends: HashSet::new(),
        }
    }

    #[inline]
    pub fn remaining(&self) -> i64 {
        self.outstanding
    }
    #[inline]
    pub fn pushed(&mut self, len: usize) {
        assert!(len < ::std::i64::MAX as usize);
        self.outstanding = self.outstanding.checked_add(len as i64).expect("messages count overflow");
    }

    #[inline]
    pub fn pulled(&mut self, len: usize) {
        assert!(len < ::std::i64::MAX as usize);
        self.outstanding = self.outstanding.checked_sub(len as i64).expect("message count overflow");
    }

    #[inline]
    pub fn end_on(&mut self, worker: WorkerId) {
        self.pending_ends.insert(worker);
    }

    #[inline]
    pub fn is_end(&self, guard: usize) -> bool {
        self.outstanding == 0 && self.pending_ends.len() == guard
    }

}


pub struct ChannelEvents {
    pub worker_id: WorkerId,
    pub ch_id: ChannelId,
    pub src: Port,
    pub dst: Port,
    pub local: bool,
    pub scopes: usize,

    drivers: HashMap<Tag, EventDriver>,
    parent_ends: HashSet<Tag>,

    guard: usize,
    peers: Vec<bool>,

    blocked: HashMap<Tag, usize>,
    block_recover: Vec<Tag>,

    hw_notified: RefCell<HashMap<Tag, bool>>,
    notifications: Vec<Notification>,
    is_discard: bool,
    eos: Option<Notification>,
}

impl ChannelEvents {
    pub fn new(edge: &Edge, worker_id: WorkerId, peers: usize) -> Self {
        let guard = if edge.local { 1 } else { peers };
        ChannelEvents {
            worker_id,
            ch_id: edge.id,
            src: edge.source,
            dst: edge.target,
            local: edge.local,
            scopes: edge.scopes,
            guard,
            peers: vec![false;guard],
            drivers: HashMap::new(),
            parent_ends: HashSet::new(),
            blocked: HashMap::new(),
            block_recover: Vec::new(),
            hw_notified: RefCell::new(HashMap::new()),
            notifications: Vec::new(),
            is_discard: false,
            eos: Some(Notification::EOS(edge.id)),
        }
    }

    pub fn update(&mut self, event: Event) {
        match event {
            Event::Pushed(tag, count, _) => {
                debug_assert_eq!(tag.len(), self.scopes, "tag {:?}", tag);
                debug_assert!(count > 0);
                let d = self.drivers.entry(tag).or_insert(EventDriver::new());
                d.pushed(count);
            },
            Event::Pulled(tag, count, _) => {
                debug_assert_eq!(tag.len(), self.scopes);
                debug_assert!(count > 0);
                let d = self.drivers.entry(tag).or_insert(EventDriver::new());
                d.pulled(count);

            },
            Event::END(tag, worker, _) => {
                debug_assert!(self.local || worker.1 < self.peers.len(), "Unknown worker {}", worker.1);
                if tag.len() == self.scopes {
                    let d = self.drivers.entry(tag).or_insert(EventDriver::new());
                    d.end_on(worker);
                    if !self.local {
                        for (i, eos) in self.peers.iter().enumerate() {
                            if *eos {
                                d.end_on(WorkerId(worker.0, i));
                            }
                        }
                    }
                } else {
                    debug_assert!(tag.len() < self.scopes);
                    for (sub, d) in self.drivers.iter_mut() {
                        if tag.is_parent_of(sub) {
                            d.end_on(worker)
                        }
                    }
                    self.parent_ends.insert(tag);
                }
            },
            Event::EOS(worker, _) => {
                if self.local {
                    self.peers[0] = true;
                } else {
                    debug_assert!(worker.1 < self.peers.len(), "Unknown worker {}", worker.1);
                    // flag indicate channel from this worker is data exhausted;
                    self.peers[worker.1] = true;
                }
                self.drivers.iter_mut().for_each(|(_, d)| d.end_on(worker));
            },
            Event::Iterations(tag, src, ch) => {
                self.notifications.push(Notification::Iteration(tag, src, ch));
            }
            //  收到下游算子在这条channel上的高水位信号，表明属于这个Tag的数据的量已经达到了警戒水位。
            //  该事件提醒当前算子应停止或减缓这个Tag的数据的继续输出；
            Event::HighWaterMark(src, ch, tag) => {
                let tag_cur = tag.current();
                let num = self.blocked.entry(tag).or_insert(0);
                *num += 1;
                if log_enabled!(log::Level::Debug) {
                    debug!("Receive HighWaterMark from {}, channel: {}, tag: {}, blocked: {}", src, ch, tag_cur, num);
                }
            },
            Event::LowWaterMark(src, ch, tag) => {
                let tag_cur = tag.current();
                let num = {
                    let num = self.blocked.get_mut(&tag).expect("expect tag in blocked map");
                    *num -= 1;
                    *num
                };
                if num == 0 {
                    self.blocked.remove(&tag);
                    self.block_recover.push(tag);
                }
                if log_enabled!(log::Level::Debug) {
                    debug!("Receive LowWaterMark from {}, channel: {}, tag: {}, blocked: {}", src, ch, tag_cur, num);
                }
            }
        }
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        // trace!("channel {}, local={}, drivers： {:?}, peers {:?}", self.ch_id, self.local, self.drivers, self.peers);
        if self.local {
            self.drivers.is_empty() && self.peers[0]
        } else {
            self.drivers.is_empty() && self.peers.iter().all(|f| *f)
        }
    }

    pub fn get_output_blocked(&self) -> Vec<Tag> {
        let mut tmp = Vec::new();
        for (t, _) in self.blocked.iter() {
        tmp.push(t.clone())
    }
        tmp
    }

    pub fn get_outstanding(&self, tag: &Tag) -> i64 {
        self.drivers.get(tag).map(|d| d.outstanding).unwrap_or(0)
    }

    pub fn print_outstanding_info(&self) {
        if !self.drivers.is_empty() {
            debug!("  channel {}_{}", self.worker_id, self.ch_id);
            for (t, d) in self.drivers.iter() {
                debug!("\t{:?}]\t{}", t, d.outstanding)
            }
        }
    }

    #[inline]
    pub fn get_recovered(&mut self) -> Vec<Tag> {
        std::mem::replace(&mut self.block_recover, Vec::new())
    }

    #[inline]
    pub fn is_discard(&self) -> bool {
        self.is_discard
    }

    #[inline]
    pub fn discard(&mut self) {
        self.is_discard = true;
    }

    #[inline]
    fn check_water_marks(&self, strategy: &Box<dyn Strategy>, event_buf: &EventsBuffer) -> IOResult<()> {
        let worker = self.worker_id;
        let ch_id = self.ch_id;
        let is_local = self.local;
        let hw = &self.hw_notified;
        for (t, s) in self.drivers.iter() {
            let water_mark = strategy.get_water_mark(ch_id.0, t);
            let notified = self.is_hw_notified(t);
            if s.remaining() >= (water_mark.1 as i64) && !notified {
                warn!("channel {} message size of {}({}) exceed high water mark {}", ch_id, t,
                      s.remaining(), water_mark.1);
                let event = Event::HighWaterMark(worker, ch_id, t.clone());
                if is_local {
                    event_buf.push(worker, event)?;
                } else {
                    event_buf.broadcast(event)?;
                }
                hw.borrow_mut().insert(t.clone(), true);
            }

            if s.remaining() <= (water_mark.0 as i64) && notified {
                info!("channel {} messages size of {} fall back to narmal {}/{};", ch_id, t,
                      s.remaining(), water_mark.0);
                let event = Event::LowWaterMark(worker, ch_id, t.clone());
                if is_local {
                    event_buf.push(worker, event)?;
                } else {
                    event_buf.broadcast(event)?;
                }
                hw.borrow_mut().insert(t.clone(), false);
            }
        }
        Ok(())
    }

    pub fn setup_target(&mut self, op: &mut Operator, strategy: &Box<dyn Strategy>, event_buf: &EventsBuffer) -> IOResult<()> {
        for n in self.notifications.drain(..) {
            op.add_notification(n);
        }
        self.check_water_marks(strategy, event_buf)?;

        let guard = self.guard;
        let ch = self.ch_id;
        self.drivers.retain(|t, d| {
            if d.is_end(guard) {
                op.add_notification(Notification::End(ch, t.clone()));
                false
            } else { true }
        });

        let children = &self.drivers;
        self.parent_ends.retain(|parent| {
            if !children.keys().any(|child| parent.is_parent_of(child)) {
                op.add_notification(Notification::End(ch, parent.clone()));
                false
            } else { true }
        });

        if self.is_closed() {
            if let Some(eos) = self.eos.take() {
                op.add_notification(eos);
                self.discard();
            }
        }

        self.drivers.iter()
            .filter(|(_, d)| d.remaining() > 0)
            .for_each(|(t, d)|
                op.add_available_input(t.clone(), d.remaining() as usize)
            );
        Ok(())
    }

    #[inline]
    pub fn is_blocked(&self, inbound: &Tag, delta: &OutputDelta) -> bool {
        self.blocked.keys().any(|out| delta.matcher_of(out).matches(inbound))
    }

    #[inline]
    fn is_hw_notified(&self, tag: &Tag) -> bool {
        *self.hw_notified.borrow().get(tag).unwrap_or(&false)
    }
}

#[inline]
pub fn get_ch_mgr<'a>(ch_mgrs: &'a mut Vec<ChannelEvents>, ch_id: &ChannelId) -> Option<&'a mut ChannelEvents> {
    let index = ch_id.0;
    if index - 1 >= ch_mgrs.len() {
        None
    } else {
        Some(&mut ch_mgrs[index - 1])
    }
}

#[inline]
pub fn ch_mgr<'a>(ch_mgrs: &'a Vec<ChannelEvents>, ch_id: &ChannelId) -> Option<&'a ChannelEvents> {
    let index = ch_id.0;
    if index - 1 >= ch_mgrs.len() {
        None
    } else {
        Some(&ch_mgrs[index - 1])
    }
}

pub struct EventManager {
    pub worker_id: WorkerId,
    in_chs: Vec<Vec<ChannelId>>,
    out_chs: HashMap<usize, Vec<(ChannelId, Port)>>,
    ch_mgrs: Vec<ChannelEvents>,
    //ch_mgrs: HashMap<ChannelId, ChannelEvents>,
    event_caster: EventCaster,
    event_buffer: EventsBuffer,
    deltas: Vec<i64>
}

fn update(worker_id: &WorkerId, manager: &mut Vec<ChannelEvents>,
          updates: &mut VecDeque<Event>, deltas: &mut Vec<i64>) {
    while let Some(event) = updates.pop_front() {
        let ch_id = match &event {
            Event::Pushed(_, count, channel) => {
                deltas[channel.0] += *count as i64;
                *channel
            },
            Event::Pulled(_, count, channel) => {
                deltas[channel.0] -= *count as i64;
                *channel
            }
            Event::END(_, _, channel) => *channel,
            Event::EOS(_, channel) => *channel,
            Event::Iterations(_, _, channel) => *channel,
            Event::HighWaterMark(_, channel, _) => *channel,
            Event::LowWaterMark(_, channel, _) => *channel,
        };
        if let Some(ch_m) = get_ch_mgr(manager, &ch_id) {
            trace!("### Worker[{}]: accept event {:?}", worker_id.1, event);
            ch_m.update(event);
        } else {
            error!("Worker[{}]: Accept event {:?}, channel not found", worker_id.1, event);
        }
    }
}

impl EventManager {
    pub fn new(worker_id: WorkerId, event_caster: EventCaster, event_buffer: EventsBuffer) -> Self {
        EventManager {
            worker_id,
            in_chs: Vec::new(),
            out_chs: HashMap::new(),
            ch_mgrs: Vec::new(),
            event_caster,
            event_buffer,
            deltas: Vec::new(),
        }
    }

    pub fn init(&mut self, df: &Dataflow) {
        let peers = df.peers;
        for edge in df.edges() {
            let port = edge.source;
            let ch_e = ChannelEvents::new(edge, self.worker_id, peers);
            self.out_chs.entry(port.index)
                .or_insert(Vec::new())
                .push((ch_e.ch_id, port));
            while self.in_chs.len() <= edge.target.index {
                self.in_chs.push(Vec::new())
            }
            self.in_chs[edge.target.index].push(ch_e.ch_id);
            self.ch_mgrs.push(ch_e);

            while self.deltas.len() <= edge.id.0 {
                self.deltas.push(0);
            }
        }
        self.ch_mgrs.sort_by(|ch1, ch2|
            ch1.ch_id.cmp(&ch2.ch_id))
    }

    #[inline]
    pub fn get_events_buffer(&self) -> &EventsBuffer {
        &self.event_buffer
    }

    #[inline]
    pub fn push(&mut self) -> IOResult<()> {
        self.event_caster.push()
    }

    #[inline]
    pub fn pull(&mut self) -> IOResult<(&mut Vec<i64>, bool)> {
        self.event_caster.collect()?;
        let updates = self.event_caster.pull()?;
        let has_updates = !updates.is_empty();
        if has_updates {
            update(&self.worker_id, &mut self.ch_mgrs, updates, &mut self.deltas);
        }
        Ok((&mut self.deltas, has_updates))
    }

    #[inline]
    pub fn extract_inbound_events(&mut self, op: &mut Operator, strategy: &Box<dyn Strategy>) -> IOResult<()> {
        let ch_events = &mut self.ch_mgrs;
        let event_buf = &self.event_buffer;
        debug_assert!(op.info().index < self.in_chs.len());
        for id in self.in_chs[op.info().index].iter() {
            if let Some(ch_e) = get_ch_mgr(ch_events, id) {
                if !ch_e.is_discard() {
                    ch_e.setup_target(op, strategy, event_buf)?;
                }
            } else {
                // TODO: Handle error;
                error!("input channel {} of operator {} not found", id, op.info().name);
            }
        }
        Ok(())
    }

    #[inline]
    pub fn is_blocked(&self, op: &Operator, t: &Tag) -> bool {
        if let Some(ch_ids) = self.out_chs.get(&op.info().index) {
            for (id, p) in ch_ids {
                let delta = op.get_output_delta(p.port);
                if let Some(ch_e) = ch_mgr(&self.ch_mgrs, id) {
                    if ch_e.is_blocked(t, &delta) {
                        return true;
                    }
                } else {
                    error!("out events of channel {} not found", id);
                }
            }
            false
        } else {
            false
        }
    }

    #[inline]
    pub fn get_outstanding_size(&self, ch: &ChannelId, tag: &Tag) -> i64 {
        if let Some(ch) = ch_mgr(&self.ch_mgrs, ch) {
            ch.get_outstanding(tag)
        } else {
            error!("Channel manager for channel {} not found;", ch);
            0
        }
    }

    pub fn log(&self) {
        debug!("==========================");
        for m in self.ch_mgrs.iter() {
            if !m.is_closed() {
                m.print_outstanding_info();
            } else {
                debug!("  channel: {}_{} closed;", self.worker_id, m.ch_id);
            }
        }
        debug!("==========================");
    }

    // TODO: Maybe not need;
    pub fn clear(&mut self) {
        let worker = self.worker_id;
        self.ch_mgrs.iter_mut().for_each(|ch| {
             if ch.is_closed() {
                 trace!("### Worker[{}]: destroy channel: {};", worker, ch.ch_id);
                 ch.discard();
             }
        });
    }
}

