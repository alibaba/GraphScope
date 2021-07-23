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

use crate::dataflow::Dataflow;
use crate::errors::{BuildJobError, IOError, IOResult};
use crate::event::io::EventEntrepot;
use crate::event::{ChannelRxState, ChannelTxState, EndOfStream, Event, EventKind};
use crate::graph::Port;
use crate::Tag;
use pegasus_common::rc::RcPointer;
use std::time::Instant;

pub struct EventManager {
    ch_rxs: Vec<RcPointer<ChannelRxState>>,
    ch_txs: Vec<ChannelTxState>,
    entrepot: EventEntrepot,
    discards: Vec<Vec<(Port, u32, Tag)>>,
    collect_cost: u128,
}

impl EventManager {
    pub(crate) fn new(entrepot: EventEntrepot, job: &Dataflow) -> Result<Self, BuildJobError> {
        let mut ch_rxs_opt = Vec::new();
        for op in job.operators.iter() {
            if let Some(op) = op {
                for input in op.inputs() {
                    let index = input.index();
                    while ch_rxs_opt.len() <= index {
                        ch_rxs_opt.push(None);
                    }
                    ch_rxs_opt[index] = Some(input.get_state().clone());
                }
            }
        }

        let mut ch_rxs = Vec::with_capacity(ch_rxs_opt.len());
        let mut ch_txs = Vec::with_capacity(ch_rxs_opt.len());
        ch_rxs.push(RcPointer::new(ChannelRxState::new(0, 0, 0)));
        ch_txs.push(ChannelTxState::new(Default::default()));
        for i in 1..ch_rxs_opt.len() {
            let ch = &mut ch_rxs_opt[i];
            match ch.take() {
                Some(ch) => ch_rxs.push(ch),
                None => {
                    return BuildJobError::server_err(
                        "EventManager#internal error: channel state lost;",
                    )
                }
            }
            match job.graph.get_edge(i) {
                Some(edge) => ch_txs.push(ChannelTxState::new(*edge)),
                None => {
                    return BuildJobError::server_err(
                        "EventManager#internal error: logical edge lost;",
                    )
                }
            }
        }

        Ok(EventManager { ch_rxs, ch_txs, entrepot, discards: vec![], collect_cost: 0 })
    }

    pub fn collect(&mut self) -> IOResult<bool> {
        let start = Instant::now();
        self.entrepot.classify()?;
        let mut updates = self.entrepot.pull()?;
        let mut has_update = false;
        for event in updates.drain(..) {
            trace_worker!("accept event: {:?}", event);
            if event.ch as usize >= self.ch_rxs.len() {
                let worker_id =
                    crate::worker_id::get_current_worker().expect("worker_id not found");
                let id = (worker_id, event.ch).into();
                let err = throw_io_error!(std::io::ErrorKind::NotFound, id);
                return Err(err);
            } else {
                let Event { tag, ch, kind } = event;
                let ch = ch as usize;
                match kind {
                    EventKind::Pushed(size) => {
                        self.ch_rxs[ch].pushed(tag, size);
                        has_update = true;
                    }
                    EventKind::EOS(EndOfStream::OneOf(source)) => {
                        has_update |= self.ch_rxs[ch].give_scope_end_of(tag, source);
                    }
                    EventKind::EOS(EndOfStream::All) => {
                        self.ch_rxs[ch].give_scope_end_all(tag);
                        has_update = true;
                    }
                    EventKind::Discard(source) => {
                        let port = self.ch_txs[ch].port;
                        for tag in self.ch_txs[ch].skip_scope(tag, source as u32).drain(..) {
                            while self.discards.len() <= port.index {
                                self.discards.push(vec![]);
                            }
                            debug_worker!(
                                "get notification try to cancel scope {:?} on port {:?} to ch: {};",
                                tag,
                                port,
                                ch
                            );
                            self.discards[port.index].push((port, ch as u32, tag));
                            has_update = true;
                        }
                    }
                }
            }
        }
        self.collect_cost += start.elapsed().as_micros();
        Ok(has_update)
    }

    #[inline]
    pub fn send_events(&mut self) -> IOResult<()> {
        self.entrepot.flush()
    }

    #[inline]
    pub fn get_discards(&mut self, op_index: usize) -> Option<&mut Vec<(Port, u32, Tag)>> {
        self.discards.get_mut(op_index)
    }

    pub fn close(&mut self) -> IOResult<()> {
        if crate::worker_id::is_in_trace() {
            let cost = self.collect_cost as f64 / 1000.0;
            info_worker!("event collect and dispatch cost {:.1} millis;", cost);
        }
        self.entrepot.close()
    }
}
