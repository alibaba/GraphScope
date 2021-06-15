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
use crate::errors::{IOResult, JobExecError};
use crate::event::EventManager;
use std::time::Instant;

mod op_runtime;
pub(crate) use op_runtime::OpRuntime;

pub struct Schedule {
    pub step_count: usize,
    memory_limit: u32,
    event_manager: EventManager,
    is_ready: bool,
    is_closed: bool,
}

impl Schedule {
    pub fn new(memory_limit: u32, event_manager: EventManager) -> Self {
        Schedule { step_count: 0, memory_limit, event_manager, is_ready: true, is_closed: false }
    }

    #[inline]
    pub(crate) fn check_ready(&mut self) -> IOResult<bool> {
        if self.is_ready {
            Ok(true)
        } else {
            self.event_manager.collect()
        }
    }

    pub(crate) fn step(&mut self, task: &mut Dataflow) -> Result<bool, JobExecError> {
        if crate::worker_id::is_in_trace() {
            info_worker!(" ==> start \tstep {}; ", self.step_count);
        }
        let start = Instant::now();
        self.is_ready = false;
        self.event_manager.collect()?;
        let ops = &mut task.operators;
        if self.memory_limit > 0 {
            let mut index = ops.len();
            while index > 0 {
                if let Some(mut op) = ops[index - 1].take() {
                    if !self.fire_operator(&mut op)? {
                        ops[index - 1].replace(op);
                    } else {
                        op.close();
                    }
                }
                index -= 1;
            }
        } else {
            let len = ops.len();
            for i in 0..len {
                if let Some(mut op) = ops[i].take() {
                    if !self.fire_operator(&mut op)? {
                        ops[i].replace(op);
                    } else {
                        op.close();
                    }
                }
            }
        }
        self.event_manager.send_events()?;

        for op in ops {
            if let Some(op) = op {
                if op.check_ready() {
                    // debug_worker!("operator {} is ready;", op.meta.name);
                    self.is_ready = true;
                    break;
                }
            }
        }

        if crate::worker_id::is_in_trace() {
            info_worker!(
                " ==> finish\tstep {}\tcost: {:?} is_ready={}\t=======",
                self.step_count,
                start.elapsed(),
                self.is_ready
            );
        }
        self.step_count += 1;
        Ok(self.is_ready)
    }

    fn fire_operator(&mut self, op: &mut OpRuntime) -> Result<bool, JobExecError> {
        let op_index = op.meta.index;
        if let Some(discards) = self.event_manager.get_discards(op_index) {
            for (port, ch, skip) in discards.drain(..) {
                op.cancel(port.port, ch, skip)?;
            }
        }
        let mut is_finished = false;
        if op.check_ready() {
            match op.fire() {
                Ok(x) => is_finished = x,
                Err(e) => {
                    if !e.can_be_retried() {
                        return Err(e);
                    }
                }
            }
            self.event_manager.collect()?;
            self.event_manager.send_events()?;
        }
        Ok(is_finished)
    }

    pub fn close(&mut self) -> IOResult<()> {
        if !self.is_closed {
            self.is_closed = true;
            self.event_manager.collect()?;
            self.event_manager.send_events()?;
            self.event_manager.close()
        } else {
            Ok(())
        }
    }
}
