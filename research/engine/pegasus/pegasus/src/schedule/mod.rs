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

use std::time::Instant;

use crate::data_plane::GeneralPull;
use crate::dataflow::Dataflow;
use crate::errors::{IOResult, JobExecError};
use crate::event::emitter::{EventCollector, EventEmitter};
use crate::event::Event;
use crate::schedule::operator::OperatorScheduler;
use crate::schedule::state::inbound::InputEndNotify;
use crate::schedule::state::outbound::OutputCancelState;

pub(crate) mod operator;
pub(crate) mod state;
pub mod strategies;

pub trait StepStrategy: Send + 'static {
    fn make_step(&mut self, task: &Dataflow) -> Result<(), JobExecError>;
}

pub struct Schedule {
    pub step_count: usize,
    event_emitter: EventEmitter,
    event_collector: EventCollector,
    sch_ops: Vec<OperatorScheduler>,
    strategy: Box<dyn StepStrategy>,
}

impl Schedule {
    pub fn new(event_emitter: EventEmitter, event_pull: GeneralPull<Event>) -> Self {
        let event_collector = EventCollector::new(event_pull);
        Schedule {
            step_count: 0,
            event_emitter,
            event_collector,
            sch_ops: vec![],
            strategy: Box::new(strategies::WaterfallStrategy::default()),
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub fn reset_step_strategy<S: StepStrategy>(&mut self, strategy: S) {
        self.strategy = Box::new(strategy);
    }

    pub fn add_schedule_op(
        &mut self, index: usize, scope_level: u32, inputs_notify: Vec<Option<Box<dyn InputEndNotify>>>,
        outputs_cancel: Vec<Option<OutputCancelState>>,
    ) {
        let op = OperatorScheduler::new(index, scope_level, inputs_notify, outputs_cancel);
        self.sch_ops.push(op);
    }

    pub fn try_notify(&mut self) -> Result<(), JobExecError> {
        self.event_collector.collect()?;
        let updates = self.event_collector.get_updates();
        if !updates.is_empty() {
            for event in updates.drain(..) {
                let index = event.target_port.index;
                assert!(index < self.sch_ops.len());
                self.sch_ops[index].accept(event)?;
            }
        }
        Ok(())
    }

    pub fn step(&mut self, task: &Dataflow) -> Result<(), JobExecError> {
        debug_worker!("========== start step {} ==========; ", self.step_count);
        let start = Instant::now();
        self.try_notify()?;

        for i in (0..task.operator_length()).rev() {
            let discards = self.sch_ops[i].get_discards();
            if !discards.is_empty() {
                task.try_cancel(i, discards)?;
            }
        }

        // fire;
        self.strategy.make_step(task)?;

        self.event_emitter.flush()?;
        debug_worker!("========== finish step {} cost: {:?} ==========", self.step_count, start.elapsed(),);
        self.step_count += 1;
        Ok(())
    }

    pub fn close(&mut self) -> IOResult<()> {
        self.event_emitter.close()
    }
}
