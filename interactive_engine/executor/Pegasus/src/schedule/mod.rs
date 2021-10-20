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

//! Schedulers.

use std::time::Duration;
use crate::channel::IOResult;
use crate::channel::eventio::EventsBuffer;
use crate::operator::Operator;
use crate::event::EventManager;
use crate::dataflow::Dataflow;
use crate::WorkerId;
use crate::execute::{Task, ExecError, GenericTask};
use crate::worker::Strategy;

pub struct Scheduler {
    pub worker: WorkerId,
    event_manager: EventManager,
    strategy: Box<dyn Strategy>,
    fired: Vec<Operator>,
}

fn check_task_finish(task: GenericTask) -> Option<Operator> {
    if let GenericTask::Operator(mut op) = task {
        if op.is_finish() {
            //trace!("### Worker[{}]: destroy operator '{}'; ", self.worker, op.name());
            op.close();
            None
        } else {
            Some(op.take())
        }
    } else {
        None
    }
}

impl Scheduler {
    pub fn new<S: Strategy>(event_manager: EventManager,
                            worker: WorkerId,
                            strategy: S) -> Self {
        Scheduler {
            worker,
            event_manager,
            strategy: Box::new(strategy),
            fired: Vec::new(),
        }
    }

    #[inline]
    pub fn set_strategy<S: Strategy>(&mut self, strategy: S) {
        self.strategy = Box::new(strategy)
    }

    #[inline]
    pub fn get_events_buffer(&self) -> &EventsBuffer {
        self.event_manager.get_events_buffer()
    }

    #[inline]
    pub fn init(&mut self, task: &Dataflow) {
        self.event_manager.init(task);
        self.strategy.init(task);
    }

    #[inline]
    pub fn get_strategy(&self) -> &Box<dyn Strategy> {
        &self.strategy
    }

    #[inline]
    pub fn get_total_elapsed(&self) -> f64 {
        self.strategy.get_total_elapsed() as f64 / 1000.0
    }

    #[inline]
    pub fn get_running_elapsed(&self) -> f64 {
        self.strategy.get_running_elapsed() as f64 / 1000.0
    }
}

fn update(event_manager: &mut EventManager, strategy: &Box<dyn Strategy>) -> IOResult<()> {
    let (deltas, has_updates) = event_manager.pull()?;
    if has_updates {
        for (ch, delta) in deltas.iter_mut().enumerate() {
            if *delta != 0 {
                strategy.messages(ch, *delta);
                *delta = 0;
            }
        }

        if ::log::log_enabled!(::log::Level::Debug) {
            event_manager.log();
        }
    }
    Ok(())
}

#[inline]
fn execute_local(mut task: GenericTask, fired: &mut Vec<Operator>) -> Result<(), ExecError> {
    task.execute()?;
    if let Some(task) = check_task_finish(task) {
        fired.push(task);
    }
    Ok(())
}

/// Notes: check active may produce new flow control events(High/LowWaterMark);
/// Make sure to flush these events timely;
fn check_active(task: &mut Dataflow, event_manager: &mut EventManager, strategy: &Box<dyn Strategy>) -> IOResult<bool> {
    let mut has_active = false;
    for op_opt in task.operators() {
        if let Some(ref mut op) = op_opt {
            event_manager.extract_inbound_events(op, strategy)?;
            if op.is_active() {
               has_active = true;
            }
        }
    }
    Ok(has_active)
}

impl Scheduler {

    pub fn check_active(&mut self, task: &mut Dataflow) -> IOResult<bool> {
        update(&mut self.event_manager, &self.strategy)?;
        let active = check_active(task, &mut self.event_manager, &self.strategy)?;
        self.event_manager.push()?;
        Ok(active)
    }

    pub fn step(&mut self, task: &mut Dataflow) -> Result<(), ExecError> {
        self.check_active(task)?;
        while let Some((op, _is_last)) = task.next_actives() {
            match self.strategy.get_task(&self.event_manager, op) {
                Ok(sub_task) => {
                    let sub_task = GenericTask::Operator(sub_task);
                    execute_local(sub_task, &mut self.fired)?;
                },
                Err(op) => {
                    self.fired.push(op);
                }
            }

            update(&mut self.event_manager, &self.strategy)?;
            let _has_active = check_active(task, &mut self.event_manager, &self.strategy)?;
        }

        for op in self.fired.drain(..) {
            task.return_op(op);
        }

        self.event_manager.push()?;
        if cfg!(debug_assertions) {
            ::std::thread::sleep(Duration::from_millis(16));
        }
        Ok(())
    }
}


