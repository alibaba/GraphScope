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

use crate::dataflow::{Dataflow, DataflowBuilder};
use crate::errors::{BuildJobError, JobExecError};
use crate::event::{EventBus, EventEntrepot, EventManager};
use crate::schedule::Schedule;
use crate::{JobConf, WorkerId};
use pegasus_executor::{Task, TaskExecError, TaskState};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub struct Worker {
    pub conf: Arc<JobConf>,
    pub id: WorkerId,
    task: Option<(Dataflow, Schedule)>,
    peer_guard: Arc<AtomicUsize>,
    start: Instant,
    cancel_hook: Arc<AtomicBool>,
}

impl Worker {
    pub(crate) fn new(
        conf: &Arc<JobConf>, id: WorkerId, peer_guard: &Arc<AtomicUsize>,
        cancel_hook: &Arc<AtomicBool>,
    ) -> Self {
        if peer_guard.fetch_add(1, Ordering::SeqCst) == 0 {
            pegasus_memory::alloc::new_task(conf.job_id as usize);
        }
        Worker {
            conf: conf.clone(),
            id,
            task: None,
            peer_guard: peer_guard.clone(),
            start: Instant::now(),
            cancel_hook: cancel_hook.clone(),
        }
    }

    pub fn dataflow<F>(&mut self, func: F) -> Result<(), BuildJobError>
    where
        F: FnOnce(&DataflowBuilder) -> Result<(), BuildJobError> + 'static,
    {
        // set current worker's id into tls variable to make it accessible at anywhere;
        let _g = crate::worker_id::guard(self.id);
        let (tx, rx) = crossbeam_channel::unbounded();
        let event_bus = EventBus::new(self.id, tx);
        let dfb = DataflowBuilder::new(self.id, &self.conf, &event_bus);
        func(&dfb)?;
        let df = dfb.build()?;
        let entrepot = EventEntrepot::new(event_bus, rx, &self.conf)?;
        let event_manager = EventManager::new(entrepot, &df)?;
        let schedule = Schedule::new(self.conf.memory_limit, event_manager);
        self.task = Some((df, schedule));
        Ok(())
    }

    pub fn run(&mut self) -> Result<TaskState, JobExecError> {
        if let Some((mut task, mut schedule)) = self.task.take() {
            let is_active = schedule.step(&mut task)?;
            if is_active {
                self.task = Some((task, schedule));
                Ok(TaskState::Ready)
            } else {
                if task.check_finish() {
                    if let Err(e) = schedule.close() {
                        warn_worker!(
                            "error occurred when close schedule after task finished: {}",
                            e
                        );
                    }
                    debug_worker!("finished;");
                    Ok(TaskState::Finished)
                } else if self.check_cancel() {
                    schedule.close().ok();
                    for op in task.operators.iter_mut() {
                        if let Some(op) = op {
                            op.close();
                        }
                    }
                    debug_worker!("be canceled;");
                    Ok(TaskState::Finished)
                } else {
                    self.task = Some((task, schedule));
                    Ok(TaskState::NotReady)
                }
            }
        } else {
            Ok(TaskState::Finished)
        }
    }

    fn check_cancel(&self) -> bool {
        if self.cancel_hook.load(Ordering::Relaxed) {
            error_worker!("has been canceled.");
            return true;
        }
        let elapsed = self.start.elapsed().as_millis();
        let is_timeout = (self.conf.time_limit as u128) < elapsed;
        if is_timeout {
            error_worker!("execute timeout, take {} millis", elapsed);
        }
        is_timeout
    }

    pub fn check_ready(&mut self) -> Result<TaskState, JobExecError> {
        if let Some((mut task, mut schedule)) = self.task.take() {
            if self.check_cancel() {
                schedule.close().ok();
                for op in task.operators.iter_mut() {
                    if let Some(op) = op {
                        op.close();
                    }
                }
                debug_worker!("be canceled;");
                Ok(TaskState::Finished)
            } else {
                if schedule.check_ready()? {
                    self.task = Some((task, schedule));
                    Ok(TaskState::Ready)
                } else if task.check_finish() {
                    debug_worker!("finished;");
                    Ok(TaskState::Finished)
                } else {
                    self.task = Some((task, schedule));
                    Ok(TaskState::NotReady)
                }
            }
        } else {
            debug_worker!("finished;");
            Ok(TaskState::Finished)
        }
    }
}

#[allow(dead_code)]
struct WorkerContext {
    resource: Box<dyn Any>,
}

impl WorkerContext {
    fn new(id: WorkerId) -> Self {
        pegasus_memory::alloc::reset_current_task(Some(id.job_id as usize));
        let resource = Box::new(pegasus_memory::alloc::trace_memory_alloc());
        WorkerContext { resource }
    }
}

impl Drop for WorkerContext {
    fn drop(&mut self) {
        pegasus_memory::alloc::reset_current_task(None);
    }
}

impl Task for Worker {
    fn execute(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
        let _c = WorkerContext::new(self.id);
        let _g = crate::worker_id::guard(self.id);
        Ok(self.run()?)
    }

    fn check_ready(&mut self) -> Result<TaskState, Box<dyn TaskExecError>> {
        let _c = WorkerContext::new(self.id);
        let _g = crate::worker_id::guard(self.id);
        Ok(Worker::check_ready(self)?)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.peer_guard.fetch_sub(1, Ordering::SeqCst) == 1 {
            pegasus_memory::alloc::remove_task(self.id.job_id as usize);
        }
    }
}
