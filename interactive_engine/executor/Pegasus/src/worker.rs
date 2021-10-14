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

use std::time::Instant;
use std::cell::{RefCell, Cell};
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use super::*;
use crate::dataflow::{DataflowBuilderImpl, Dataflow};
use crate::channel::eventio::{EventCaster, EventsBuffer};
use crate::channel::{Push, Pull, IOResult};
use crate::allocate::{Runtime, ParallelConfig, RuntimeEnv, AllocateId, ThreadPush, ThreadPull, Thread};
use crate::event::{EventManager, WaterMark};
use crate::operator::{Operator, OperatorWrapper};
use crate::schedule::Scheduler;

pub const DEFAULT_BATCH_SIZE: usize = 1024;
/// WorkerId type.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, Abomonation, Serialize, Deserialize)]
pub struct WorkerId(pub usize, pub usize);

impl Display for WorkerId {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{}_{}", self.0, self.1)
    }
}

impl WorkerId {
    #[inline]
    pub fn task_id(&self) -> usize {
        self.0
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.1
    }
}

//#[derive(Default)]
pub struct WorkerRuntimeInfo {
    start_time: Cell<Instant>,
    pub(crate) running_elapsed: usize,
    pub(crate) step_count: usize,
    // average message size in each channel;
    msg_size: Vec<usize>,
    // may be more;
}

impl Default for WorkerRuntimeInfo {
    fn default() -> Self {
        WorkerRuntimeInfo::new(Cell::new(Instant::now()))
    }
}

impl WorkerRuntimeInfo {
    pub fn new(instant: Cell<Instant>) -> Self {
        WorkerRuntimeInfo {
            start_time: instant,
            running_elapsed: 0,
            step_count: 0,
            msg_size: Vec::new(),
        }
    }

    pub fn update_size_of(&mut self, channel: usize, size: usize) {
        while self.msg_size.len() <= channel {
            self.msg_size.push(0);
        }
        self.msg_size[channel] = size;
    }

    pub fn get_message_size_of(&self, ch: usize) -> usize {
        self.msg_size[ch]
    }

    pub fn new_step(&mut self) -> usize {
        self.step_count += 1;
        self.step_count
    }

    pub fn last_step_elapsed(&mut self, elapsed: usize) {
        self.running_elapsed += elapsed;
    }

    pub fn get_total_elapsed(&self) -> usize {
        let elapsed = self.start_time.get().elapsed();
        let elapsed = elapsed.as_secs() * 1_000_000 + elapsed.subsec_micros() as u64;
        elapsed as usize
    }
}

pub trait Strategy: Send + 'static {

    fn get_step(&self) -> usize;

    fn init(&mut self, task: &Dataflow);

    // update message average size of channel with the id;
    fn update_avg_size(&self, ch: usize, size: usize);

    /// update and get the step count;
    fn new_step(&self) -> usize;

    fn end_step(&self);

    fn messages(&self, ch: usize, delta: i64);

    fn get_total_elapsed(&self) -> usize;

    fn get_running_elapsed(&self) -> usize;

    fn get_task(&self, events: &EventManager, op: Operator) -> Result<OperatorWrapper, Operator>;

    /// 动态决策作业数据通道的警戒水位线；
    fn get_water_mark(&self, ch: usize, tag: &Tag) -> WaterMark;
}

pub struct DefaultStrategy {
    high_water: usize,
    low_water: usize,
    capacity: Option<usize>,
    runtime_info: RefCell<WorkerRuntimeInfo>,
    clock: Cell<Instant>,
}

impl Default for DefaultStrategy {
    fn default() -> Self {
        DefaultStrategy::new(256 * 1024, 256 * 1024, None)
    }
}

impl DefaultStrategy {
    pub fn new(high: usize, low: usize, capacity: Option<usize>) -> Self {
        let clock = Cell::new(Instant::now());
        DefaultStrategy {
            high_water: high,
            low_water: low,
            capacity,
            runtime_info: RefCell::new(WorkerRuntimeInfo::new(clock.clone())),
            clock
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            DefaultStrategy::default()
        } else {
            DefaultStrategy::new(2 * capacity, capacity, Some(capacity))
        }
    }
}

impl Strategy for DefaultStrategy {
    fn get_step(&self) -> usize {
        self.runtime_info.borrow().step_count
    }

    fn init(&mut self, _task: &Dataflow) {
        debug!("Default strategy inited;")
    }

    fn update_avg_size(&self, ch: usize, size: usize) {
        self.runtime_info.borrow_mut().update_size_of(ch, size);
    }

    fn new_step(&self) -> usize {
        self.clock.replace(Instant::now());
        let iter = self.runtime_info.borrow_mut().new_step();
        debug!("===========new step: {} ========== ", iter);
        iter
    }

    fn end_step(&self) {
        let elapsed = self.clock.get().elapsed();
        let elapsed = elapsed.as_secs() * 1_000_000 + elapsed.subsec_micros() as u64;
        self.runtime_info.borrow_mut().last_step_elapsed(elapsed as usize);
        let borrow = self.runtime_info.borrow();
        debug!("=== step {} end, cost {}/us, total cost {}/us ===",borrow.step_count, elapsed, borrow.running_elapsed);
    }

    fn messages(&self, ch: usize, delta: i64) {
        if delta > 0 {
            debug!("channel {} +{}", ch, delta);
        } else {
            debug!("channel {} {}", ch, delta);
        }
    }

    /// total elapsed: running time + waiting time
    fn get_total_elapsed(&self) -> usize {
        let elapsed = self.runtime_info.borrow().start_time.get().elapsed();
        let elapsed = elapsed.as_secs() * 1_000_000 + elapsed.subsec_micros() as u64;
        elapsed as usize
    }

    /// running elapsed: only running time
    fn get_running_elapsed(&self) -> usize {
        self.runtime_info.borrow().running_elapsed
    }

    fn get_task(&self, events: &EventManager, mut op: Operator) -> Result<OperatorWrapper, Operator> {
        let mut inputs = op.extract_actives();
        for (t, r) in op.take_available_inputs() {
            inputs.insert(t, (r, None));
        }

        for (t, (_, c))  in inputs.iter_mut() {
            if events.is_blocked(&op, t) {
                c.replace(0);
            } else if let Some(size) = self.capacity {
                c.replace(size);
            }
        }

        Ok(OperatorWrapper::new(op, inputs))
    }

    fn get_water_mark(&self, _ch: usize, _t: &Tag) -> WaterMark {
        WaterMark(self.low_water, self.high_water)
    }
}

/// A worker represents a data-parallel computation unit of a job;
/// But it doesn't bind any physical resources, e.g. memory, cpu...;
pub struct Worker  {
    /// parallel degree configuration of this job;
    pub parallel: ParallelConfig,
    /// the id of a worker in the job ;
    pub id: WorkerId,
    /// the user defined job represented as a directed cycle graph;
    task: Option<Dataflow>,
    /// System resources manage;
    runtime: Arc<Runtime>,
    /// drive task to run;
    scheduler: Scheduler,
    /// configure how many mill seconds are allowed for this worker to run at most;
    timeout: Arc<AtomicUsize>,
}

impl Debug for Worker {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Worker[{}]", self.id)
    }
}

impl Worker {
    pub(crate) fn new(a: &Arc<Runtime>, task_id: usize, parallel: ParallelConfig, index: usize) -> Self {
        Worker::with_strategy(a, task_id, parallel, index, DefaultStrategy::default())
    }

    pub fn with_strategy<S: Strategy>(a: &Arc<Runtime>, task_id: usize, parallel: ParallelConfig,
                                      index: usize, strategy: S) -> Self {
        let id = WorkerId(task_id, index);
        let (sx, rx) = crossbeam_channel::unbounded();
        let events_buffer = EventsBuffer::new(parallel.total_peers(), id, sx);
        let (pushes, pull) = a.get_env().allocate(AllocateId(task_id, index, 0), parallel).unwrap();
        let event_caster = EventCaster::new(id, ChannelId(0), rx, parallel, pushes, pull);
        let event_manager = EventManager::new(id, event_caster, events_buffer);

        Worker {
            parallel,
            id: WorkerId(task_id, index),
            task: None,
            runtime: a.clone(),
            scheduler: Scheduler::new(event_manager, id, strategy),
            timeout: Arc::new(AtomicUsize::new(usize::max_value())),
        }
    }

    #[inline]
    pub fn set_schedule_strategy<S: Strategy>(&mut self, strategy: S) {
        self.scheduler.set_strategy(strategy);
    }

    pub fn set_timeout(&mut self, timeout_ms: Arc<AtomicUsize>) {
        self.timeout = timeout_ms;
    }

    pub fn dataflow<F>(&mut self, name: &str, func: F) -> Result<(), ExecError>
        where F: for<'a> FnOnce(&DataflowBuilderImpl<'a>) -> Result<(), String> + 'static
    {
        self.dataflow_opt(name, DEFAULT_BATCH_SIZE, true, func)
    }

    pub fn dataflow_opt<F>(&mut self, name: &str, batch_size: usize, print: bool, func: F) -> Result<(), ExecError>
        where F: for<'a> FnOnce(&DataflowBuilderImpl<'a>) -> Result<(), String> + 'static {
        let plan = {
            let event_buffer = self.scheduler.get_events_buffer();
            let builder = DataflowBuilderImpl::new(name, batch_size,
                                                   self, event_buffer);
            func(&builder)?;
            builder.build(print)
        }?;

        self.scheduler.init(&plan);
        self.task.replace(plan);
        Ok(())
    }

//    pub fn dataflow_with_client<T: Data, F>(&mut self, name: &str, batch_size: usize, func: F) -> Result<Client<T>, ExecError>
//        where F: for<'a> FnOnce(Stream<T, DataflowBuilderImpl<'a>>) + 'static
//    {
//
//        let (sx, rx) = crossbeam_channel::unbounded();
//        let client = Client::new(sx);
//        let source = StreamingSource::new(batch_size, rx);
//
//        let (sx, rx) = crossbeam_channel::unbounded();
//        let event_buffer = EventsBuffer::new(self.parallel.total_peers(),
//                                             self.id, sx);
//        let builder = DataflowBuilderImpl::new(name, batch_size,
//                                               self, &event_buffer);
//        let stream = source.into_stream_more(batch_size, &builder);
//        func(stream);
//        let plan = builder.build()?;
//
//        let event_manage = EventManager::from(&plan);
//        let (pushes, pull) = self.allocate(0);
//        let event_caster = EventCaster::new(self.id, batch_size, ChannelId(0), rx, pushes, pull);
//        let s = Scheduler::new(event_buffer, event_caster, event_manage,
//                               self.runtime.clone(), plan, self.id);
//        self.task.replace(s);
//
//        Ok(client)
//    }

    #[inline]
    pub fn allocate<T: Data>(&self, id: usize, size: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>) {
        trace!("estimate size of message in channel {} is {}", id, size);
        self.scheduler.get_strategy().update_avg_size(id, size);
        let id = AllocateId(self.id.0, self.id.1, id);
        // TODO: safely unwrap
        self.runtime.get_env().allocate(id, self.parallel).unwrap()
    }

    #[inline]
    pub fn pipeline<T: Data>(&self, id: usize, size: usize) -> (ThreadPush<T>, ThreadPull<T>) {
        trace!("estimate size of message in channel {} is {}", id, size);
        self.scheduler.get_strategy().update_avg_size(id, size);
        Thread::pipeline::<T>()
    }

    pub(crate) fn run(&mut self) -> Result<bool, ExecError> {
        self.scheduler.get_strategy().new_step();
        let scheduler = &mut self.scheduler;
        if let Some(ref mut task) = self.task {
            scheduler.step(task)?;
        } else {
            error!("Error#Worker[{}]: job not found, check if worker is initialized;", self.id);
        }
        self.scheduler.get_strategy().end_step();
        Ok(self.is_finish())
    }

    #[inline]
    pub fn has_inner_active(&self) -> bool {
        self.task.as_ref().map(|plan| {
            plan.has_active()
        }).unwrap_or(false)
    }

    pub fn check_active(&mut self) -> IOResult<bool> {
        if !self.has_inner_active() {
            let scheduler = &mut self.scheduler;
            if let Some(ref mut task) = self.task {
                scheduler.check_active(task)
            } else {
                error!("Error#Worker[{}]: job not found, check if worker is initialized;", self.id);
                Ok(false)
            }
        } else {
            Ok(true)
        }
    }

    #[inline]
    pub fn checkout_timeout(&self) -> bool {
        let total_elapsed = self.scheduler.get_total_elapsed();
        if total_elapsed >= self.timeout.load(Ordering::Relaxed) as f64 {
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_finish(&self) -> bool {
        if let Some(ref task) = self.task {
            let elapsed = self.scheduler.get_total_elapsed();
            let flag = task.check_finished();
            if flag {
                info!("Worker[{:?}] task {} finish, running time {} ms, total cost {} ms.", self.id, task.name, self.scheduler.get_running_elapsed(), elapsed);
                true
            } else if self.timeout.load(Ordering::Relaxed) as f64 <= elapsed {
                error!("Worker[{:?}] task {} timeout, running time {} ms, total cost {} ms", self.id, task.name, self.scheduler.get_running_elapsed(), elapsed);
                true
            } else {
                false
            }
        } else {
            true
        }
    }
}


