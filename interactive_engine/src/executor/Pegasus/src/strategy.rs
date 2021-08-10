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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::cmp::{min, max};
use std::time::Instant;
use super::*;
use crate::worker::{Strategy, WorkerRuntimeInfo};
use crate::event::{WaterMark, EventManager};
use crate::dataflow::{Dcg, Dataflow};
use crate::operator::{Operator, OperatorWrapper, OperatorMode};
use crate::common::Port;
use crate::memory::MB;

#[derive(Clone)]
pub struct MemResourceManager {
    pub total_size: usize,
    reserved: Arc<AtomicUsize>,
}

pub struct Reserved {
    size: usize,
    backend: Arc<AtomicUsize>,
}

impl Debug for Reserved {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        writeln!(f, "resources: reserve {} bytes", self.size)
    }
}

impl Reserved {
    pub fn new(size: usize, backend: &Arc<AtomicUsize>) -> Self {
        trace!("resources: reserved {} bytes in advance;", size);
        Reserved {
            size,
            backend: backend.clone(),
        }
    }
}

impl Drop for Reserved {
    fn drop(&mut self) {
        trace!("resources: released {} reserved bytes;", self.size);
        self.backend.fetch_sub(self.size, Ordering::SeqCst);
    }
}

impl MemResourceManager {
    pub fn new(total_size_in_mb: usize) -> Self {
        let init = crate::memory::used_memory_in_bytes();
        MemResourceManager {
            total_size: total_size_in_mb * MB + init,
            reserved: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    pub fn has_available(&self) -> bool {
        self.available_size() > 0
    }

    #[inline]
    pub fn available_size(&self) -> usize {
        let reserved = self.reserved.load(Ordering::SeqCst);
        let used = crate::memory::used_memory_in_bytes();
        let total = used + reserved;
        debug!("resources: usage: {} + {} = {}({})", used, reserved, total, self.total_size);
        if total > self.total_size {
            0
        } else {
            self.total_size - total
        }
    }

    pub fn reserve(&self, capacity: &mut usize) -> Option<Reserved> {
        let mut reserved = self.reserved.load(Ordering::SeqCst);
        let used = crate::memory::used_memory_in_bytes();
        let mut total = used + reserved;
        debug!("resources: usage: {} + {} = {}({})", used, reserved, total, self.total_size);
        loop {
            if total >= self.total_size {
                *capacity = 1;
                return None;
            } else {
                let available = self.total_size - total;
                let reserve = min(available >> 1, *capacity);
                if reserve == 0 {
                    *capacity = 0;
                    return  None;
                } else {
                    let new_reserved = match self.reserved.compare_exchange(reserved, reserved + reserve,Ordering::SeqCst, Ordering::SeqCst ) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    if new_reserved == reserved {
                        *capacity = reserve;
                        return Some(Reserved::new(reserve, &self.reserved));
                    } else {
                        reserved = new_reserved;
                        total = reserved + crate::memory::used_memory_in_bytes();
                    }
                }
            }
        }
    }
}

pub struct ResourceBoundStrategy {
    batch_size: usize,
    init_capacity: usize,
    output_bounds: RefCell<Vec<HashMap<Tag, usize>>>,
    data_expand: RefCell<HashMap<usize, (usize, usize)>>,
    resource: MemResourceManager,
    graph: Dcg,
    runtime: RefCell<WorkerRuntimeInfo>,
    history_flow: RefCell<Vec<usize>>,
    clock: Cell<Instant>,
}

impl ResourceBoundStrategy {
    pub fn new(batch_size: usize, init_capacity: usize, resource: &MemResourceManager) -> Self {
        let clock = Cell::new(Instant::now());
        ResourceBoundStrategy {
            batch_size,
            init_capacity,
            output_bounds: RefCell::new(Vec::new()),
            data_expand: RefCell::new(HashMap::new()),
            resource: resource.clone(),
            graph: Dcg::new(),
            runtime: RefCell::new(WorkerRuntimeInfo::new(clock.clone())),
            history_flow: RefCell::new(Vec::new()),
            clock,
        }
    }


    /// max (stack)msize(or used defined) of messages in operator's output channel
    /// vs min size of messages in operator's input channel;
    /// it estimate the message expand through operator;
    #[inline]
    fn get_data_expand(&self, op: &Operator) -> (usize, usize) {
        let mut data_expand = self.data_expand.borrow_mut();
        let (input, output) = data_expand.entry(op.info().index).or_insert_with(|| {
            self.estimate_data_expand(op)
        });
        (*input, *output)
    }

    /// get total count of messages in operator's input channel currently;
    fn get_inputs_size(&self, events: &EventManager, tag: &Tag, op: &Operator) -> usize {
        let index = op.info().index;
        let mut size = 0;
        for i in 0..op.input_len() {
            let port = Port::new(index, i);
            if let Some(ch) = self.graph.get_input_channels(&port) {
                size += events.get_outstanding_size(ch, tag) as usize;
            }
        }
        size
    }

    ///
    fn estimate_messages_expand(&self, op: &Operator) -> f64 {
        let history = self.history_flow.borrow();
        let index = op.info().index;
        let mut input_size = 0;
        for i in 0..op.input_len() {
            let p = Port::new(index, i);
            if let Some(ch) = self.graph.get_input_channels(&p) {
                if history.len() > ch.0 {
                    input_size += history[ch.0];
                }
            }
        }

        let mut output_size = 0;
        for i in 0..op.output_len() {
            let p = Port::new(index, i);
            if let Some(ch) = self.graph.get_out_channel(&p) {
                if history.len() > ch.0 {
                    output_size += history[ch.0];
                }
            }
        }

        if input_size == 0 {
            if output_size == 0 {
                return 1.0;
            } else {
                return ::std::f64::MAX;
            }
        }

        output_size as f64 / input_size as f64
    }

    fn estimate_data_expand(&self, op: &Operator) -> (usize, usize) {
        let index = op.info().index;
        let mut output_size = 0;
        let runtime = self.runtime.borrow();
        for i in 0..op.output_len() {
            let port = Port::new(index, i);
            if let Some(ch) = self.graph.get_out_channel(&port) {
                output_size = max(runtime.get_message_size_of(ch.0), output_size);
            }
        }
        let mut input_size = ::std::usize::MAX;
        for i in 0..op.input_len() {
            let port = Port::new(index, i);
            if let Some(ch) = self.graph.get_input_channels(&port) {
                input_size = min(runtime.get_message_size_of(ch.0), input_size);
            }
        }
        debug_assert!(output_size > 0);
        debug_assert!(input_size < ::std::usize::MAX);

        (input_size, output_size)
    }


    fn get_output_capacity(&self, tag: &Tag, op: &Operator, events: &EventManager,
                           permits: &mut Vec<Reserved>) -> usize {
        let available = self.resource.available_size();
        if available <= 0 {
            return 8;
        }
        let index = op.info().index;
        let (_input, output) = self.get_data_expand(&op);

        if output == 0 {
            warn!("Strategy#get_output_capacity: operator {} has no output;", op.info());
            return ::std::usize::MAX;
        }

        let mut output_bounds = self.output_bounds.borrow_mut();
        while output_bounds.len() <= index {
            output_bounds.push(HashMap::new());
        }

        if let Some(bound) = output_bounds[index].get_mut(tag) {
            let mut max_outstanding = 0;
            for i in 0..op.output_len() {
                let p = Port::new(index, i);
                if let Some(ch) = self.graph.get_out_channel(&p) {
                    max_outstanding = max(events.get_outstanding_size(ch, tag), max_outstanding);
                }
            }

            debug_assert!(max_outstanding >= 0);
            if max_outstanding == 0 {
                let expand = self.estimate_messages_expand(&op);
                if expand == ::std::f64::MAX {
                    warn!("Strategy#get_output_capacity: operator {} seems has no input;", op.info());
                    return 0;
                }

                if expand == 0.0 {
                    warn!("Strategy#get_output_capacity: operator {} has no output;", op.info());
                    return ::std::usize::MAX;
                }
                let mut inputs = self.get_inputs_size(events, tag, &op);
                if inputs == 0 {
                    inputs = self.batch_size;
                }

                trace!("expand ration of {} is {}", op.info(), expand);
                let mut capacity = (expand * inputs as f64) as usize * output;
                let permit = self.resource.reserve(&mut capacity);
                if capacity > 0 {
                    capacity = capacity / output + 1;
                    *bound = capacity;
                    permits.push(permit.unwrap());
                    return capacity;
                } else {
                    return 8;
                }
            } else if max_outstanding > 0 {
                let outstanding = max_outstanding as usize;
                if outstanding >= *bound >> 1 {
                    trace!("operator {}'s output remaining large message, skip output;", op.info());
                    return 0;
                } else {
                    let mut left = (*bound - outstanding) * output;
                    let permit = self.resource.reserve(&mut left);
                    if left > 0 {
                        left = left / output;
                        permits.push(permit.unwrap());
                    }
                    return left;
                }
            } else {
                error!("negative outstanding {}", max_outstanding);
                return 0;
            }
        } else {
            // set init bound capacity;
            let mut capacity = self.init_capacity * self.batch_size * output;
            let permit = self.resource.reserve(&mut capacity);
            if capacity > 0 {
                capacity = capacity / output;
                output_bounds[index].insert(tag.clone(), capacity);
                permits.push(permit.unwrap());
                return capacity;
            } else {
                return 8;
            }
        }
    }
}

impl Strategy for ResourceBoundStrategy {
    fn update_avg_size(&self, ch: usize, size: usize) {
        self.runtime.borrow_mut().update_size_of(ch, size);
    }

    fn new_step(&self) -> usize {
        self.clock.replace(Instant::now());
        let iter = self.runtime.borrow_mut().new_step();
        trace!("===========new step: {} ========== ", iter);
        iter
    }

    fn messages(&self, ch: usize, delta: i64) {
        if delta > 0 {
            let mut history_flow = self.history_flow.borrow_mut();
            while history_flow.len() <= ch {
                history_flow.push(0);
            }
            history_flow[ch] += delta as usize;
        }
    }

    /// total elapsed: running time + waiting time
    fn get_total_elapsed(&self) -> usize {
        self.runtime.borrow().get_total_elapsed()
    }

    /// running elapsed: only running time
    fn get_running_elapsed(&self) -> usize {
        self.runtime.borrow().running_elapsed
    }

    fn get_task(&self, events: &EventManager, mut op: Operator) -> Result<OperatorWrapper, Operator> {
        // This is a source operator if it's input size equals 0, the scheduler reject to
        // schedule this operator immediately if no resources is available;
        if op.input_len() == 0 && !self.resource.has_available() {
            return Err(op);
        }

        // This is a sink operator if it has no inner outputs, the scheduler trust it won't consume much
        // resources, and always schedule it immediately;
//        if op.output_len() == 0 || self.graph.is_last(&op) {
//
//        }

        // Get all inputs waiting to run;
        let mut inputs = op.extract_actives();
        for (t, c) in op.take_available_inputs() {
            inputs.insert(t, (c, None));
        }
        if inputs.is_empty() {
            return Ok(OperatorWrapper::on_notify(op));
        }

        if op.output_len() == 0 || self.graph.is_last(&op) {
            // fast pass, as this is a sink operator;
            return Ok(OperatorWrapper::new(op, inputs));
        }

        let mut block_count = 0;
        let mut permits = Vec::new();
        // get and set output capacity for each input on this operator;
        for (t, (_, c)) in inputs.iter_mut() {
            // if the input is blocked by some back pressure signal;
            if events.is_blocked(&op, t) {
                c.replace(0);
                block_count += 1;
                trace!("input {:?} of {} is blocked by back pressure", t, op.info());
            } else {
                match op.info().mode() {
                    OperatorMode::Clip => {
                        // keep output unbound; as it always consume more and produce less;
                        trace!("output of {} is unbound; as it does clip", op.info());
                    }
                    OperatorMode::Pass => {
                        let (input, output) = self.get_data_expand(&op);
                        let expand = output as f64 / input as f64;
                        if expand > 1.0 {
                            let input_count = self.get_inputs_size(events, t, &op);
                            let mut capacity = ((input_count * input) as f64 * expand) as usize + 1;
                            let permit = self.resource.reserve(&mut capacity);
                            if capacity > 0 {
                                let ca = capacity / output / self.batch_size;
                                trace!("output capacity of {}:{:?} is {}", op.info(), t, ca * self.batch_size);
                                c.replace(ca * self.batch_size);
                                permits.push(permit.unwrap());
                            } else {
                                c.replace(16);
                                // block_count += 1;
                                // trace!("input {:?} of {} is block by resource usage;", t, op.info());
                            }
                        } else {
                            // keep output unbound;
                            trace!("output of {} is unbound;", op.info());
                        }
                    }
                    OperatorMode::Source | OperatorMode::Expand | OperatorMode::Unknown => {
                        let mut capacity = self.get_output_capacity(t, &op, events, &mut permits);
                        if capacity == 0 {
                            capacity = 8;
                            c.replace(capacity);
                            //trace!("input {:?} of {} is blocked by resource usage; ", t, op.info());
                            //block_count += 1;
                        }
                        if capacity != ::std::usize::MAX {
                            trace!("output capacity of {}:{:?} is {}", op.info(), t, capacity);
                            c.replace(capacity);
                        } else {
                            trace!("output of {}:{:?} is unbound;", op.info(), t);
                        }
                    }
                    _ => { /* sink are handle above */ }
                }
            }
        }

        if block_count == inputs.len() {
            // all input is blocked, nothing to run;
            debug!("prevent {} to run as it outputs are blocked;", op.info());
            Err(op)
        } else {
            if permits.is_empty() {
                Ok(OperatorWrapper::new(op, inputs))
            } else {
                Ok(OperatorWrapper::with_permits(op, inputs, permits))
            }
        }
    }


    fn get_water_mark(&self, ch: usize, tag: &Tag) -> WaterMark {
        let default_bound = self.init_capacity * self.batch_size;
        if let Some(edge) = self.graph.get_edge(&ChannelId(ch)) {
            let source = edge.source;
            let output_bounds = self.output_bounds.borrow();
            if output_bounds.len() > source.index {
                if let Some(bound) = output_bounds[source.index].get(tag) {
                    WaterMark(bound + 1, 3 * bound / 4)
                } else {
                    WaterMark(default_bound + 1, 3 * default_bound / 4)
                }
            } else {
                WaterMark(default_bound + 1, 3 * default_bound / 4)
            }
        } else {
            error!("channel {} not found in dcg graph;", ch);
            WaterMark(default_bound + 1, 3 * default_bound / 4)
        }
    }

    fn end_step(&self) {
        let elapsed = self.clock.get().elapsed();
        let elapsed = elapsed.as_secs() * 1_000_000 + elapsed.subsec_micros() as u64;
        self.runtime.borrow_mut().last_step_elapsed(elapsed as usize);
        let borrow = self.runtime.borrow();
        trace!("=== step {} end, cost {}/us, total cost {}/us ===", borrow.step_count, elapsed, borrow.running_elapsed);
    }

    #[inline]
    fn init(&mut self, task: &Dataflow) {
        self.graph.init(task.edges())
    }

    #[inline]
    fn get_step(&self) -> usize {
        self.runtime.borrow().step_count
    }
}

