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

use std::collections::HashMap;
use super::*;
use crate::{WorkerId, ChannelId};
use crate::operator::branch::Condition;
use crate::event::Event;
use crate::channel::eventio::EventsBuffer;
use crate::communication::{Pipeline, Exchange};
use crate::operator::advanced::scope::Scope;
use crate::operator::binary::Binary;

pub trait Iteration<D: Data, A> {

    fn iterate<F>(&self, max_times: u32, func: F) -> Stream<D, A>
        where F: Fn(&Stream<D, A>) -> Stream<D, A>;

    fn iterate_until<P, F>(&self, max_times: u32, until: P, func: F) -> Stream<D, A>
        where P: Condition<D> + 'static, F: FnOnce(&Stream<D, A>) -> Stream<D, A>;

    fn iterate_more<F>(&self, max_times: u32, func: F) -> Stream<D, A>
        where F: FnOnce(&Stream<D, A>) -> (Option<Stream<D, A>>, Stream<D, A>);
}

#[derive(Debug, Eq, PartialEq, Hash)]
struct LoopState {
    pub(crate) worker: WorkerId,
    pub(crate) tag: Tag,
    pub(crate) current: u32,
}

#[allow(dead_code)]
impl LoopState {
    pub fn new(worker: WorkerId, tag: Tag, current: u32) -> Self {
        LoopState {
            worker,
            tag,
            current
        }
    }
}

struct LoopControlOperator<D: Data> {
    worker: WorkerId,
    scopes: usize,
    ch_ids: (ChannelId, ChannelId),
    max_times: u32,
    peers: usize,
    condition: Option<Box<dyn Condition<D>>>,
    event_buf: EventsBuffer,
    /// Record iteration information; The key is the iteration tag, the value is an array of bool,
    /// the index `n` of the array indicate the `(n + 1)` th iteration, and a `true` value indicate there were
    /// messages in the nth iteration on this worker.
    /// TODO: compress Vec<bool>
    history: HashMap<Tag, Vec<bool>>,
    /// Record all iterations started on this worker, each iteration has a unique tag description;
    iteration_start: HashSet<Tag>,
    /// Record all iterations' finish information on different workers;
    /// The key is the iteration tag, and the value is a id set of worker on which this iteration is finished;
    iteration_end: HashMap<Tag, HashSet<WorkerId>>,
    /// The flag indicate that whether the main input of the loop body is exhausted,
    /// if `true`, means no more data would enter the loop, no new iteration would be created;
    outer_input_exhaust: bool,
}

impl<D: Data> LoopControlOperator<D> {
    pub fn new(info: &OperatorInfo, max_times: u32, main_ch: ChannelId,
               back_ch: ChannelId, event_buf: &EventsBuffer) -> Self {
        LoopControlOperator {
            worker: info.worker,
            scopes: info.scopes,
            max_times,
            peers: info.peers,
            condition: None,
            ch_ids: (main_ch, back_ch),
            event_buf: event_buf.clone(),
            history: HashMap::new(),
            iteration_start: HashSet::new(),
            iteration_end: HashMap::new(),
            outer_input_exhaust: false,
        }
    }

    pub fn loop_until<C: Condition<D> + 'static>(&mut self, condition: C) {
        self.condition.replace(Box::new(condition));
    }

    #[inline]
    fn sync_loop_state(&mut self, worker: WorkerId, tag: Tag, is_receive: bool) -> IOResult<()> {
        if !is_receive {
            let event = Event::Iterations(tag.clone(), worker, self.ch_ids.1);
            trace!("### Worker[{}]: broadcast loop state: {:?}", worker, event);
            self.event_buf.broadcast_exclude(worker, event)?;
        }
        let set = self.iteration_end.entry(tag)
            .or_insert(HashSet::new());
        set.insert(worker);
        Ok(())
    }
}

impl<D: Data> OperatorCore for LoopControlOperator<D> {
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut output1_borrow = outputs[0].borrow_mut();
        let out_loop = OutputHandle::<D>::downcast(&mut output1_borrow);

        let mut output2_borrow = outputs[1].borrow_mut();
        let return_loop = OutputHandle::<D>::downcast(&mut output2_borrow);


        let condition = &self.condition;
        let history = &mut self.history;
        {
            // The main data stream into the loop, this will start the first iteration;
            let mut input1_borrow = inputs[0].borrow_mut();
            let input_main = InputHandle::<D>::downcast(&mut input1_borrow);
            input_main.for_each_batch(|dataset| {
                let (t, data) = dataset.take();
                debug_assert_eq!(0, t.current());
                let mut out_session = out_loop.session(&t);
                let mut loop_session = return_loop.session(&t);
                // start a data stream branch, check if into loop;
                // indicate if the first iteration has data on this worker;
                let has_into_loop = history.entry(t.to_parent())
                    .or_insert(vec![false]);
                if let Some(pre) = condition {
                    for datum in data {
                        if pre.predicate(&datum) {
                            out_session.give(datum)?;
                        } else {
                            has_into_loop[0] = true;
                            loop_session.give(datum)?;
                        }
                    }
                } else {
                    has_into_loop[0] = true;
                    loop_session.give_batch(data)?;
                }
                Ok(true)
            })?;
        }

        {
            // The feedback data stream, check if converge or continue iteration;
            let mut input2_borrow = inputs[1].borrow_mut();
            let feedback = InputHandle::<D>::downcast(&mut input2_borrow);
            let max_it = self.max_times as usize;
            feedback.for_each_batch(|dataset| {
                let (t, data) = dataset.take();
                let p = t.to_parent();
                let mut out_session = out_loop.session(&t);
                // this data has complete `loop_times` iterations, and will start the next iteration soon;
                let loop_times = t.current() as usize;
                let next_loop_times = loop_times + 1;
                let his = history.entry(p).or_insert(vec![false;next_loop_times]);
                while his.len() < next_loop_times {
                    his.push(false);
                }

                if loop_times >=  max_it {
                    out_session.give_batch(data)?;
                } else {
                    let mut loop_session = return_loop.session(&t);
                    if let Some(pre) = condition {
                        for datum in data {
                            if pre.predicate(&datum) {
                                out_session.give(datum)?;
                            } else {
                                his[next_loop_times - 1] = true;
                                loop_session.give(datum)?;
                            }
                        }
                    } else {
                        // if not converge condition is specific, only iterate fixed times;
                        his[next_loop_times - 1] = true;
                        loop_session.give_batch(data)?;
                    }
                }
                Ok(true)
            })?;
        }

        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {

        let worker = self.worker;
        for n in notifies.drain(..) {
            match n {
                Notification::End(ch, end) => {
                    if ch == self.ch_ids.0 {
                        // Loop is always after a scope enter; The end would eight be a parent end or current scope end
                        // do when parent scope end:
                        if end.len() == self.scopes - 1 {
                            let first = Tag::from(&end, 1);
                            if let Some(his) = self.history.get(&end) {
                                debug_assert!(his.len() >= 1);
                                if his.len() == 1 && !his[0] {
                                    // indicate no data entered the first iteration;
                                    self.sync_loop_state(worker, first.clone(), false)?;
                                }
                            } else {
                                // no history indicates that no data has arrived this operator,
                                // this also means that no data entered the first iteration;
                                self.sync_loop_state(worker, first.clone(), false)?;
                            }
                            // end tag is always exist on each worker, no matter whether data exist;
                            self.iteration_start.insert(end);
                            outputs[1].borrow_mut().transmit_end(first)?;
                        }
                    } else {
                        // end signal of the nth iteration;
                        debug_assert_eq!(ch, self.ch_ids.1);
                        debug_assert_eq!(end.len(), self.scopes);
                        let p = end.to_parent();
                        // get the iteration history:
                        // 1. there is no record of the nth iteration: sync loop state;
                        // 2. there is record of the nth iteration, but the record is false and:
                        //      a. no more higher iterations history: sync loop state;
                        //      b. there are higher iterations history: not sync;
                        // 3. there is record of the nth iteration, and record is true: not sync;
                        // 4. there is record of the nth iteration: not sync;
                        // The cur'th iteration is finished;
                        let cur = end.current() as usize;
                        if let Some(his) = self.history.get(&p) {
                            if his.len() < cur || (his.len() == cur && !his[cur - 1]) {
                                self.sync_loop_state(worker, end.clone(), false)?;
                            }
                        } else {
                            self.sync_loop_state(worker, end.clone(), false)?;
                        }
                        if !outputs[1].borrow().is_closed() {
                            outputs[1].borrow_mut().transmit_end(end.advance())?;
                        }
                    }
                },
                Notification::Iteration(tag, worker, ch) => {
                    debug_assert_eq!(ch, self.ch_ids.1);
                    self.sync_loop_state(worker, tag, true)?;
                },
                Notification::EOS(ch) => {
                    if ch == self.ch_ids.0 {
                        trace!("### Worker[{}]: enter loop channel: {} exhausted;", self.worker, ch);
                        self.outer_input_exhaust = true;
                    } else {
                        debug_assert_eq!(ch, self.ch_ids.1);
                        debug_assert!(self.outer_input_exhaust);
                    }
                }
            }
        };
        // recompute states after notification, to figure out if any iteration has finished;
        let peers = self.peers;
        let mut tmp = Vec::new();
        self.iteration_end.retain(|t, w| {
            if w.len() == peers {
                tmp.push(t.clone());
                false
            } else { true }
        });

        for end in tmp {
            let p = end.to_parent();
            self.history.remove(&p);
            if self.iteration_start.remove(&p) {
                outputs[0].borrow_mut().transmit_end(p)?;
            }
        }

        if log_enabled!(log::Level::Trace) {
            trace!("### Worker[{}] check loop inner state : \n\thistory: {:?}, \n\tin iterations: {:?} \n\titeration ends {:?}",
                   self.worker, self.history, self.iteration_start, self.iteration_end);
        }

        if self.outer_input_exhaust && self.iteration_start.is_empty() {
            trace!("### Worker[{}]: loop_ctrl is closing loop body...;", self.worker);
            outputs[1].borrow_mut().close()?;
        }

        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> Stream<D, A> {
    pub fn add_loop_control<F, B, O>(&self, body: F, builder: B) -> Stream<D, A>
        where O: OperatorCore + 'static,
              B: FnOnce(&OperatorInfo, ChannelId, ChannelId, &EventsBuffer) -> O,
              F: FnOnce(&Stream<D, A>) -> Stream<D, A>
    {
        let info = self.allocate_operator_info("loop_ctrl");
        info.set_pass();
        let port = Port::first(info.index);
        let leave_loop = Stream::from(port, self);
        let main_input = self.connect(port, Pipeline);
        let leave_output = leave_loop.get_output().clone();

        let port = Port::second(info.index);
        let into_loop = Stream::from(port, self);
        let enter_output = into_loop.get_output().clone();
        let after_loop = body(&into_loop);
        let feedback_input = after_loop.connect(port, Pipeline);

        let event_buffer = self.get_event_buffer();
        let core = builder(&info, main_input.ch_id, feedback_input.ch_id, event_buffer);

        let op = OperatorBuilder::new(info)
            .core(core)
            .add_output(leave_output, OutputDelta::None)
            .add_output(enter_output, OutputDelta::Advance)
            .add_input(main_input)
            .add_input(feedback_input);

        self.add_operator(op);
        leave_loop
    }

    pub fn add_loop_control_more<F, B, O>(&self, body: F, builder: B) -> Stream<D, A>
        where O: OperatorCore + 'static,
              B: FnOnce(&OperatorInfo, ChannelId, ChannelId, &EventsBuffer) -> O,
              F: FnOnce(&Stream<D, A>) -> (Option<Stream<D, A>>, Stream<D, A>)
    {
        let info = self.allocate_operator_info("loop_ctrl_more");
        info.set_pass();
        // fist input port;
        let port = Port::first(info.index);
        let main_input = self.connect(port, Pipeline);
        // first output port;
        let leave_inner = Stream::<D, A>::from(port, self);
        let main_output = leave_inner.get_output().clone();
        // second output port;
        let port = Port::second(info.index);
        let into_loop = Stream::from(port, self);
        let loop_output = into_loop.get_output().clone();
        let (leave_user, feedback) = body(&into_loop);
        let feedback_input = feedback.connect(Port::second(info.index), Pipeline);
        let stream = if let Some(leave) = leave_user {
            leave_inner.binary("leave_merge", &leave, Pipeline, Pipeline, |_info| {
                |mut input, output| {
                    input.first_for_each(|dataset| {
                        let mut session = output.session(&dataset);
                        session.give_batch(dataset.data())
                    })?;
                    input.second_for_each(|dataset| {
                        let mut session = output.session(&dataset);
                        session.give_batch(dataset.data())
                    })?;
                    Ok(())
                }
            })
        } else { leave_inner };

        let event_buffer = self.get_event_buffer();
        let core = builder(&info, main_input.ch_id, feedback_input.ch_id, event_buffer);
        let op = OperatorBuilder::new(info)
            .core(core)
            .add_output(main_output, OutputDelta::None)
            .add_output(loop_output, OutputDelta::Advance)
            .add_input(main_input)
            .add_input(feedback_input);
        self.add_operator(op);
        stream
    }
}

#[inline]
fn enter_loop<D: Data, A: DataflowBuilder>(input: &Stream<D, A>) -> Stream<D, A> {
    if input.is_local() {
        let target = input.worker_id().1 as u64;
        input.enter(Exchange::new(move |_| target))
    } else {
        input.enter(Pipeline)
    }
}

impl<D: Data, A: DataflowBuilder> Iteration<D, A> for Stream<D, A> {
    fn iterate<F>(&self, max_times: u32, func: F) -> Stream<D, A> where F: Fn(&Stream<D, A>) -> Stream<D, A> {
        if max_times <= 8 {
            let mut stream = func(self);
            for _i in 1..max_times {
                stream = func(&stream);
            }
            stream
        } else {
            let enter = enter_loop(self);
            let iterate = enter.add_loop_control(func, |info, ch1, ch2, events| {
                LoopControlOperator::<D>::new(info, max_times, ch1, ch2, events)
            });
            iterate.leave()
        }
    }

    fn iterate_until<P, F>(&self, max_times: u32, until: P, func: F) -> Stream<D, A> where P: Condition<D> + 'static, F: FnOnce(&Stream<D, A>) -> Stream<D, A> {
        let enter = enter_loop(self);
        let iterate = enter.add_loop_control( func, |info, ch1, ch2, events| {
            let mut op = LoopControlOperator::<D>::new(info, max_times, ch1, ch2, events);
            op.loop_until(until);
            op
        });
        iterate.leave()
    }

    fn iterate_more<F>(&self, max_times: u32, func: F) -> Stream<D, A>
        where F: FnOnce(&Stream<D, A>) -> (Option<Stream<D, A>>, Stream<D, A>) {
        let enter = enter_loop(self);
        let iterate = enter.add_loop_control_more(func, |info, ch1, ch2, events| {
            LoopControlOperator::<D>::new(info, max_times, ch1, ch2, events)
        });
        iterate.leave()
    }
}
