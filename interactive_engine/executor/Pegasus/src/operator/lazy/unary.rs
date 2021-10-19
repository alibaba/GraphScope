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

pub trait LazyUnary<D: Data, A> {

    fn lazy_unary<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F:  Transform<D, D2> + 'static;

    fn lazy_unary_state<S, D2, P, B, F, N>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, S: OperatorState, B: FnOnce(&OperatorInfo) -> (F, N),
              F: STransform<D, D2, S> + Sync + 'static,
              N: Notify<S, D2> + 'static;
}

struct LazyUnaryOperator<I, O, F> {
    func: F,
    actives: HashMap<Tag, Transformer<I, O, F>>,
}

impl<I, O, F> LazyUnaryOperator<I, O, F>
    where I: Data, O: Data, F: Transform<I, O>
{
    pub fn new(func: F) -> Self {
        LazyUnaryOperator {
            func,
            actives: HashMap::new(),
        }
    }
}

impl<I, O, F> OperatorCore for LazyUnaryOperator<I, O, F>
    where I: Data, O: Data, F: Transform<I, O> + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let func = &self.func;
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<I>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let actives = &mut self.actives;
        let mut tags = Vec::new();
        input.for_each_batch(|data_set| {
            let mut session = output.session(&data_set);
            let (t, d) = data_set.take();
            debug_assert!(!actives.contains_key(&t));
            let mut transform = Transformer::new(d, func.clone());
            session.give_iterator(&mut transform)?;
            if transform.has_next() {
                actives.insert(t.clone(), transform);
                tags.push(t);
                Ok(false)
            } else {
                Ok(true)
            }
        })?;

        if tags.is_empty() {
            Ok(ScheduleState::Idle)
        } else {
            Ok(ScheduleState::Active(tags))
        }
    }

    fn on_active(&mut self, actives: &mut Vec<Tag>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);

        let mut i = actives.len();
        'OUT: while i > 0 {
            let t = actives[i - 1].clone();
            if let Some(mut trans) = self.actives.remove(&t) {
                let mut session = output.session(&t);
                'IN: while trans.has_next() {
                    let r = trans.next().expect("has next error");
                    if !session.give(r)? {
                        break 'IN;
                    }
                }
                if !trans.has_next() {
                    if i == actives.len() {
                        actives.remove(i - 1);
                    } else {
                        actives.swap_remove(i - 1);
                    }
                } else {
                    self.actives.insert(t, trans);
                    break 'OUT;
                }
            } else {
                error!("Operator inner active data don't have {:?}", t);
            }
            i -= 1;
        }

        Ok(())
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                debug_assert!(!self.actives.contains_key(&t), "get notify {:?}, but still active;", t);
                outputs[0].borrow_mut().transmit_end(t)?;
            }
        }
        Ok(())
    }
}

struct LazyUnaryStateOperator<I, O, F, N, S> {
    receive_fn : Arc<F>,
    notify_fn: N,
    actives: HashMap<Tag, StateTransformer<I, O, S>>,
    states: HashMap<Tag, S>,
}

impl<I, O, F, N, S> LazyUnaryStateOperator<I, O, F, N, S>
    where I: Data, O: Data, S: OperatorState,
          F: STransform<I, O, S> + Sync, N: Notify<S, O>
{
    pub fn new(receive: F, notify: N) -> Self {
        LazyUnaryStateOperator {
            receive_fn: Arc::new(receive),
            notify_fn: notify,
            actives: HashMap::new(),
            states: HashMap::new(),
        }
    }
}

impl<I, O, F, N, S> OperatorCore for LazyUnaryStateOperator<I, O, F, N, S>
    where I: Data, O: Data, S: OperatorState,
          F: STransform<I, O, S> + Sync + 'static, N: Notify<S, O> + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<I>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);

        let func = &self.receive_fn;
        let actives = &mut self.actives;
        let states = &mut self.states;
        let mut tags = Vec::new();
        input.for_each_batch(|data_set| {
            let (t, d) = data_set.take();
            debug_assert!(!actives.contains_key(&t), "");
            let mut session = output.session(&t);
            let s = states.remove(&t).unwrap_or(Default::default());
            let mut trans = StateTransformer::new(d, s, func.clone());
            while trans.has_next() {
                let r = trans.next().expect("LazyUnaryState#on_receive: has next error");
                if !session.give(r)? {
                    break
                }
            }

            if trans.has_next() {
                actives.insert(t.clone(), trans);
                tags.push(t);
                Ok(false)
            } else {
                let s = trans.return_state();
                states.insert(t, s);
                Ok(true)
            }
        })?;

        if tags.is_empty() {
            Ok(ScheduleState::Idle)
        } else {
            Ok(ScheduleState::Active(tags))
        }

    }

    fn on_active(&mut self, actives: &mut Vec<Tag>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let mut i = actives.len();
        'OUT: while i > 0 {
            let tag = actives[i - 1].clone();
            if let Some(mut trans) = self.actives.remove(&tag) {
                let mut session = output.session(&tag);
                'IN: while trans.has_next() {
                    let r = trans.next().expect("LazyUnaryState#on_active: has next error");
                    if !session.give(r)? {
                        break 'IN;
                    }
                }

                if !trans.has_next() {
                    // no more data in transform, not be active any more;
                    if i == actives.len() {
                        actives.remove(i);
                    } else {
                        actives.swap_remove(i);
                    }
                    // get state back;
                    let s = trans.return_state();
                    self.states.insert(tag, s);
                } else {
                    // return the unfinished transformer back;
                    self.actives.insert(tag, trans);
                    // output capacity should be exhausted, stop output;
                    break 'OUT;
                }
            }
            i -= 1;
        }
        Ok(())
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                debug_assert!(!self.actives.contains_key(&t),
                              "LazyUnaryState#on_notify: receive end notify while it is still in active");
                let mut session = output.session(&t);
                if let Some(state) = self.states.remove(&t) {
                    let result = self.notify_fn.on_notify(state);
                    for r in result {
                        session.give(r)?;
                    }
                } else {
                    warn!("LazyUnaryState#on_notify: no state found for {:?}", t);
                }
                session.transmit_end()?;
            }
        }

        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> LazyUnary<D, A> for Stream<D, A> {

    fn lazy_unary<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F:  Transform<D, D2> + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let func = constructor(&op_info);
        let op_core = LazyUnaryOperator::<D, D2, _>::new(func);
        self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }

    fn lazy_unary_state<S, D2, P, B, F, N>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, S: OperatorState, B: FnOnce(&OperatorInfo) -> (F, N),
              F: STransform<D, D2, S> + Sync + 'static, N: Notify<S, D2> + 'static
    {

        let op_info = self.allocate_operator_info(name);
        let (receive, notify) = constructor(&op_info);
        let op_core = LazyUnaryStateOperator::<D, D2,_, _,S>::new(receive, notify);
        self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }
}

