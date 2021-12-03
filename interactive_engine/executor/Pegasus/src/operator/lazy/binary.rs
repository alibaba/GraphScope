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

use super::*;
use std::collections::HashMap;

pub trait BinaryFunc<L, R, O> {

    fn left(&self, data: L) -> Option<Box<dyn Iterator<Item = O> + Send>>;

    fn right(&self, data: R) -> Option<Box<dyn Iterator<Item = O> + Send>>;
}

impl<L, R, D1, D2, O> BinaryFunc<D1, D2, O> for (L, R)
    where D1: Data, D2: Data, O: Data,
          L: Transform<D1, O>, R: Transform<D2, O>
{
    #[inline(always)]
    fn left(&self, data: D1) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (self.0).exec(data)
    }

    #[inline(always)]
    fn right(&self, data: D2) -> Option<Box<dyn Iterator<Item = O> + Send>> {
        (self.1).exec(data)
    }
}

pub trait BinaryStateFunc<L, R, S, O> {
    fn left(&self, data: L, state: &mut S) -> Option<Box<dyn Iterator<Item = O> + Send>>;

    fn right(&self, data: R, state: &mut S) -> Option<Box<dyn Iterator<Item = O> + Send>>;

    fn notify(&self, state: S) -> Box<dyn Iterator<Item = O> + Send>;
}

impl<L, R, N, D1, D2, S, D3>  BinaryStateFunc<D1, D2, S, D3> for (L, R, N)
    where D1: Data, D2: Data, D3: Data, S: OperatorState,
          L: STransform<D1, D3, S>,
          R: STransform<D2, D3, S>,
          N: Notify<S, D3>
{
    #[inline(always)]
    fn left(&self, data: D1, state: &mut S) -> Option<Box<dyn Iterator<Item = D3> + Send>> {
        (self.0).exec(data, state)
    }

    #[inline(always)]
    fn right(&self, data: D2, state: &mut S) -> Option<Box<dyn Iterator<Item = D3> + Send>> {
        (self.1).exec(data, state)
    }

    #[inline(always)]
    fn notify(&self, state: S) -> Box<dyn Iterator<Item = D3> + Send> {
        (self.2).on_notify(state)
    }
}


pub trait LazyBinary<D1: Data, A> {
    fn lazy_binary<D2, D3, C1, C2, B, L, R>(&self, name: &str, other: &Stream<D2, A>,
                                         comm_1: C1, comm_2: C2, constructor: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> (L, R),
              L: Transform<D1, D3> + 'static, R: Transform<D2, D3> + 'static;


    fn lazy_binary_state<D2, D3, S, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>,
                                                  comm_1: C1, comm_2: C2, constructor: B) -> Stream<D3, A>
        where D2: Data, D3: Data, S: OperatorState, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: BinaryStateFunc<D1, D2, S, D3> + Send + Sync + 'static;
}

struct LazyBinaryOperator<I1, I2, O, L, R> {
    left_func: L,
    right_func: R,
    actives_left: HashMap<Tag, Transformer<I1, O, L>>,
    actives_right: HashMap<Tag, Transformer<I2, O, R>>,
    end_notify: HashMap<Tag, HashSet<ChannelId>>,
    ready: Vec<Tag>,
}

impl<I1, I2, O, L, R> LazyBinaryOperator<I1, I2, O, L, R>
    where I1: Data, I2: Data, O: Data ,
          L: Transform<I1, O>, R: Transform<I2, O>
{
    pub fn new(left: L, right: R) -> Self {
        LazyBinaryOperator {
            left_func: left,
            right_func: right,
            actives_left: HashMap::new(),
            actives_right: HashMap::new(),
            end_notify: HashMap::new(),
            ready: Vec::new(),
        }
    }
}

impl<I1, I2, O, L, R> OperatorCore for LazyBinaryOperator<I1, I2, O, L, R>
    where I1: Data, I2: Data, O: Data,
          L: Transform<I1, O> + 'static,
          R: Transform<I2, O> + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input1_borrow = inputs[0].borrow_mut();
        let input1 = InputHandle::<I1>::downcast(&mut input1_borrow);

        let mut input2_borrow = inputs[1].borrow_mut();
        let input2 = InputHandle::<I2>::downcast(&mut input2_borrow);

        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let mut tags = Vec::new();
        let actives_1 = &mut self.actives_left;
        let actives_2 = &mut self.actives_right;
        {
            let left_func = &self.left_func;
            input1.for_each_batch(|dataset| {
                let (t, d) = dataset.take();
                debug_assert!(!actives_1.contains_key(&t), "LazyBinaryOperator: unexpected tag in actives left;");
                debug_assert!(!actives_2.contains_key(&t), "LazyBinary: unexpected tag in actives left;");
                let mut session = output.session(&t);
                let mut trans = Transformer::new(d, left_func.clone());
                while trans.has_next() {
                    let r = trans.next().expect("LazyBinaryOperator: has next error");
                    if !session.give(r)? {
                        break
                    }
                }

                if trans.has_next() {
                    actives_1.insert(t.clone(), trans);
                    tags.push(t);
                    Ok(false)
                } else {
                    Ok(true)
                }
            })?;
        }

        if output.has_capacity() {
            let right_func = &self.right_func;
            input2.for_each_batch(|dataset| {
                let (t, d) = dataset.take();
                debug_assert!(!actives_1.contains_key(&t), "LazyBinary: unexpected tag in actives left;");
                debug_assert!(!actives_2.contains_key(&t), "LazyBinary: unexpected tag in actives left;");
                let mut session = output.session(&t);
                let mut trans = Transformer::new(d, right_func.clone());
                while trans.has_next() {
                    let r = trans.next().expect("LazyBinary: has next error");
                    if !session.give(r)? {
                        break
                    }
                }

                if trans.has_next() {
                    debug_assert!(!actives_1.contains_key(&t), "LazyBinary: same active tag both in left and right");
                    actives_2.insert(t.clone(), trans);
                    tags.push(t);
                    Ok(false)
                } else {
                    Ok(true)
                }
            })?;
        }


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
            let mut session = output.session(&t);
            if let Some(mut trans) = self.actives_left.remove(&t) {
                'IN1: while trans.has_next() {
                    let r = trans.next().expect("LazyBinary: has next error");
                    if !session.give(r)? {
                        break 'IN1;
                    }
                }
                if !trans.has_next() {
                    if i == actives.len() {
                        actives.remove(i);
                    } else {
                        actives.swap_remove(i);
                    }
                } else {
                    self.actives_left.insert(t, trans);
                    break 'OUT;
                }
            } else {
                if let Some(mut trans) = self.actives_right.remove(&t) {
                    'IN2: while trans.has_next() {
                        let r = trans.next().expect("LazyBinary: has next error");
                        if !session.give(r)? {
                            break 'IN2;
                        }
                    }

                    if !trans.has_next() {
                        if i == actives.len() {
                            actives.remove(i);
                        } else {
                            actives.swap_remove(i);
                        }
                    } else {
                        self.actives_right.insert(t, trans);
                        break 'OUT;
                    }
                } else {
                    warn!("Operator inner active data don't have {:?}", t);
                }
            }
            i -= 1;
        }
        Ok(())
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(ch, t) = n {
                let count = {
                    let set = self.end_notify.entry(t.clone())
                        .or_insert(HashSet::new());
                    set.insert(ch);
                    set.len()
                };
                if count == 2 {
                    self.end_notify.remove(&t);
                    self.ready.push(t);
                }
            }
        }

        for e in self.ready.drain(..) {
            debug_assert!(!self.actives_left.contains_key(&e), "Receive end notify while data is still in active");
            debug_assert!(!self.actives_right.contains_key(&e), "Receive end notify while data is still in acitve");
            outputs[0].borrow_mut().transmit_end(e)?;
        }

        Ok(())
    }
}

struct LazyBinaryStateOperator<I1, I2, O, S, F> {
    func: Arc<F>,
    actives_left: HashMap<Tag, StateTransformer<I1, O, S>>,
    actives_right: HashMap<Tag, StateTransformer<I2, O, S>>,
    end_notify: HashMap<Tag, HashSet<ChannelId>>,
    ready: Vec<Tag>,
    states: HashMap<Tag, S>,
}

impl<I1, I2, O, S, F> LazyBinaryStateOperator<I1, I2, O, S, F>
    where I1: Data, I2: Data, O: Data, S: OperatorState, F: BinaryStateFunc<I1, I2, S, O>
{
    pub fn new(func: F) -> Self {
        LazyBinaryStateOperator {
            func: Arc::new(func),
            actives_left: HashMap::new(),
            actives_right: HashMap::new(),
            end_notify: HashMap::new(),
            ready: Vec::new(),
            states: HashMap::new(),
        }
    }
}

impl<I1, I2, O, S, F> OperatorCore for LazyBinaryStateOperator<I1, I2, O, S, F>
    where I1: Data, I2: Data, O: Data, S: OperatorState,
          F: BinaryStateFunc<I1, I2, S, O> + Send + Sync + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input1_borrow = inputs[0].borrow_mut();
        let input1 = InputHandle::<I1>::downcast(&mut input1_borrow);

        let mut input2_borrow = inputs[1].borrow_mut();
        let input2 = InputHandle::<I2>::downcast(&mut input2_borrow);

        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let func = &self.func;
        let states = &mut self.states;
        let mut tags = Vec::new();
        let actives_1 = &mut self.actives_left;
        let actives_2 = &mut self.actives_right;
        {
            input1.for_each_batch(|dataset| {
                let (t, d) = dataset.take();
                debug_assert!(!actives_1.contains_key(&t), "LazyBinaryState: unexpected tag in actives left;");
                debug_assert!(!actives_2.contains_key(&t), "LazyBinaryState: unexpected tag in actives left;");
                let mut session = output.session(&t);
                let func = func.clone();
                let s = states.remove(&t).unwrap_or(Default::default());
                let mut trans = StateTransformer::new(d, s,
                                                      move |item: I1, st: &mut S| func.left(item, st));
                while trans.has_next() {
                    let r = trans.next().expect("LazyBinaryState: has next error");
                    if !session.give(r)? {
                        break
                    }
                }

                if trans.has_next() {
                    actives_1.insert(t.clone(), trans);
                    tags.push(t);
                    Ok(false)
                } else {
                    let s = trans.return_state();
                    states.insert(t, s);
                    Ok(true)
                }
            })?;
        }

        if output.has_capacity() {
            input2.for_each_batch(|dataset| {
                let (t, d) = dataset.take();
                debug_assert!(!actives_1.contains_key(&t), "LazyBinaryState: unexpected tag in actives left;");
                debug_assert!(!actives_2.contains_key(&t), "LazyBinaryState: unexpected tag in actives left;");
                let mut session = output.session(&t);
                let func = func.clone();
                let s = states.remove(&t).unwrap_or(Default::default());
                let mut trans = StateTransformer::new(d, s,
                                                      move |item: I2, st: &mut S| func.right(item, st));
                while trans.has_next() {
                    let r = trans.next().expect("LazyBinary: has next error");
                    if !session.give(r)? {
                        break
                    }
                }

                if trans.has_next() {
                    actives_2.insert(t.clone(), trans);
                    tags.push(t);
                    Ok(true)
                } else {
                    let s = trans.return_state();
                    states.insert(t, s);
                    Ok(false)
                }
            })?;
        }


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
            let mut session = output.session(&t);
            if let Some(mut trans) = self.actives_left.remove(&t) {
                'IN1: while trans.has_next() {
                    let r = trans.next().expect("LazyBinary: has next error");
                    if !session.give(r)? {
                        break 'IN1;
                    }
                }
                if !trans.has_next() {
                    if i == actives.len() {
                        actives.remove(i);
                    } else {
                        actives.swap_remove(i);
                    }
                    let s = trans.return_state();
                    self.states.insert(t, s);
                } else {
                    self.actives_left.insert(t, trans);
                    break 'OUT;
                }
            } else {
                if let Some(mut trans) = self.actives_right.remove(&t) {
                    'IN2: while trans.has_next() {
                        let r = trans.next().expect("LazyBinary: has next error");
                        if !session.give(r)? {
                            break 'IN2;
                        }
                    }

                    if !trans.has_next() {
                        if i == actives.len() {
                            actives.remove(i);
                        } else {
                            actives.swap_remove(i);
                        }
                        let s = trans.return_state();
                        self.states.insert(t, s);
                    } else {
                        self.actives_right.insert(t, trans);
                        break 'OUT;
                    }
                } else {
                    warn!("LazyBinaryState: Operator inner active data don't have {:?}", t);
                }
            }
            i -= 1;
        }
        Ok(())
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(ch, t) = n {
                let count = {
                    let set = self.end_notify.entry(t.clone())
                        .or_insert(HashSet::new());
                    set.insert(ch);
                    set.len()
                };
                if count == 2 {
                    self.end_notify.remove(&t);
                    self.ready.push(t);
                }
            }
        }

        if !self.ready.is_empty() {
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<O>::downcast(&mut output_borrow);
            for e in self.ready.drain(..) {
                debug_assert!(!self.actives_left.contains_key(&e), "Receive end notify while data is still in active");
                debug_assert!(!self.actives_right.contains_key(&e), "Receive end notify while data is still in acitve");
                let mut session = output.session(&e);
                if let Some(s) = self.states.remove(&e) {
                    let result = self.func.notify(s);
                    for item in result {
                        session.give(item)?;
                    }
                } else {
                    warn!("LazyBinaryState: no state found for {:?}", e);
                }
                session.transmit_end()?;
            }
        }

        Ok(())
    }
}

impl<D1: Data, A: DataflowBuilder> LazyBinary<D1, A> for Stream<D1, A> {

    fn lazy_binary<D2, D3, C1, C2, B, L, R>(&self, name: &str, other: &Stream<D2, A>,
                                         comm_1: C1, comm_2: C2, constructor: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> (L, R),
              L: Transform<D1, D3> + 'static, R: Transform<D2, D3> + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let (left, right) = constructor(&op_info);
        let op_core = LazyBinaryOperator::new(left, right);
        self.add_binary(op_info, other, comm_1, comm_2, op_core)
    }

    fn lazy_binary_state<D2, D3, S, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>,
                                                  comm_1: C1, comm_2: C2, constructor: B) -> Stream<D3, A>
        where D2: Data, D3: Data, S: OperatorState, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: BinaryStateFunc<D1, D2, S, D3> + Send + Sync + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let func = constructor(&op_info);
        let op_core = LazyBinaryStateOperator::new(func);
        self.add_binary(op_info, other, comm_1, comm_2, op_core)
    }
}
