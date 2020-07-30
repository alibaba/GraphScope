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

use std::marker::PhantomData;
use std::cell::RefCell;
use std::collections::HashMap;
use super::*;
use crate::stream::Stream;
use crate::communication::Communicate;
use crate::ChannelId;
use crate::channel::DataSet;

/// Construct binary operators, which consume two input stream, and produce data to one output stream;
///
pub trait Binary<D1: Data, A> {

    fn binary<D2, D3, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>,
                                    comm_1: C1, comm_2: C2, builder: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(BinaryInput<D1, D2>, &mut OutputHandle<D3>) -> IOResult<()> + Send + 'static;

    fn binary_notify<D2, D3, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>,
                                           comm_1: C1, comm_2: C2, builder: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(Option<BinaryInput<D1, D2>>, &mut OutputHandle<D3>, Option<&[Tag]>) -> IOResult<()> + Send + 'static;

    fn binary_state<D2, D3, C1, C2, B, F, N, S>(&self, name: &str, other: &Stream<D2, A>,
                                                comm_1: C1, comm_2: C2, builder: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>, S: OperatorState,
              B: FnOnce(&OperatorInfo) -> (F, N),
              F: FnMut(BinaryInput<D1, D2>, &mut OutputHandle<D3>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
              N: FnMut(&[Tag], &mut OutputHandle<D3>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static;
}

/// A composite input stream proxy consist of two input streams;
pub struct BinaryInput<'a, I1: Data, I2: Data> {
    first: &'a mut InputHandle<I1>,
    second: &'a mut InputHandle<I2>,
}

impl<'a, I1: Data, I2: Data> BinaryInput<'a, I1, I2> {
    pub fn new(first: &'a mut InputHandle<I1>, second: &'a mut InputHandle<I2>) -> Self {
        BinaryInput {
            first,
            second
        }
    }

    #[inline]
    pub fn first_for_each<F>(&mut self, func: F) -> IOResult<usize>
        where F: FnMut(DataSet<I1>) -> IOResult<bool>
    {
        self.first.for_each_batch(func)
    }

    #[inline]
    pub fn second_for_each<F>(&mut self, func: F) -> IOResult<usize>
        where F: FnMut(DataSet<I2>) -> IOResult<bool>
    {
        self.second.for_each_batch(func)
    }
}

struct BinaryOperator<I1, I2, O, F> {
    receive_fn: F,
    end_notify: HashMap<Tag, HashSet<ChannelId>>,
    ready: Vec<Tag>,
    _ph: PhantomData<(I1, I2, O)>
}

impl<I1, I2, O, F> BinaryOperator<I1, I2, O, F>
    where I1: Data, I2: Data, O: Data,
          F: FnMut(BinaryInput<I1, I2>,&mut OutputHandle<O>) -> IOResult<()> + Send + 'static,
{
    pub fn new(receive_fn: F) -> Self {
        BinaryOperator {
            receive_fn,
            end_notify: HashMap::new(),
            ready: Vec::new(),
            _ph: PhantomData
        }
    }
}

impl<I1, I2, O, F> OperatorCore for BinaryOperator<I1, I2, O, F>
    where I1: Data, I2: Data, O: Data,
          F: FnMut(BinaryInput<I1, I2>, &mut OutputHandle<O>) -> IOResult<()> + Send + 'static,
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input1_borrow = inputs[0].borrow_mut();
        let mut input2_borrow = inputs[1].borrow_mut();

        let input1 = InputHandle::<I1>::downcast(&mut input1_borrow);
        let input2 = InputHandle::<I2>::downcast(&mut input2_borrow);

        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let input = BinaryInput::new(input1, input2);
        (self.receive_fn)(input, output)?;
        Ok(ScheduleState::Idle)
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
            outputs[0].borrow_mut().transmit_end(e)?;
        }

        Ok(())
    }
}

pub struct BinaryNotifyOperator<I1, I2, O, F> {
    func: F,
    end_notify: HashMap<Tag, HashSet<ChannelId>>,
    ready_end_notify: Vec<Tag>,
    _ph: PhantomData<(I1, I2, O)>
}

impl<I1, I2, O, F> BinaryNotifyOperator<I1, I2, O, F>
    where I1: Data, I2: Data, O: Data,
          F: FnMut(Option<BinaryInput<I1, I2>>, &mut OutputHandle<O>, Option<&[Tag]>) -> IOResult<()> + Send + 'static
{
    pub fn new(func: F) -> Self {
        BinaryNotifyOperator {
            func,
            end_notify: HashMap::new(),
            ready_end_notify: Vec::new(),
            _ph: PhantomData
        }
    }
}

impl<I1, I2, O, F> OperatorCore for BinaryNotifyOperator<I1, I2, O, F>
    where I1: Data, I2: Data, O: Data,
          F: FnMut(Option<BinaryInput<I1, I2>>, &mut OutputHandle<O>,
              Option<&[Tag]>) -> IOResult<()> + Send + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input1_borrow = inputs[0].borrow_mut();
        let mut input2_borrow = inputs[1].borrow_mut();

        let input1 = InputHandle::<I1>::downcast(&mut input1_borrow);
        let input2 = InputHandle::<I2>::downcast(&mut input2_borrow);

        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let input = BinaryInput::new(input1, input2);
        (self.func)(Some(input), output, None)?;
        Ok(ScheduleState::Idle)
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
                    self.ready_end_notify.push(t);
                }
            }
        }

        if !self.ready_end_notify.is_empty() {
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<O>::downcast(&mut output_borrow);
            (self.func)(None, output, Some(&self.ready_end_notify))?;

            for e in self.ready_end_notify.drain(..) {
                output.transmit_end(e)?;
            }
        }
        Ok(())
    }
}

pub struct BinaryStateOperator<I1, I2, O, F, N, S> {
    receive_fn : F,
    notify_fn : N,
    end_notify: HashMap<Tag, HashSet<ChannelId>>,
    ready_end_notify: Vec<Tag>,
    states: HashMap<Tag, S>,
    _ph: PhantomData<(I1, I2, O)>
}

impl<I1, I2, O, F, N, S> BinaryStateOperator<I1, I2, O, F, N, S>
    where I1: Data, I2: Data, O: Data, S: OperatorState,
          F: FnMut(BinaryInput<I1, I2>, &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
          N: FnMut(&[Tag], &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static
{
    pub fn new(receive: F, notify: N) -> Self {
        BinaryStateOperator {
            receive_fn: receive,
            notify_fn: notify,
            end_notify: HashMap::new(),
            ready_end_notify: Vec::new(),
            states: HashMap::new(),
            _ph: PhantomData
        }
    }
}

impl<I1, I2, O, F, N, S> OperatorCore for BinaryStateOperator<I1, I2, O, F, N, S>
    where I1: Data, I2: Data, O: Data, S: OperatorState,
          F: FnMut(BinaryInput<I1, I2>, &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
          N: FnMut(&[Tag], &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> Result<ScheduleState, IOError> {
        let mut input1_borrow = inputs[0].borrow_mut();
        let mut input2_borrow = inputs[1].borrow_mut();

        let input1 = InputHandle::<I1>::downcast(&mut input1_borrow);
        let input2 = InputHandle::<I2>::downcast(&mut input2_borrow);

        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let input = BinaryInput::new(input1, input2);
        let states = &mut self.states;
        (self.receive_fn)(input, output, states)?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> Result<(), IOError> {
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
                    self.ready_end_notify.push(t);
                }
            }
        }

        if !self.ready_end_notify.is_empty() {
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<O>::downcast(&mut output_borrow);
            (self.notify_fn)(&self.ready_end_notify, output, &mut self.states)?;
        }
        Ok(())
    }
}

impl<D1: Data, A: DataflowBuilder> Binary<D1, A> for Stream<D1, A> {
    fn binary<D2, D3, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>, comm_1: C1, comm_2: C2, builder: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(BinaryInput<D1, D2>, &mut OutputHandle<D3>) -> IOResult<()> + Send + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let receive_fn = builder(&op_info);
        let op_core = BinaryOperator::new(receive_fn);
        self.add_binary(op_info, other, comm_1, comm_2, op_core)
    }

    fn binary_notify<D2, D3, C1, C2, B, F>(&self, name: &str, other: &Stream<D2, A>,
                                              comm_1: C1, comm_2: C2, constructor: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>,
              B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(Option<BinaryInput<D1, D2>>, &mut OutputHandle<D3>, Option<&[Tag]>) -> IOResult<()> + Send + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let func = constructor(&op_info);
        let op_core = BinaryNotifyOperator::new(func);
        self.add_binary(op_info, other, comm_1, comm_2, op_core)

    }

    fn binary_state<D2, D3, C1, C2, B, F, N, S>(&self, name: &str, other: &Stream<D2, A>,
                                                comm_1: C1, comm_2: C2, builder: B) -> Stream<D3, A>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>, S: OperatorState,
              B: FnOnce(&OperatorInfo) -> (F, N),
              F: FnMut(BinaryInput<D1, D2>, &mut OutputHandle<D3>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
              N: FnMut(&[Tag], &mut OutputHandle<D3>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let (r, n) = builder(&op_info);
        let op_core = BinaryStateOperator::new(r, n );
        self.add_binary(op_info, other, comm_1, comm_2, op_core)
    }
}
