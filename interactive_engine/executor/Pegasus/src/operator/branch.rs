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
use super::*;
use crate::Data;
use crate::channel::input::InputHandle;
use crate::channel::output::OutputHandle;
use crate::channel::IOResult;
use crate::communication::Pipeline;

pub trait Condition<D: Data> : Send {
    fn predicate(&self, r: &D) -> bool;
}

impl<D: Data, F> Condition<D> for F where F: Fn(&D) -> bool + Send {
    #[inline]
    fn predicate(&self, r: &D) -> bool {
        (self)(r)
    }
}

impl<D: Data, F: ?Sized + Condition<D> + Sync> Condition<D> for Arc<F> {
    #[inline]
    fn predicate(&self, r: &D) -> bool {
        (**self).predicate(r)
    }
}

impl<D: Data> Condition<D> for bool {
    #[inline]
    fn predicate(&self, _r: &D) -> bool {
        *self
    }
}

impl<D: Data> Condition<D> for () {
    fn predicate(&self, _r: &D) -> bool {
        unimplemented!()
    }
}

struct BranchOperator<D, F> {
    condition: F,
    _ph: PhantomData<D>
}

impl<D, F> BranchOperator<D, F> where D: Data, F: Condition<D> {
    pub fn new(condition: F) -> Self {
        BranchOperator {
            condition,
            _ph: PhantomData
        }
    }
}

impl<D, F> OperatorCore for BranchOperator<D, F> where D: Data, F: Condition<D> {

    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<D>::downcast(&mut input_borrow);
        let mut output1_borrow = outputs[0].borrow_mut();
        let output1 = OutputHandle::<D>::downcast(&mut output1_borrow);
        let mut output2_borrow = outputs[1].borrow_mut();
        let output2 = OutputHandle::<D>::downcast(&mut output2_borrow);

        let condition = &self.condition;
        let mut has_capacity = true;
        input.for_each_batch(|dataset| {
            let mut session1 = output1.session(&dataset);
            let mut session2 = output2.session(&dataset);
            for datum in dataset.data() {
                if condition.predicate(&datum) {
                    has_capacity &= session1.give(datum)?;
                } else {
                    has_capacity &= session2.give(datum)?;
                }
            };
            Ok(has_capacity)
        })?;

        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                outputs[0].borrow_mut().transmit_end(t)?;
            }
        }
        Ok(())
    }
}

pub trait Branch<D: Data, A> {
    fn branch<F>(&self, name: &str, condition: F) -> (Stream<D, A>, Stream<D, A>) where F: Condition<D> + 'static;
}

impl<D: Data, A: DataflowBuilder> Branch<D, A> for Stream<D, A> {

    fn branch<F>(&self, name: &str, condition: F) -> (Stream<D, A>, Stream<D, A>) where F: Condition<D> + 'static {
        let op_info = self.allocate_operator_info(name);
        let op_core = BranchOperator::new(condition);
        let port1 = Port::first(op_info.index);
        let new_stream_left = Stream::from(port1, self);
        let port2 = Port::second(op_info.index);
        let new_stream_right = Stream::from(port2, self);
        let input = self.connect(port1, Pipeline);
        let output1 = new_stream_left.get_output().clone();
        let output2 = new_stream_right.get_output().clone();

        let op = OperatorBuilder::new(op_info)
            .add_input(input)
            .add_output(output1, OutputDelta::None)
            .add_output(output2, OutputDelta::None)
            .core(op_core);
        self.add_operator(op);
        (new_stream_left, new_stream_right)
    }
}
