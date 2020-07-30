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
use crate::communication::Pipeline;
use crate::operator::unary::UnaryOperator;

pub trait Scope<D: Data, A> {

    fn enter<P: Communicate<D>>(&self, comm: P) -> Stream<D, A>;

    fn leave(&self) -> Stream<D, A>;
}

#[inline]
fn pass<D: Data>(input: &mut InputHandle<D>, output: &mut OutputHandle<D>) -> IOResult<()> {
    input.for_each_batch(|dataset| {
        output.session(&dataset).give_batch(dataset.data())?;
        Ok(true)
    })?;
    Ok(())
}

struct LeaveOp<D> {
    scope: usize,
    _ph: PhantomData<D>
}

impl<D: Data> LeaveOp<D> {
    pub fn new(scope: usize) -> Self {
        LeaveOp {
            scope,
            _ph: PhantomData
        }
    }
}

impl<D: Data> OperatorCore for LeaveOp<D> {
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<D>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<D>::downcast(&mut output_borrow);
        pass(input, output)?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>,
                 outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            match n {
                Notification::End(_, t) => {
                    // end event of either current scope or parent scope;
                    // as this operator's output is in parent scope, any current scope end event is meaningless,
                    // guard no end event would leak to parent;
                    if t.len() < self.scope {
                        trace!("{} leave scope {}", t, self.scope);
                        outputs[0].borrow_mut().transmit_end(t)?;
                    }
                },
                _ => ()
            }
        }
        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> Scope<D, A> for Stream<D, A> {
    fn enter<P: Communicate<D>>(&self, comm: P) -> Stream<D, A> {
        let name = format!("enter-{}", self.scopes() + 1);
        let info = self.allocate_operator_info(&name);
        info.set_pass();
        let core = UnaryOperator::<D, D, _>::new(pass);
        let new_stream = self.add_unary(info, comm, core, OutputDelta::ToChild);
        new_stream.into_scope();
        new_stream
    }

    fn leave(&self) -> Stream<D, A> {
        let name = format!("leave-{}", self.scopes());
        let info = self.allocate_operator_info(&name);
        info.set_pass();
        let core = LeaveOp::<D>::new(self.scopes());
        let new_stream = self.add_unary(info, Pipeline, core, OutputDelta::ToParent);
        new_stream.out_scope();
        new_stream
    }
}
