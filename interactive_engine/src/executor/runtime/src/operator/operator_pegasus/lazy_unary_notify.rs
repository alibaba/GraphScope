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
use std::collections::HashMap;
use std::cell::RefCell;

use pegasus::Data;
use pegasus::communication::Communicate;
use pegasus::operator::{OperatorInfo, OperatorCore, ScheduleState, Notification, OperatorChain, FireResult};
use pegasus::stream::{Stream, DataflowBuilder};
use pegasus::tag::Tag;
use pegasus::channel::IOResult;
use pegasus::channel::input::{TaggedInput, InputHandle};
use pegasus::channel::output::{TaggedOutput, OutputHandle, OutputDelta};

use super::UTransform;

pub trait LazyUnaryNotify<D: Data, A> {
    fn lazy_unary_notify<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F:  UTransform<Vec<D>, D2> + 'static;
}

struct LazyUnaryNotifyOperator<I, O, F> {
    func: F,
    actives: HashMap<Tag, Box<dyn Iterator<Item=O> + Send>>,
    _ph: PhantomData<I>
}

impl<I, O, F> LazyUnaryNotifyOperator<I, O, F>
    where I: Data, O: Data, F: UTransform<Vec<I>, O>
{
    pub fn new(func: F) -> Self {
        LazyUnaryNotifyOperator {
            func,
            actives: HashMap::new(),
            _ph: PhantomData
        }
    }
}

impl<I, O, F> OperatorCore for LazyUnaryNotifyOperator<I, O, F>
    where I: Data, O: Data, F: UTransform<Vec<I>, O> + 'static
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
            debug_assert!(!self.actives.contains_key(&t));
            let result = self.func.exec(d, None);
            if let Some(mut result) = result {
                if !session.give_iterator(  &mut result)? {
                    self.actives.insert(t.clone(), result);
                    tags.push(t);
                    Ok(false)
                } else {
                    Ok(true)
                }
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
                let mut is_empty = true;
                'IN: while let Some(r) = trans.next() {
                    if !session.give(r)? {
                        is_empty = false;
                        break 'IN;
                    }
                }

                if !is_empty {
                    self.actives.insert(t, trans);
                    break 'OUT;
                } else {
                    if i == actives.len() {
                        actives.remove(i - 1);
                    } else {
                        actives.swap_remove(i - 1);
                    }
                }
            } else {
                error!("Operator inner active data don't have {:?}", t);
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
                let mut session = output.session(&t);
                let result = self.func.exec(vec![], Some(t));
                if let Some(result) = result {
                    session.give_entire_iterator(result)?;
                }
                session.transmit_end()?;
            }
        }
        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> LazyUnaryNotify<D, A> for Stream<D, A> {

    fn lazy_unary_notify<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F:  UTransform<Vec<D>, D2> + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let func = constructor(&op_info);
        let op_core = LazyUnaryNotifyOperator::<D, D2, _>::new(func);
        self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }
}

#[cfg(test)]
mod tests {
    use pegasus::run_local;
    use pegasus::communication::Pipeline;
    use pegasus::operator::LazyUnary;
    use pegasus::operator::IntoStream;

    use operator::operator_pegasus::lazy_unary_notify::LazyUnaryNotify;

    #[test]
    fn test_lazy_unary_notify() {
        run_local(1, 0, |worker| {
            worker.dataflow("test_lazy_unary", |builder| {
                (0..3u32).into_stream(builder)
                    .lazy_unary_notify("unary", Pipeline, |_info| {
                        |data: Vec<u32>, notification: Option<_>| {
                            let mut output = Vec::new();
                            if !data.is_empty() {
                                for item in data.into_iter() {
                                    for i in 0..1024 {
                                        output.push(item + 1);
                                    }
                                }
                                Some(output.into_iter())
                            } else {
                                if notification.is_some() {
                                    for i in 0..1024 {
                                        output.push(i);
                                    }
                                    Some(output.into_iter())
                                } else {
                                    None
                                }
                            }
                        }
                    })
                    .lazy_unary_state("count", Pipeline, |_info| {
                        (
                            |data: u32, count: &mut usize|  {
                                // println!("get data {:?}", data);
                                *count += 1;
                                Some(::std::iter::once(data))
                            },
                            |count: usize| {
                                println!("### [Test Lazy Unary] final count: {}", count);
                                assert_eq!(count, 4*1024);
                                vec![0]
                            }
                        )
                    });
                Ok(())
            }).expect("build plan failure");
        }).unwrap();
    }
}

