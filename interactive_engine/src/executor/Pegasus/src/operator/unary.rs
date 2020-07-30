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

/// Utilities for user to construct operators of unary primitive;
/// A operator of unary primitive consist of one input stream, one output stream, and the user-defined
/// data computing logic;
/// In the `Unary` trait define, the type parameter `D: Data` specific the data type of input stream,
/// and the type parameter `A` indicates the underlying runtime environment, usually this is transparent
/// to the users;
pub trait Unary<D: Data, A> {

    /// Create a simple unary operator only focus on handle each inbound data in the input stream;
    /// The `channel` parameter of the method specific the type of input stream, either a intra-process
    /// pipeline channel or a inter-processes shuffled channel, see the `Communication` module for more details;
    ///
    /// The `constructor` parameter is a user-defined `FnOnce` closure, it takes the `&OperatorInfo` as input,
    /// and generate the new `Fn` closure `F` as the core data-computing UDF, users can capture any reasonable local
    /// variables into the `F` if it is needed and applicable;
    ///
    /// `F` is a `Fn` closure, takes the input proxy `&InputHandle` and output proxy `&OutputHandle`
    /// as input parameters; The type parameter `D` of `InputHandle` indicate the data type of input stream,
    /// and the type parameter `D2` of `OutputHandle` indicate the data type of output stream;
    ///
    /// At runtime, the engine will invoke this function whenever the input stream is ready(to pull new data),
    /// and each time the function should consume the data in input and produce new data to output;
    ///
    /// #   Note!: `F` should be `Send` as it may be delivered between threads;
    ///
    /// # Returns
    /// Returns a new stream representation whose data were from the output of the just created operator;
    ///
    /// # Examples:
    /// ```
    /// use pegasus::operator::IntoStream;
    /// use pegasus::operator::Unary;
    /// use pegasus::communication::Pipeline;
    ///
    /// pegasus::run_local(1, 0, |worker| {
    ///     worker.dataflow("unary-example", |builder| {
    ///         (0..10).into_stream(builder)
    ///                .unary("unary", Pipeline, |info| {
    ///                     info.set_pass();
    ///                     |input, output| {
    ///                         input.for_each_batch(|dataset| {
    ///                             let mut session = output.session(&dataset);
    ///                             session.give_batch(dataset.data())
    ///                         })?;
    ///                         Ok(())
    ///                     }
    ///                 });
    ///             Ok(())
    ///     }).expect("build failure");
    /// }).unwrap();
    /// ```
    fn unary<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F: Fn(&mut InputHandle<D>, &mut OutputHandle<D2>) -> IOResult<()> + Send + 'static;

    /// Create a notification interested unary operator, which not only handles the inbound data,
    /// but also deals with notifications of the input stream;
    ///
    /// See the `unary` method for more descriptions about parameter `channel` and `constructor`;
    ///
    /// Unlike the `F` in `unary` method, this `F` function take three parameters, the first two are
    /// same with the `unary`, the last one contains `End` notifications of each tagged input stream;
    ///
    /// For example, if the parameter is not `None`, and the tag '[0]' exists in the inner slice, it
    /// means that the input data stream of '[0]' is end, no more data with this tag would appear in
    /// the future; According to these signals, users can customize special logic to respond to input
    /// data stream end;
    ///
    /// At runtime, the engine will invoke the `F` function whenever the input stream has any ready data
    /// or there are new notifications; It is left up to user to decide the order of handle data
    /// or notifications;
    ///
    /// # Note!:
    /// The UDF returned from the `constructor` should be `Send`;
    ///
    /// # Examples:
    /// ```
    /// use pegasus::operator::Unary;
    /// use pegasus::operator::IntoStream;
    /// use pegasus::communication::Pipeline;
    /// use std::collections::HashMap;
    /// // a count example;
    /// pegasus::run_local(1, 0, |worker| {
    ///     worker.dataflow("unary-notify-example", |builder| {
    ///         (0..10).into_stream(builder)
    ///                 .unary_notify("count-notify", Pipeline, |info| {
    ///                     info.set_clip();
    ///                     let mut counts = HashMap::new();
    ///                     move |input, output, notifications| {
    ///                         if let Some(input) = input {
    ///                             input.for_each_batch(|dataset| {
    ///                                 let (tag, data) = dataset.take();
    ///                                 let count = counts.entry(tag).or_insert(0);
    ///                                 *count += data.len();
    ///                                 Ok(true)
    ///                             })?;
    ///                         }
    ///                         if let Some(ends) = notifications {
    ///                             for end in ends {
    ///                                 // output the final count value after data stream is end;
    ///                                 let count = counts.remove(end).unwrap_or(0);
    ///                                 let mut session = output.session(end);
    ///                                 session.give(count)?;
    ///                             }
    ///                         }
    ///                         Ok(())
    ///                     }
    ///                  });
    ///         Ok(())
    ///     }).expect("build plan failure");
    /// }).unwrap();
    ///
    /// ```
    fn unary_notify<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(Option<&mut InputHandle<D>>, &mut OutputHandle<D2>, Option<&[Tag]>) -> IOResult<()> + Send + 'static;

    /// Create a unary operator, and register user interested states;
    /// See the `unary` method for more descriptions about parameter `channel`;
    ///
    /// As mentioned above, the input data stream can be classified into different scopes by the `tag`,
    /// see `Scope` module for more details; One state is attached and updated in one scope independently;
    ///
    /// The `constructor` parameter is a user-defined `FnOnce` closure, it takes the `&OperatorInfo` as input,
    /// and generate two `Fn` UDF closure;
    ///
    /// The first UDF is used to handle new inbound data, it will be invoked when input stream is ready(to pull);
    /// It takes three parameters as input, the first two are input stream proxy `&InputHandle<D>` and the
    /// output stream proxy `&OutputHandle<D>`, the last one is a map which maintains the states of each
    /// tagged input stream; Usually, users can get the unique `tag` of the input stream from inbound data,
    /// and then create or update state `S` according to the `tag`;
    ///
    /// The second UDF is used to handle ripe state; We say a state is ripe if it won't change(be updated)
    /// any more; A state would be ripe if and only if the input stream of it's attached scope is exhausted
    /// at current operator; This UDF take two parameters as input, the first is the output stream proxy,
    /// and the second is `Vec<(Tag, S)>`, a collection of tuples, each tuple consist of a `Tag`
    /// and a ripe state `S`, which means the state `S` of scope `Tag` is ripe. The UDF should deal with
    /// these ripe states, for example, output these states to the output stream;
    ///
    /// # Note:
    /// Both the data-handle UDF or the state-handle UDF should be `Send`;
    /// The user defined state should be `Send`;
    ///
    /// # Examples:
    /// ```rust
    /// use pegasus::operator::Unary;
    /// use pegasus::operator::IntoStream;
    /// use pegasus::communication::Pipeline;
    ///
    /// pegasus::run_local(1, 0, |worker| {
    ///     worker.dataflow("unary-state-example", |builder| {
    ///         (0..10).into_stream(builder)
    ///                .unary_state("count-state", Pipeline, |info| {
    ///                     info.set_clip();
    ///                     (
    ///                         |input, _output, states| {
    ///                            input.for_each_batch(|dataset| {
    ///                                 let (tag, data) = dataset.take();
    ///                                 let count = states.entry(tag).or_insert(0u64);
    ///                                 *count += data.len() as u64;
    ///                                 Ok(true)
    ///                            })?;
    ///                            Ok(())
    ///                         },
    ///                         |output, states| {
    ///                             for (t, count) in states {
    ///                                 output.session(&t).give(count)?;
    ///                             }
    ///                             Ok(())
    ///                         }
    ///                     )
    ///                 });
    ///         Ok(())
    ///     }).expect("build plan failure");
    /// }).unwrap();
    ///
    /// ```
    ///
    fn unary_state<D2, P, B, F, N, S>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, S: OperatorState + 'static,
              B: FnOnce(&OperatorInfo) -> (F, N),
              F: Fn(&mut InputHandle<D>, &mut OutputHandle<D2>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
              N: Fn(&mut OutputHandle<D2>, Vec<(Tag, S)>) -> IOResult<()> + Send + 'static;

}

/// Dataflow operator with single input, single output;
/// Type parameter `I` describes the static data type of input;
/// Type parameter `O` describes the static data type of output;
pub(crate) struct UnaryOperator<I, O, F> {
    receive_fn: F,
    _phantom: PhantomData<(I, O)>,
}

impl<I, O, F> UnaryOperator<I, O, F>
    where I: Data,
          O: Data,
          F: Fn(&mut InputHandle<I>, &mut OutputHandle<O>) -> IOResult<()>
{
    /// Create a new unary operator;
    pub fn new(receive_fn: F) -> Self {
        UnaryOperator {
            receive_fn,
            _phantom: PhantomData
        }
    }
}

impl<I, O, F> OperatorCore for UnaryOperator<I, O, F>
    where I: Data,
          O: Data,
          F: Fn(&mut InputHandle<I>, &mut OutputHandle<O>) -> IOResult<()> + Send,
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<I>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        (self.receive_fn)(input, output)?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, output: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                output[0].borrow_mut().transmit_end(t)?;
            }
        }
        Ok(())
    }
}


struct UnaryNotifyOperator<I, O, F> {
    func: F,
    end_notify: Vec<Tag>,
    _ph: PhantomData<(I, O)>
}

impl<I, O, F> UnaryNotifyOperator<I, O, F>
    where I: Data, O: Data,
          F: FnMut(Option<&mut InputHandle<I>>, &mut OutputHandle<O>, Option<&[Tag]>) -> IOResult<()> + Send
{
    pub fn new(func: F) -> Self {
        UnaryNotifyOperator {
            func,
            end_notify: Vec::new(),
            _ph: PhantomData
        }
    }
}

impl<I, O, F> OperatorCore for UnaryNotifyOperator<I, O, F>
    where I: Data, O: Data,
          F: FnMut(Option<&mut InputHandle<I>>, &mut OutputHandle<O>, Option<&[Tag]>) -> IOResult<()> + Send
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<I>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        (self.func)(Some(input), output, None)?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                self.end_notify.push(t);
            }
        }

        if !self.end_notify.is_empty() {
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<O>::downcast(&mut output_borrow);
            (self.func)(None, output, Some(&self.end_notify))?;
            for n in self.end_notify.drain(..) {
                output.transmit_end(n)?;
            }
        }
        Ok(())
    }
}

struct UnaryStateOperator<I, O, S, F, N> {
    states: HashMap<Tag, S>,
    receive_fn: F,
    notify_fn: N,
    end_notify: Vec<Tag>,
    _ph: PhantomData<(I, O)>
}

impl<I, O, S, F, N> UnaryStateOperator<I, O, S, F, N>
    where I: Data, O: Data, S: OperatorState,
          F: Fn(&mut InputHandle<I>, &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send,
          N: Fn(&mut OutputHandle<O>, Vec<(Tag, S)>) -> IOResult<()> + Send
{
    pub fn new(receive: F, notify: N) -> Self {
        UnaryStateOperator {
            states: HashMap::new(),
            receive_fn: receive,
            notify_fn: notify,
            end_notify: Vec::new(),
            _ph: PhantomData,
        }
    }
}

impl<I, O, S, F, N> OperatorCore for UnaryStateOperator<I, O, S, F, N>
    where I: Data, O: Data, S: OperatorState,
          F: Fn(&mut InputHandle<I>, &mut OutputHandle<O>, &mut HashMap<Tag, S>) -> IOResult<()> + Send,
          N: Fn(&mut OutputHandle<O>, Vec<(Tag, S)>) -> IOResult<()> + Send
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>], outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<I>::downcast(&mut input_borrow);
        let mut output_borrow = outputs[0].borrow_mut();
        let output = OutputHandle::<O>::downcast(&mut output_borrow);
        let states = &mut self.states;
        (self.receive_fn)(input, output, states)?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
         for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                self.end_notify.push(t);
            }
        }

        if !self.end_notify.is_empty() {
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<O>::downcast(&mut output_borrow);
            let states = &mut self.states;
            let mut mature_states = Vec::with_capacity(self.end_notify.len());
            for e in self.end_notify.iter() {
                if let Some((t, state)) = states.remove_entry(e) {
                    mature_states.push((t, state));
                }
            }

            (self.notify_fn)(output, mature_states)?;

            for e in self.end_notify.drain(..) {
                output.transmit_end(e)?;
            }
        }

       Ok(())
    }
}



impl<D: Data, A: DataflowBuilder> Unary<D, A> for Stream<D, A> {

    fn unary<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
        F: Fn(&mut InputHandle<D>, &mut OutputHandle<D2>) -> IOResult<()> + Send + 'static,
        {

            let op_info = self.allocate_operator_info(name);
            let receive_fn = constructor(&op_info);
            let op_core = UnaryOperator::new(receive_fn);
            self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }

    fn unary_notify<D2, P, B, F>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, P: Communicate<D>, B: FnOnce(&OperatorInfo) -> F,
              F: FnMut(Option<&mut InputHandle<D>>, &mut OutputHandle<D2>, Option<&[Tag]>) -> IOResult<()> + Send + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let func = constructor(&op_info);
        let op_core = UnaryNotifyOperator::new(func);
        self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }

    fn unary_state<D2, P, B, F, N, S>(&self, name: &str, channel: P, constructor: B) -> Stream<D2, A>
        where D2: Data, S: OperatorState + 'static, P: Communicate<D>,
              B: FnOnce(&OperatorInfo) -> (F, N),
              F: Fn(&mut InputHandle<D>, &mut OutputHandle<D2>, &mut HashMap<Tag, S>) -> IOResult<()> + Send + 'static,
              N: Fn(&mut OutputHandle<D2>, Vec<(Tag, S)>) -> IOResult<()> + Send + 'static
    {
        let op_info = self.allocate_operator_info(name);
        let (f, n ) = constructor(&op_info);
        let op_core = UnaryStateOperator::new(f, n);
        self.add_unary(op_info, channel, op_core, OutputDelta::None)
    }
}
