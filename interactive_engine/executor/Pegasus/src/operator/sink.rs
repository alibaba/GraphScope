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
use std::marker::PhantomData;
use crate::common::Bytes;
use crate::communication::Pipeline;
use crate::serialize::{CPSerialize, write_binary};

pub enum SinkStream<D> {
    Data(Vec<D>),
    END
}

pub trait Sink<D: Data> {
    fn sink<B, F>(&self, func: B) where B: FnOnce(&OperatorInfo) -> F,
                                        F: Fn(u32, SinkStream<D>) + Send + 'static;
}

pub trait BinarySinker: Send + 'static {
    fn sink(&mut self, res: Bytes) -> IOResult<()>;
}

/// Serialize messages and sink them out;
/// use `sinker` to do sink, the user defined `FnOnce` function `func` is used to create a new
/// `Fn` function, which accept a tuple of `(Tag, SinkStream<D>)`, and convert it to new format message,
/// which would be serialized and sink efficiently;
/// The type of converted message must implement `CPSerialize`;
/// # Note:
/// System doesn't care which information will be contained in the binary, it is user's responsibility to
/// define their own's logic to create messages with enough information. System will make true that these
/// messages will be sink as binary successfully or error reported;
pub trait SinkBinary<D: Data> {
    fn sink_bytes<B, F, S, C>(&self, sinker: S, func: B)
        where C: CPSerialize + Send + 'static,
              B: FnOnce(&OperatorInfo) -> F,
              S: BinarySinker,
              F: Fn(u32, SinkStream<D>) -> Option<C> + Send + 'static;
}

pub struct SinkOperator<D, F> {
    sink: F,
    _ph: PhantomData<D>,
}

impl<D, F> SinkOperator<D, F> where F: Fn(u32, SinkStream<D>) + Send
{
    pub fn new(sink: F) -> Self {
        SinkOperator {
            sink,
            _ph: PhantomData
        }
    }
}

impl<D, F> OperatorCore for SinkOperator<D, F> where D: Data, F: Fn(u32, SinkStream<D>) + Send
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<D>::downcast(&mut input_borrow);
        input.for_each_batch(|dataset| {
            let (t, data) = dataset.take();
            (self.sink)(t.current(), SinkStream::Data(data));
            Ok(true)
        })?;

        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>,
                 _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                (self.sink)(t.current(), SinkStream::END);
            }
        }
        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> Sink<D> for Stream<D, A> {
    fn sink<B, F>(&self, constructor: B)
        where B: FnOnce(&OperatorInfo) -> F,
              F: Fn(u32, SinkStream<D>) + Send + 'static
    {
        let info = self.allocate_operator_info("sink");
        let sink = constructor(&info);
        let op_core = SinkOperator::new(sink);
        let port = Port::first(info.index);
        let input = self.connect(port, Pipeline);
        let op = OperatorBuilder::new(info)
            .add_input(input)
            .core(op_core);
        self.add_operator(op)
    }
}



impl <F: ?Sized + Fn(Bytes) -> IOResult<()> + Send + 'static> BinarySinker for F {
    #[inline]
    fn sink(&mut self, res: Bytes) -> Result<(), IOError> {
        (*self)(res)
    }
}

pub struct SinkBinaryOperator<D, B, F, S> {
    fold: F,
    sinker: S,
    _ph: PhantomData<(B, D)>
}

impl<D: Data, B: CPSerialize, F, S> SinkBinaryOperator<D, B, F, S>
    where F: Fn(u32, SinkStream<D>) -> Option<B> + Send , S: BinarySinker
{
    pub fn new(fold: F, sink: S) -> Self {
        SinkBinaryOperator {
            fold,
            sinker: sink,
            _ph: PhantomData
        }
    }
}

impl<D, B, F, S> OperatorCore for SinkBinaryOperator<D, B, F, S>
    where D: Data, B: CPSerialize + Send, S: BinarySinker,
          F: Fn(u32, SinkStream<D>) -> Option<B> + Send
{
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
                  _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        let mut input_borrow = inputs[0].borrow_mut();
        let input = InputHandle::<D>::downcast(&mut input_borrow);
        input.for_each_batch(|dataset| {
            let (t, data) = dataset.take();
            let result = (self.fold)(t.current(), SinkStream::Data(data));
            if let Some(r) = result {
                let bytes = write_binary(&r)?;
                self.sinker.sink(bytes)?;
            }
            Ok(true)
        })?;
        Ok(ScheduleState::Idle)
    }

    fn on_notify(&mut self, notifies: &mut Vec<Notification>,
                 _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        for n in notifies.drain(..) {
            if let Notification::End(_, t) = n {
                let notify = (self.fold)(t.current(), SinkStream::END);
                if let Some(n) = notify {
                    let bytes = write_binary(&n)?;
                    self.sinker.sink(bytes)?;
                }
            }
        }
        Ok(())
    }
}

impl<D: Data, A: DataflowBuilder> SinkBinary<D> for Stream<D, A> {
    fn sink_bytes<B, F, S, C>(&self, sinker: S, constructor: B)
        where C: CPSerialize + Send + 'static,
              B: FnOnce(&OperatorInfo) -> F,
              S: BinarySinker,
              F: Fn(u32, SinkStream<D>) -> Option<C> + Send + 'static
    {
        let info = self.allocate_operator_info("sink_binary");
        let fold = constructor(&info);
        let op_core = SinkBinaryOperator::new(fold, sinker);
        let port = Port::first(info.index);
        let input = self.connect(port, Pipeline);
        let op = OperatorBuilder::new(info)
            .add_input(input)
            .core(op_core);
        self.add_operator(op)
    }
}


