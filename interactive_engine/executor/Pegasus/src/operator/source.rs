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

use std::cell::RefCell;
use std::collections::HashMap;
use super::*;
use crossbeam_channel::{Receiver, TryRecvError};

lazy_static! {
    static ref SOURCE_TAG: Tag =  Tag::new(0) ;
}

/// Input source of the dataflow job, which produce a bounded/unbounded data set/stream at once;
pub trait IntoStream<D: Data>: Sized {

    fn into_stream<B: DataflowBuilder>(self, builder: &B) -> Stream<D, B>;

    fn into_stream_more<B: DataflowBuilder>(self, batch_size: usize, builder: &B) -> Stream<D, B>;
}

struct SourceOnce<D: Data> {
    src: Box<dyn Iterator<Item = D> + Send>,
    input_batch: usize,
}

impl<D: Data> SourceOnce<D> {
    pub fn new<I: Iterator<Item = D> + Send + 'static>(input_batch: usize, src: I) -> Self {
        SourceOnce {
            src: Box::new(src),
            input_batch
        }
    }
}

impl<D: Data> OperatorCore for SourceOnce<D> {
    // For the source, there is no input;
    fn on_receive(&mut self, _inputs: &[RefCell<Box<dyn TaggedInput>>], _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        unimplemented!()
    }

    fn on_active(&mut self, actives: &mut Vec<Tag>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        if !actives.is_empty() {
            debug_assert!(actives.len() == 1);
            //let active = actives.remove(0);
            //assert_eq!(*actives[0], SOURCE_TAG);
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<D>::downcast(&mut output_borrow);
            let mut session = output.session_of(0);
            if let Some(next) = self.src.next() {
                if session.give(next)? {
                    for data in self.src.by_ref().take(self.input_batch - 1) {
                        if !session.give(data)? {
                            break
                        }
                    }
                }
            } else {
                session.transmit_end()?;
                actives.clear();
            }
        }

        Ok(())
    }

    fn on_notify(&mut self, _notify: &mut Vec<Notification>,
                 _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        unimplemented!()
    }
}

pub struct StreamingSource<D: Data> {
    input: Receiver<(u32, Option<D>)>,
    batch_size: usize,
    buffer: HashMap<u32, Vec<D>>
}

#[allow(dead_code)]
impl<D: Data> StreamingSource<D> {
    pub fn new(batch: usize, input: Receiver<(u32, Option<D>)>) -> Self {
        StreamingSource {
            input,
            batch_size: batch,
            buffer: HashMap::new(),
        }
    }
}

impl<D: Data> OperatorCore for StreamingSource<D> {
    fn on_receive(&mut self, _inputs: &[RefCell<Box<dyn TaggedInput>>],
                  _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> FireResult {
        unimplemented!()
    }

    fn on_active(&mut self, actives: &mut Vec<Tag>, outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        if !actives.is_empty() {
            debug_assert!(actives.len() == 1);
            let mut output_borrow = outputs[0].borrow_mut();
            let output = OutputHandle::<D>::downcast(&mut output_borrow);
            loop {
                match self.input.try_recv() {
                    Ok((t, Some(r))) => {
                        let buf = self.buffer.entry(t).or_insert(Vec::new());
                        buf.push(r);
                        if buf.len() == self.batch_size {
                            let batch = ::std::mem::replace(buf, Vec::new());
                            output.session_of(t).give_batch(batch)?;
                        }
                    },
                    Ok((t, None)) => {
                        let mut session = output.session_of(t);
                        if let Some(batch) = self.buffer.remove(&t) {
                            session.give_batch(batch)?;
                        }
                        session.transmit_end()?;
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        info!("Client disconnected, make input exhausted ...");
                        actives.clear();
                    }
                }
            }
        }

        Ok(())
    }

    fn on_notify(&mut self, _notifies: &mut Vec<Notification>, _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        unimplemented!()
    }
}

impl<I: IntoIterator + Send + 'static> IntoStream<I::Item> for I
    where I::Item: Data, I::IntoIter: Send + 'static
{
    fn into_stream<B: DataflowBuilder>(self, builder: &B) -> Stream<<I as IntoIterator>::Item, B> {
       self.into_stream_more(2048, builder)
    }

    fn into_stream_more<B: DataflowBuilder>(self, batch_size: usize, builder: &B) -> Stream<I::Item, B> {
        let stream = Stream::new(Port::first(0), builder);
        let op_info = stream.allocate_operator_info("source");
        let src = SourceOnce::new(batch_size, self.into_iter().fuse());
        let output = stream.get_output().clone();
        let op = OperatorBuilder::new(op_info)
            .add_output(output, OutputDelta::None)
            .core(src);

        stream.add_operator(op);
        stream
    }
}

impl<D: Data> IntoStream<D> for StreamingSource<D> {
    fn into_stream<B: DataflowBuilder>(self, builder: &B) -> Stream<D, B> {
        self.into_stream_more(1, builder)
    }

    fn into_stream_more<B: DataflowBuilder>(self, _batch_size: usize, builder: &B) -> Stream<D, B> {
        let index = builder.allocate_operator_index();
        let info = OperatorInfo::new(builder.worker_id(), "source",
                                     index, builder.peers(), 1);
        info.set_source();
        let stream = Stream::new(Port::first(index), builder);
        let output = stream.get_output().clone();
        let op = OperatorBuilder::new(info)
            .add_output(output, OutputDelta::None)
            .core(self);
        stream.add_operator(op);
        stream
    }
}
