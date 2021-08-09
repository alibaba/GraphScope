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
use dataflow::builder::*;
use dataflow::message::RawMessage;
use dataflow::manager::context::RuntimeContext;
use dataflow::common::iterator::IteratorList;
use operator::operator_pegasus::lazy_unary_notify::LazyUnaryNotify;

use itertools::Itertools;

use pegasus::stream::Stream;
use pegasus::operator::source::IntoStream;
use pegasus::stream::DataflowBuilder as PegasusPlanBuilder;
use pegasus::common::Port;
use pegasus::communication::{Broadcast, Communicate, Exchange, Pipeline};
use pegasus::operator::unary::Unary;
use pegasus::operator::binary::Binary;
use pegasus::channel::output::TaggedOutput;
use pegasus::operator::branch::Branch;
use pegasus::operator::advanced::iterate::Iteration;

use maxgraph_store::api::{Vertex, Edge};

pub struct TinyDataflowBuilder<B: PegasusPlanBuilder> {
    builder: B,
    index: HashMap<(i32, i32), Port>,
}

impl<B: PegasusPlanBuilder> TinyDataflowBuilder<B> {
    pub fn new(builder: &B) -> Self {
        TinyDataflowBuilder {
            builder: builder.clone(),
            index: HashMap::new(),
        }
    }

    #[inline]
    pub fn get_stream(&self, operator_id: i32, stream_index: i32) -> Result<Stream<RawMessage, B>, String> {
        if let Some(port) = self.index.get(&(operator_id, stream_index)) {
            if let Some(stream) = self.builder.fetch_stream_meta::<RawMessage>(port) {
                Ok(Stream::with(stream, &self.builder))
            } else {
                Err(format!("no stream on port {:?}", port))
            }
        } else {
            Err(format!("no port found by {}/{}", operator_id, stream_index))
        }
    }
}

pub fn add_unary_operator<P, B>(comm: P, mut unary: Box<dyn UnaryOperator>,
                                stream: &Stream<RawMessage, B>) -> Port
    where P: Communicate<RawMessage>, B: PegasusPlanBuilder
{
    let stream = stream.lazy_unary_notify("lazy_unary_notify", comm, |_info| {
            move |data: Vec<RawMessage>, notifies: Option<_>| {
                if !data.is_empty() {
                    let mut collector: Box<MessageCollector> = Box::new(PegasusMessageCollector::new());
                    unary.execute(data, &mut collector);
                    collector.get_output_iterator()
                } else {
                    if notifies.is_some() {
                        Some(unary.finish())
                    } else {
                        None
                    }
                }
            }
    });

    stream.port()
}

pub fn add_binary_operator<P, B>(left_comm: P, binary: Box<dyn BinaryOperator>,
                                 left: &Stream<RawMessage, B>,
                                 right: &Stream<RawMessage, B>) -> Port
    where P: Communicate<RawMessage>, B: PegasusPlanBuilder
{
    let right_shuffle = binary.get_right_input_shuffle();
    match right_shuffle.get_shuffle_type() {
        ShuffleType::BROADCAST => {
            let right_comm = Broadcast::new();
            add_binary_operator_more(left_comm, right_comm, left, right, binary)
        }
        ShuffleType::EXCHANGE => {
            let right_comm = Exchange::new(move |v: &RawMessage| right_shuffle.route(v));
            add_binary_operator_more(left_comm, right_comm, left, right, binary)
        }
        ShuffleType::PIPELINE => {
            let right_comm = Pipeline;
            add_binary_operator_more(left_comm, right_comm, left, right, binary)
        }
    }
}

fn add_binary_operator_more<PL, PR, B>(left_comm: PL, right_comm: PR,
                                       left: &Stream<RawMessage, B>,
                                       right: &Stream<RawMessage, B>,
                                       mut binay: Box<dyn BinaryOperator>) -> Port
    where PL: Communicate<RawMessage>, PR: Communicate<RawMessage>, B: PegasusPlanBuilder
{
    let stream = left.binary_notify("binary", right, left_comm, right_comm, |_info| {
        move |mut input_op, outut, mut notify_op| {
            if let Some(ref mut input) = input_op {
                input.first_for_each(|dataset| {
                    let mut session = outut.session(&dataset);
                    for datum in dataset.data() {
                        let result = binay.execute_left(datum);
                        session.give_entire_iterator(result)?;
                    }
                    Ok(session.has_capacity())
                })?;

                if outut.has_capacity() {
                    input.second_for_each(|dataset| {
                        let mut session = outut.session(&dataset);
                        for datum in dataset.data() {
                            let result = binay.execute_right(datum);
                            session.give_entire_iterator(result)?;
                        }
                        Ok(session.has_capacity())
                    })?;
                }
            }

            if let Some(notifies) = notify_op.take() {
                for t in notifies {
                    let mut session = outut.session(t);
                    let result = binay.finish();
                    session.give_entire_iterator(result)?;
                }
            }

            Ok(())
        }
    });
    stream.port()
}


impl<B: PegasusPlanBuilder> DataflowBuilder for TinyDataflowBuilder<B> {
    fn get_worker_num(&self) -> i32 {
        self.builder.peers() as i32
    }

    fn get_worker_index(&self) -> i32 {
        self.builder.worker_id().1 as i32
    }

    fn get_processor_num(&self) -> i32 {
        self.builder.parallel().processes as i32
    }

    fn add_source(&mut self, mut source: Box<dyn SourceOperator>) {
        let op_id = source.get_id();
//        let source = source.execute().collect::<Vec<_>>();
//        let source = if self.get_worker_index() == 0 {
//            source.execute().collect::<Vec<_>>()
//        } else {
//            vec![]
//        };
        let stream = source.execute().into_stream(&self.builder);
        self.index.insert((op_id, 0), stream.port());
    }

    fn add_unary(&mut self, unary: Box<dyn UnaryOperator>) {
        let op_id = unary.get_id();
        let input_op_id = unary.get_input_id();
        let stream_idx = unary.get_stream_index();
        // TODO: Throw get stream error;
        let stream = self.get_stream(input_op_id, stream_idx).unwrap();
        let input_shuffle = unary.get_input_shuffle();
        let port = match input_shuffle.get_shuffle_type() {
            ShuffleType::BROADCAST => {
                let comm = Broadcast::new();
                add_unary_operator(comm, unary, &stream)
            }
            ShuffleType::EXCHANGE => {
                let comm = Exchange::new(move |v: &RawMessage| input_shuffle.route(v));
                add_unary_operator(comm, unary, &stream)
            }
            ShuffleType::PIPELINE => {
                add_unary_operator(Pipeline, unary, &stream)
            }
        };
        self.index.insert((op_id, 0), port);
    }

    fn add_binary(&mut self, binary: Box<dyn BinaryOperator>) {
        let op_id = binary.get_id();
        let left_input_id = binary.get_left_input_id();
        let right_input_id = binary.get_right_input_id();
        let left_stream_idx = binary.get_left_stream_index();
        let right_stream_idx = binary.get_right_stream_index();
        // TODO: Throw get stream error;
        let left = self.get_stream(left_input_id, left_stream_idx).unwrap();
        let right = self.get_stream(right_input_id, right_stream_idx).unwrap();
        let left_shuffle = binary.get_left_input_shuffle();
        let port = match left_shuffle.get_shuffle_type() {
            ShuffleType::BROADCAST => {
                let left_comm = Broadcast::new();
                add_binary_operator(left_comm, binary, &left, &right)
            }
            ShuffleType::EXCHANGE => {
                let left_comm = Exchange::new(move |v: &RawMessage| left_shuffle.route(v));
                add_binary_operator(left_comm, binary, &left, &right)
            }
            ShuffleType::PIPELINE => {
                let left_comm = Pipeline;
                add_binary_operator(left_comm, binary, &left, &right)
            }
        };

        self.index.insert((op_id, 0), port);
    }

    fn add_branch(&mut self, branch: Box<dyn BranchOperator>) {
        let id = branch.get_id();
        let input_id = branch.get_input_id();
        let input_stream_index = branch.get_stream_index();
        // TODO: Throw get stream error;
        let stream = self.get_stream(input_id, input_stream_index).unwrap();
        let cond = move |r: &RawMessage| branch.branch(r);
        let (left, right) = stream.branch("branch", cond);
        self.index.insert((id, 0), left.port());
        self.index.insert((id, 1), right.port());
    }

    fn add_loop<V, VI, E, EI, F>(&mut self, loop_operator: LoopOperator, script: &str, context: &RuntimeContext<V, VI, E, EI, F>)
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync {
        let id = loop_operator.get_id();
        let input_id = loop_operator.get_input_id();
        let input_stream_index = loop_operator.get_stream_index();
        // TODO: Throw get stream error;
        let stream = self.get_stream(input_id, input_stream_index).unwrap();
        let max_iter = loop_operator.get_loop_limit() as u32;

        let mut loop_builder = TinyDataflowBuilder::new(&self.builder);
        let stream = stream.iterate_more(max_iter, move |input| {
            loop_builder.index.insert((input_id, input_stream_index), input.port());
            loop_operator.build_loop(&mut loop_builder, script, context);
            let feedback = (loop_operator.get_feedback_id(), loop_operator.get_feed_stream_index());
            // TODO: Throw get stream error;
            let mut feedback_stream = loop_builder.get_stream(feedback.0, feedback.1).unwrap();
            if let Some(leave_id) = loop_operator.get_leave_id() {
                if leave_id < 0 {
                    (None, feedback_stream)
                } else {
                    // This should be a safe unwrap, because leave id is present;
                    let leave_index = loop_operator.get_leave_stream_index().unwrap();
                    let leave_stream = loop_builder.get_stream(leave_id, leave_index).unwrap();
                    if !loop_operator.get_loop_last_emit() {
                        let max_loop = loop_operator.get_loop_limit() as u32;
                        feedback_stream = feedback_stream.unary("emit", Pipeline, move |info| {
                            info.set_pass();
                            move |input, output| {
                                input.for_each_batch(|dataset| {
                                    let (t, d) = dataset.take();
                                    if t.current() < max_loop as u32 {
                                        output.session(&t).give_batch(d)?;
                                    }
                                    Ok(true)
                                })?;
                                Ok(())
                            }
                        })
                    }
                    (Some(leave_stream), feedback_stream)
                }
            } else {
                (None, feedback_stream)
            }
        });
        self.index.insert((id, 0), stream.port());
    }

    fn add_program<V, VI, E, EI, F>(&mut self, program: Box<ProgramOperator>, context: &RuntimeContext<V, VI, E, EI, F>) -> Result<(), String>
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync {
        let err_msg = format!("Program is not supported in pegasus_runtime.");
        error!("{}", err_msg);
        Err(format!("Program is not supported in pegasus_runtime."))
    }
}


struct PegasusMessageCollector {
    iterator_list: Vec<Box<Iterator<Item=RawMessage> + Send>>,
}

impl PegasusMessageCollector {
    pub fn new() -> Self {
        PegasusMessageCollector {
            iterator_list: vec![],
        }
    }
}

impl MessageCollector for PegasusMessageCollector {
    fn collect_message(&mut self, message: RawMessage) {
        self.iterator_list.push(Box::new(Some(message).into_iter()));
    }

    fn collect_iterator(&mut self, message_list: Box<Iterator<Item=RawMessage> + Send>) {
        self.iterator_list.push(message_list);
    }

    fn get_output_iterator(&mut self) -> Option<Box<Iterator<Item=RawMessage> + Send>> {
        let mut iterator_list = vec![];
        ::std::mem::swap(&mut self.iterator_list, &mut iterator_list);
        Some(Box::new(IteratorList::new(iterator_list)))
    }
}
