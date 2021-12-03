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

use dataflow::plan::query_plan::QueryFlowPlan;
use dataflow::message::RawMessage;
use dataflow::manager::context::RuntimeContext;
use maxgraph_store::api::{Vertex, Edge};
use maxgraph_common::proto::query_flow::OperatorType;

// Shuffle type
#[derive(Clone)]
pub enum ShuffleType {
    PIPELINE,
    EXCHANGE,
    BROADCAST,
}

// Stream Shuffle
pub trait InputStreamShuffle: Send + Sync {
    fn get_shuffle_type(&self) -> &ShuffleType;

    // route message to u64 for EXCHANGE shuffle type
    fn route(&self, message: &RawMessage) -> u64;
}

// Base operator with id
pub trait Operator: Send {
    fn get_id(&self) -> i32;
}

// Source operator
pub trait SourceOperator: Operator {
    fn execute(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send>;
}

// Unary operator
pub trait UnaryOperator: Operator {
    // get input operator id
    fn get_input_id(&self) -> i32;

    // get input stream shuffle
    fn get_input_shuffle(&self) -> Box<InputStreamShuffle>;

    // get input stream index
    fn get_stream_index(&self) -> i32;

    // execute input message
    fn execute<'a>(&mut self, message: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>);

    // input stream finish
    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send>;
}

// Binary operator
pub trait BinaryOperator: Operator {
    // get left input id
    fn get_left_input_id(&self) -> i32;

    // get left stream index
    fn get_left_stream_index(&self) -> i32;

    // get left input stream shuffle
    fn get_left_input_shuffle(&self) -> Box<InputStreamShuffle>;

    // get right input id
    fn get_right_input_id(&self) -> i32;

    // get right stream index
    fn get_right_stream_index(&self) -> i32;

    // get right input stream shuffle
    fn get_right_input_shuffle(&self) -> Box<InputStreamShuffle>;

    // execute left message
    fn execute_left(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>>;

    // execute right message
    fn execute_right(&mut self, message: RawMessage) -> Box<Iterator<Item=RawMessage>>;

    // left and right stream finish
    fn finish(&mut self) -> Box<Iterator<Item=RawMessage>>;
}

// Branch operator
pub trait BranchOperator: Operator {
    // branch message to true/false, when true, the message will send to stream with index 0,
    // other wise the message will send stream with index 1
    fn branch(&self, message: &RawMessage) -> bool;

    // get input id of branch operator
    fn get_input_id(&self) -> i32;

    // get input stream index
    fn get_stream_index(&self) -> i32;
}

// Program operator
pub trait ProgramOperator: Operator {
    // get input operator id
    fn get_input_id(&self) -> i32;

    // get input stream index
    fn get_stream_index(&self) -> i32;

    // get program type
    fn get_program_type(&self) -> OperatorType;

    // get program argument payload
    fn get_program_argument(&self) -> &Vec<u8>;
}

// Output the given message or iterator of message
pub trait MessageCollector {
    // Output single message
    fn collect_message(&mut self, message: RawMessage);

    // Output message list
    fn collect_iterator(&mut self, message_list: Box<Iterator<Item=RawMessage> + Send>);

    fn get_output_iterator(&mut self) -> Option<Box<Iterator<Item=RawMessage> + Send>> {
        None
    }
}

// Dataflow builder
pub trait DataflowBuilder {
    // total num of workers
    fn get_worker_num(&self) -> i32;

    // worker index
    fn get_worker_index(&self) -> i32;

    // total num of processor
    fn get_processor_num(&self) -> i32;

    // add source operator
    fn add_source(&mut self, source: Box<SourceOperator>);

    // add unary operator
    fn add_unary(&mut self, unary: Box<UnaryOperator>);

    // add binary operator
    fn add_binary(&mut self, binary: Box<BinaryOperator>);

    // add branch operator
    fn add_branch(&mut self, branch: Box<BranchOperator>);

    // add loop operator
    fn add_loop<V, VI, E, EI, F>(&mut self, loop_operator: LoopOperator, script: &str, context: &RuntimeContext<V, VI, E, EI, F>)
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync;

    fn add_program<V, VI, E, EI, F>(&mut self, program: Box<ProgramOperator>, context: &RuntimeContext<V, VI, E, EI, F>) -> Result<(), String>
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync;
}

pub struct LoopOperator {
    id: i32,
    input_id: i32,
    stream_index: i32,
    feedback_id: i32,
    feed_stream_index: i32,
    leave_id: Option<i32>,
    leave_stream_index: Option<i32>,
    loop_limit: i32,
    loop_last_emit: bool,
    // if true, emit last loop output
    plan: QueryFlowPlan,
}

impl LoopOperator {
    pub fn new(id: i32,
               input_id: i32,
               stream_index: i32,
               feedback_id: i32,
               feed_stream_index: i32,
               leave_id: Option<i32>,
               leave_stream_index: Option<i32>,
               loop_limit: i32,
               loop_last_emit: bool,
               plan: QueryFlowPlan) -> Self {
        LoopOperator {
            id,
            input_id,
            stream_index,
            feedback_id,
            feed_stream_index,
            leave_id,
            leave_stream_index,
            loop_limit,
            loop_last_emit,
            plan,
        }
    }

    pub fn get_id(&self) -> i32 {
        self.id
    }

    // get input id for loop operator
    pub fn get_input_id(&self) -> i32 {
        self.input_id
    }

    // get input stream index
    pub fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    pub fn get_feedback_id(&self) -> i32 {
        self.feedback_id
    }

    pub fn get_feed_stream_index(&self) -> i32 {
        self.feed_stream_index
    }

    pub fn get_leave_id(&self) -> Option<i32> {
        self.leave_id
    }

    pub fn get_leave_stream_index(&self) -> Option<i32> {
        self.leave_stream_index
    }

    pub fn get_loop_limit(&self) -> i32 {
        self.loop_limit
    }

    pub fn get_loop_last_emit(&self) -> bool {
        self.loop_last_emit
    }

    pub fn build_loop<V, VI, E, EI, F>(&self,
                           loop_builder: &mut impl DataflowBuilder,
                           script: &str,
                           context: &RuntimeContext<V, VI, E, EI, F>)
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + Send + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + Send + 'static,
              F: Fn(&i64) -> u64 + 'static + Send + Sync {
        let _result = self.plan.build(loop_builder, context.get_query_id(), script, context);
    }
}

// Builder for loop
pub trait DataflowLoopBuilder: DataflowBuilder {
    // get feedback operator id
    fn get_feedback(&self) -> i32;

    // get feedback stream index
    fn get_feedback_stream_index(&self) -> i32;

    // get leave operator id
    fn get_leave(&self) -> Option<i32>;

    // get leave stream index
    fn get_leave_stream_index(&self) -> i32;
}
