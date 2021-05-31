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

use maxgraph_store::api::*;
use maxgraph_common::proto::query_flow::OperatorBase;

use dataflow::manager::requirement::RequirementManager;
use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::manager::lambda::{LambdaManager, LambdaType};
use dataflow::manager::context::RuntimeContext;
use dataflow::builder::{Operator, UnaryOperator, MessageCollector, InputStreamShuffle};
use std::sync::Arc;
use dataflow::message::{RawMessage, RawMessageType};
use std::{fs,thread,time};
use maxgraph_common::proto::message::ListProto;
use maxgraph_common::proto::lambda_service::LambdaData;
use itertools::{zip, Itertools};
use std::collections::HashMap;
use execution::build_empty_router;

//Operator for LambdaMap and LambdaFlatMap
pub struct LambdaOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    shuffle_flag: bool,
    before_requirement: RequirementManager,
    after_requirement: RequirementManager,
    si: SnapshotId,
    lambda_manager: Arc<LambdaManager>,
    lambda_index: String,
    lambda_type: LambdaType,
}

impl<F> LambdaOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new<V, VI, E, EI>(base: &OperatorBase,
               input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               shuffle_flag: bool,
               context: &RuntimeContext<V, VI, E, EI, F>,
               lambda_manager: Arc<LambdaManager>,
               lambda_index: String,
               lambda_type: LambdaType) -> Self
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {

        LambdaOperator{
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            shuffle_flag,
            before_requirement: RequirementManager::new(base.get_before_requirement().to_vec()),
            after_requirement: RequirementManager::new(base.get_after_requirement().to_vec()),
            si: context.get_snapshot_id(),
            lambda_manager,
            lambda_index,
            lambda_type
        }
    }
}

impl<F> Operator for LambdaOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for LambdaOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a+MessageCollector>) {
        let mut message_list = ListProto::new();

        let orig_data = data.clone();

        for message in data.into_iter() {
            let message = self.before_requirement.process_requirement(message);
            if message.get_message_type() == RawMessageType::VERTEX {
                unimplemented!();
            }
            let empty_fn = Arc::new(build_empty_router());
            message_list.mut_value().push(message.to_proto(Some(empty_fn.as_ref())));
        }

         let result_list = match self.lambda_type {
             LambdaType::FILTER => {
                 let mut _result_list = vec![];
                 let result_id_set= self.lambda_manager.send_lambda_filter_query(message_list, &self.lambda_index);
                 for message in orig_data.into_iter() {
                     if result_id_set.contains(&message.get_id()) {
                         _result_list.push(self.after_requirement.process_requirement(message));
                     }
                 }
                 _result_list
             },
             LambdaType::MAP => {
                 let mut _result_list = vec![];

                 let mut list_proto= self.lambda_manager.send_lambda_map_query(message_list, &self.lambda_index);
                 for (mut message, mut result_proto) in zip(orig_data.into_iter(), list_proto.take_value().into_iter()) {
                     let mut result = RawMessage::from_proto(&mut result_proto);
                     self.after_requirement.process_take_extend_entity(&mut message, &mut result);
                     _result_list.push(self.after_requirement.process_requirement(result));
                 }

                 _result_list
             },
             LambdaType::FLATMAP => {
                 let mut _result_list = vec![];

                 let (mut list_proto, mut result_id_list) = self.lambda_manager.send_lambda_flatmap_query(message_list, &self.lambda_index);
                 let mut orig_data_iter = orig_data.iter();
                 let mut orig_one_message = orig_data_iter.next();

                 for (mut result_proto, result_id) in zip(list_proto.take_value().into_iter(), result_id_list.take_value().into_iter()){
                     let mut result = RawMessage::from_proto(&mut result_proto);

                     if orig_one_message.unwrap().get_id() != result_id {
                         orig_one_message = orig_data_iter.next();
                     }
                     let mut orig_one_message_unwarp = orig_one_message.unwrap().clone();
                     self.after_requirement.process_take_extend_entity(&mut orig_one_message_unwarp, &mut result);
                     _result_list.push(self.after_requirement.process_requirement(result));
                 }

                 _result_list
             },
         };

        collector.collect_iterator(Box::new(result_list.into_iter()));
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}
