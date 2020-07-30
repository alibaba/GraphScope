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
use maxgraph_common::proto::query_flow::{OperatorBase, OdpsOutputConfig};

use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::message::{RawMessage, ValuePayload};
use dataflow::io::tunnel::*;

use std::sync::Arc;
use protobuf::parse_from_bytes;

pub struct WriteOdpsOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    write_count: Option<i64>,
}

impl<F> WriteOdpsOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase) -> Self {
        let output_config = parse_from_bytes::<OdpsOutputConfig>(base.get_argument().get_payload()).expect("parse odps output config");
        return WriteOdpsOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            write_count: None,
        };
    }
}

unsafe impl<F> Send for WriteOdpsOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {}

impl<F> Operator for WriteOdpsOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for WriteOdpsOperator<F>
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

    fn execute<'a>(&mut self, data: Vec<RawMessage>, _collector: &mut Box<'a + MessageCollector>) {
        for message in data.into_iter() {
            error!("write record error");
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if let Some(write_count) = self.write_count {
            info!("write {} records to tunnel", write_count);
            return Box::new(Some(RawMessage::from_value(ValuePayload::Long(write_count))).into_iter());
        } else {
            info!("There is None records is written to tunnel");
            return Box::new(None.into_iter());
        }
    }
}
