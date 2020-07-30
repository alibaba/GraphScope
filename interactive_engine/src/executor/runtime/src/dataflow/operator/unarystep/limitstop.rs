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

use dataflow::builder::{UnaryOperator, Operator, InputStreamShuffle, MessageCollector};
use dataflow::message::RawMessage;
use dataflow::manager::context::EarlyStopState;

use std::sync::Arc;

pub trait GlobalStopFlagOperator: UnaryOperator {
    fn generate_stop_flag(&mut self) -> bool;
}

pub struct GlobalLimitStopOperator {
    operator: Box<GlobalStopFlagOperator>,
    early_stop_state: Arc<EarlyStopState>,
}

impl GlobalLimitStopOperator {
    pub fn new(operator: Box<GlobalStopFlagOperator>,
               early_stop_state: Arc<EarlyStopState>) -> Self {
        GlobalLimitStopOperator {
            operator,
            early_stop_state,
        }
    }
}

impl Operator for GlobalLimitStopOperator {
    fn get_id(&self) -> i32 {
        self.operator.get_id()
    }
}

impl UnaryOperator for GlobalLimitStopOperator {
    fn get_input_id(&self) -> i32 {
        self.operator.get_input_id()
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        self.operator.get_input_shuffle()
    }

    fn get_stream_index(&self) -> i32 {
        self.operator.get_stream_index()
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        self.operator.execute(data, collector);
        if self.operator.generate_stop_flag() {
            self.early_stop_state.enable_global_stop();
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        self.operator.finish()
    }
}

pub struct GlobalStopFilterOperator {
    operator: Box<UnaryOperator>,
    early_stop_state: Arc<EarlyStopState>,
}

impl GlobalStopFilterOperator {
    pub fn new(operator: Box<UnaryOperator>,
               early_stop_state: Arc<EarlyStopState>) -> Self {
        GlobalStopFilterOperator {
            operator,
            early_stop_state,
        }
    }
}

impl Operator for GlobalStopFilterOperator {
    fn get_id(&self) -> i32 {
        self.operator.get_id()
    }
}

impl UnaryOperator for GlobalStopFilterOperator {
    fn get_input_id(&self) -> i32 {
        self.operator.get_input_id()
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        self.operator.get_input_shuffle()
    }

    fn get_stream_index(&self) -> i32 {
        self.operator.get_stream_index()
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        if self.early_stop_state.check_global_stop() {
            return;
        }

        self.operator.execute(data, collector);
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        self.operator.finish()
    }
}
