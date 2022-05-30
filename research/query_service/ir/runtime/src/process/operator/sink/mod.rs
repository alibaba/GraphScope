//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
mod graph_writer;
mod sink;

pub use graph_writer::GraphWriter;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::MapFunction;

use crate::error::FnGenResult;
use crate::process::record::Record;

pub trait SinkFunctionGen {
    fn gen_sink(self) -> FnGenResult<Box<dyn MapFunction<Record, Vec<u8>>>>;
}

pub trait GraphSinkGen {
    fn gen_graph_writer(self) -> FnGenResult<GraphWriter>;
}

impl SinkFunctionGen for algebra_pb::logical_plan::Operator {
    fn gen_sink(self) -> FnGenResult<Box<dyn MapFunction<Record, Vec<u8>>>> {
        if let Some(opr) = self.opr {
            match opr {
                algebra_pb::logical_plan::operator::Opr::Sink(sink) => sink.gen_sink(),
                _ => Err(ParsePbError::from("algebra_pb op is not a sink op"))?,
            }
        } else {
            Err(ParsePbError::EmptyFieldError("algebra op is empty".to_string()))?
        }
    }
}

impl GraphSinkGen for algebra_pb::logical_plan::Operator {
    fn gen_graph_writer(self) -> FnGenResult<GraphWriter> {
        if let Some(opr) = self.opr {
            match opr {
                algebra_pb::logical_plan::operator::Opr::Sink(sink) => {
                    if let Some(sink_target) = sink.sink_target {
                        if let Some(algebra_pb::sink::sink_target::Inner::SinkVineyard(sink_vineyard)) =
                            sink_target.inner
                        {
                            sink_vineyard.gen_graph_writer()
                        } else {
                            Err(ParsePbError::Unsupported(format!(
                                "SinkTarget {:?} in sink op is not a reduce operator",
                                sink_target
                            )))?
                        }
                    } else {
                        Err(ParsePbError::EmptyFieldError("SinkTarget in sink op".to_string()))?
                    }
                }
                _ => Err(ParsePbError::from("algebra_pb op is not a sink op"))?,
            }
        } else {
            Err(ParsePbError::EmptyFieldError("algebra op is empty".to_string()))?
        }
    }
}
