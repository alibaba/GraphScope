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

use crate::error::FnGenResult;
use crate::process::operator::sink::graph_writer::SinkVineyardOp;
use crate::process::operator::sink::sink::{DefaultSinkOp, RecordSinkEncoder};

pub enum Sinker {
    DefaultSinker(RecordSinkEncoder),
    GraphSinker(GraphWriter),
}

pub trait SinkGen {
    fn gen_sink(self) -> FnGenResult<Sinker>;
}

impl SinkGen for algebra_pb::logical_plan::Operator {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        if let Some(opr) = self.opr {
            match opr {
                algebra_pb::logical_plan::operator::Opr::Sink(sink) => {
                    if let Some(sink_target) = sink.sink_target {
                        let inner = sink_target
                            .inner
                            .ok_or(ParsePbError::EmptyFieldError(
                                "sink_target inner is missing".to_string(),
                            ))?;
                        match inner {
                            algebra_pb::sink::sink_target::Inner::SinkDefault(sink_default) => {
                                let default_sink_op = DefaultSinkOp {
                                    tags: sink.tags,
                                    id_name_mappings: sink_default.id_name_mappings,
                                };
                                default_sink_op.gen_sink()
                            }
                            algebra_pb::sink::sink_target::Inner::SinkVineyard(sink_vineyard) => {
                                let sink_vineyard_op = SinkVineyardOp {
                                    tags: sink.tags,
                                    graph_name: sink_vineyard.graph_name,
                                    graph_schema: sink_vineyard.graph_schema,
                                };
                                sink_vineyard_op.gen_sink()
                            }
                        }
                    } else {
                        Err(ParsePbError::EmptyFieldError("sink_target is missing".to_string()))?
                    }
                }
                _ => Err(ParsePbError::from("algebra_pb op is not a sink op"))?,
            }
        } else {
            Err(ParsePbError::EmptyFieldError("algebra op is empty".to_string()))?
        }
    }
}
