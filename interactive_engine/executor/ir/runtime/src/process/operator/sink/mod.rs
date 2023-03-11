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
mod sink;
#[cfg(feature = "with_v6d")]
mod sink_vineyard;

use std::convert::TryInto;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::physical as pb;

use crate::error::FnGenResult;
use crate::process::operator::sink::sink::{DefaultSinkOp, RecordSinkEncoder};
#[cfg(feature = "with_v6d")]
use crate::process::operator::sink::sink_vineyard::{GraphSinkEncoder, SinkVineyardOp};

pub enum Sinker {
    DefaultSinker(RecordSinkEncoder),
    #[cfg(feature = "with_v6d")]
    GraphSinker(GraphSinkEncoder),
}

pub trait SinkGen {
    fn gen_sink(self) -> FnGenResult<Sinker>;
}

impl SinkGen for pb::PhysicalOpr {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        let op_kind = self.try_into()?;
        match op_kind {
            pb::physical_opr::operator::OpKind::Sink(sink) => sink.gen_sink(),
            _ => Err(ParsePbError::from("op is not a sink"))?,
        }
    }
}

impl SinkGen for pb::Sink {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        if let Some(sink_target) = self.sink_target {
            let inner = sink_target
                .inner
                .ok_or(ParsePbError::EmptyFieldError("sink_target inner is missing".to_string()))?;
            let tags = self
                .tags
                .into_iter()
                .map(|tag| tag.tag)
                .collect();
            match inner {
                algebra_pb::sink::sink_target::Inner::SinkDefault(sink_default) => {
                    let default_sink_op =
                        DefaultSinkOp { tags, id_name_mappings: sink_default.id_name_mappings };
                    default_sink_op.gen_sink()
                }
                algebra_pb::sink::sink_target::Inner::SinkVineyard(_sink_vineyard) => {
                    #[cfg(feature = "with_v6d")]
                    {
                        let sink_vineyard_op = SinkVineyardOp {
                            tags,
                            graph_name: _sink_vineyard.graph_name,
                            graph_schema: _sink_vineyard.graph_schema,
                        };
                        sink_vineyard_op.gen_sink()
                    }
                    #[cfg(not(feature = "with_v6d"))]
                    Err(graph_proxy::GraphProxyError::UnSupported(
                                    "sink_target of Vineyard is not as a feature. Try \'cargo build --features with_v6d\'".to_string()))?
                }
            }
        } else {
            Err(ParsePbError::EmptyFieldError("sink_target is missing".to_string()))?
        }
    }
}
