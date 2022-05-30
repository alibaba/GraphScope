//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::fmt::Debug;
use std::sync::Arc;

use ir_common::generated::algebra as algebra_pb;

use crate::error::{FnExecError, FnExecResult, FnGenResult};
use crate::graph::GraphSchema;
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::sink::GraphSinkGen;
use crate::process::record::Record;

#[derive(Clone)]
pub struct GraphWriter {
    graph_id: i64,
    graph_schema: GraphSchema,
}

impl Debug for GraphWriter {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        unimplemented!()
    }
}

impl Accumulator<Record, ()> for GraphWriter {
    fn accum(&mut self, next: Record) -> FnExecResult<()> {
        // TODO: ensure all vertices/edges stored on current
        let mut graph = crate::get_graph_writer().ok_or(FnExecError::NullGraphError)?;
        let entry = next
            .get(None)
            .ok_or(FnExecError::get_tag_error("empty head when accum in GraphWriter "))?;
        if let Some(v) = entry.as_graph_vertex() {
            // TODO: avoid clone
            Arc::get_mut(&mut graph)
                .ok_or(FnExecError::write_store_error("get graph writer failed"))?
                .add_vertex(v.clone())
                .map_err(|e| FnExecError::write_store_error(&e.to_string()))?;
            Ok(())
        } else if let Some(e) = entry.as_graph_edge() {
            // TODO: avoid clone
            Arc::get_mut(&mut graph)
                .ok_or(FnExecError::write_store_error("get graph writer failed"))?
                .add_edge(e.clone())
                .map_err(|e| FnExecError::write_store_error(&e.to_string()))?;
            Ok(())
        } else {
            Err(FnExecError::unexpected_data_error("neither vertex nor edge in GraphWriter"))?
        }
    }

    fn finalize(&mut self) -> FnExecResult<()> {
        let mut graph = crate::get_graph_writer().ok_or(FnExecError::NullGraphError)?;
        Arc::get_mut(&mut graph)
            .ok_or(FnExecError::write_store_error("get graph writer failed"))?
            .finish()
            .map_err(|e| FnExecError::write_store_error(&e.to_string()))
    }
}

impl GraphSinkGen for algebra_pb::SinkVineyard {
    fn gen_graph_writer(self) -> FnGenResult<GraphWriter> {
        unimplemented!()
    }
}
