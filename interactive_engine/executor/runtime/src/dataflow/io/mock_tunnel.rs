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

use maxgraph_common::proto::query_flow::{OdpsQueryInput, OdpsOutputConfig, OdpsGraphInput};
use maxgraph_store::api::*;

use dataflow::message::RawMessage;

use std::sync::Arc;

pub fn load_odps_ids(_key: (i32, String, OdpsQueryInput)) -> Vec<i64> {
    vec![]
}

#[allow(dead_code)]
pub struct TunnelReader {}

impl TunnelReader {
    pub fn new(_query: OdpsQueryInput) -> Result<Self, String> {
        Ok(TunnelReader {})
    }
    pub fn is_match(&self) -> bool {
        false
    }
}

impl Iterator for TunnelReader {
    type Item = i64;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[allow(dead_code)]
pub struct TunnelWriter<V, VI, E, EI>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
}

impl<V, VI, E, EI> TunnelWriter<V, VI, E, EI>
    where V: 'static + Vertex, VI: 'static + Iterator<Item=V>, E: 'static + Edge, EI: 'static + Iterator<Item=E> {
    pub fn new(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>, _output_config: OdpsOutputConfig) -> Result<Self, String> {
        Ok(TunnelWriter { graph })
    }

    #[allow(dead_code)]
    pub fn write(&self, _m: &RawMessage) -> bool {
        false
    }
}

#[allow(dead_code)]
pub struct EdgeReader {}

impl EdgeReader {
    pub fn new(_input: OdpsGraphInput, _idx: u64, _count: u64) -> Result<Self, String> {
        Ok(EdgeReader {})
    }
}

impl Iterator for EdgeReader {
    type Item = RawMessage;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
