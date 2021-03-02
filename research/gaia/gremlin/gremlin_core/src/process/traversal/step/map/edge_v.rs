//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use crate::process::traversal::step::map::MapFuncGen;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{QueryParams, Vertex, VertexOrEdge};
use pegasus::api::function::{FnResult, MapFunction};
use std::collections::HashSet;

enum Kind {
    OutV,
    InV,
    OtherV,
}

pub struct EdgeVertexStep {
    pub params: QueryParams<Vertex>,
    kind: Kind,
    as_labels: Vec<String>,
}

impl EdgeVertexStep {
    pub fn out_v() -> Self {
        EdgeVertexStep { params: QueryParams::new(), kind: Kind::OutV, as_labels: vec![] }
    }

    pub fn in_v() -> Self {
        EdgeVertexStep { params: QueryParams::new(), kind: Kind::InV, as_labels: vec![] }
    }

    pub fn other_v() -> Self {
        EdgeVertexStep { params: QueryParams::new(), kind: Kind::OtherV, as_labels: vec![] }
    }
}

impl Step for EdgeVertexStep {
    fn get_symbol(&self) -> StepSymbol {
        unimplemented!()
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        &self.as_labels
    }
}

struct EdgeVertexFunc {
    labels: HashSet<String>,
    params: QueryParams<Vertex>,
    get_src: bool,
}

impl MapFunction<Traverser, Traverser> for EdgeVertexFunc {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        if let Some(elem) = input.get_element() {
            match elem.get() {
                VertexOrEdge::E(e) => {
                    let id = if self.get_src { e.src_id } else { e.dst_id };
                    let graph = crate::get_graph().unwrap();
                    let mut r = graph.get_vertex(&[id], &self.params).expect("xxx");
                    if let Some(v) = r.next() {
                        return Ok(input.split(v, &self.labels));
                    } else {
                        // TODO: throw error;
                        panic!("vertex with id {} not found", e.src_id);
                    }
                }
                _ => (),
            }
        }
        panic!("invalid input for EdgeVertexStep;");
    }
}

impl MapFuncGen for EdgeVertexStep {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let labels = self.get_tags();
        let params = self.params.clone();
        match self.kind {
            Kind::OutV => Box::new(EdgeVertexFunc { labels, params, get_src: true }),
            Kind::InV => Box::new(EdgeVertexFunc { labels, params, get_src: false }),
            Kind::OtherV => unimplemented!(),
        }
    }
}
