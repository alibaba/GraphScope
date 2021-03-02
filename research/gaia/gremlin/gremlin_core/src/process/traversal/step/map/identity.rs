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

use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{QueryParams, Vertex, VertexOrEdge};
use pegasus::api::function::{FnResult, MapFunction};
use std::collections::HashSet;

pub struct IdentityStep {
    pub params: QueryParams<Vertex>,
    as_labels: Vec<String>,
}

impl IdentityStep {
    pub fn new(props: Option<Vec<String>>) -> Self {
        let mut params = QueryParams::new();
        params.props = props;
        IdentityStep { params, as_labels: vec![] }
    }
}

struct IdentityFunc {
    pub params: QueryParams<Vertex>,
    labels: HashSet<String>,
}

impl Step for IdentityStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Identity
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        self.as_labels.as_slice()
    }
}

// TODO(bingqing): throw error
impl MapFunction<Traverser, Traverser> for IdentityFunc {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        if let Some(elem) = input.get_element() {
            if self.params.props.is_some() {
                // the case of preserving some properties in vertex in previous
                match elem.get() {
                    VertexOrEdge::V(v) => {
                        // the case of preserving properties on demand for vertex
                        let id = v.id;
                        let graph = crate::get_graph().unwrap();
                        let mut r = graph.get_vertex(&[id], &self.params).expect("failure");
                        if let Some(v) = r.next() {
                            Ok(input.modify_head(v, &self.labels))
                        } else {
                            panic!("vertex with id {} not found", id);
                        }
                    }
                    VertexOrEdge::E(_) => {
                        // the case that we assume all properties are already preserved for edge, so we do nothing
                        Ok(input)
                    }
                }
            } else {
                // the case of identity step or as step
                if let Some(head_element) = input.get_element() {
                    Ok(input.split(head_element.clone(), &self.labels))
                } else if let Some(head_object) = input.get_object() {
                    Ok(input.split_with_value(head_object.clone(), &self.labels))
                } else {
                    panic!("invalid head in identity;")
                }
            }
        } else {
            panic!("invalid input for identity;")
        }
    }
}

impl MapFuncGen for IdentityStep {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let labels = self.get_tags();
        let params = self.params.clone();
        Box::new(IdentityFunc { params, labels })
    }
}
