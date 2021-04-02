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
use crate::process::traversal::step::{MapFuncGen, RemoveLabel, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{QueryParams, Tag, Vertex, VertexOrEdge};
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct IdentityStep {
    pub params: QueryParams<Vertex>,
    as_labels: Vec<Tag>,
    remove_labels: Vec<Tag>,
}

impl IdentityStep {
    pub fn new(props: Option<Vec<String>>) -> Self {
        let mut params = QueryParams::new();
        params.props = props;
        IdentityStep { params, as_labels: vec![], remove_labels: vec![] }
    }
}

struct IdentityFunc {
    params: QueryParams<Vertex>,
    labels: BitSet,
    remove_labels: BitSet,
}

impl Step for IdentityStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Identity
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[Tag] {
        self.as_labels.as_slice()
    }
}

impl RemoveLabel for IdentityStep {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_labels.push(label);
    }

    fn remove_tags(&self) -> &[Tag] {
        self.remove_labels.as_slice()
    }
}

// runtime identity step is used in the following scenarios:
// 1. g.V().out().identity(props), where identity gives the props needs to be saved (will shuffle to out vertex firstly)
// TODO: 2. g.V().outE().as("a"), where identity may gives the tag "a". We do this only because compiler may give plan like this.
// 3. g.V().union(identity(), both()), which is the real gremlin identity step
// 4. g.V().count().as("a"), where identity gives the tag "a", since count() is an op in engine
// 5. Give hint of tags to remove. Since we may not able to remove tags in some OpKind, e.g., Filter, Sort, Group, etc, we add identity (map step) to remove tags.
impl MapFunction<Traverser, Traverser> for IdentityFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if let Some(elem) = input.get_element() {
            if self.params.props.is_some() {
                // the case of preserving some properties in vertex in previous
                match elem.get() {
                    VertexOrEdge::V(v) => {
                        // the case of preserving properties on demand for vertex
                        let id = v.id;
                        let graph = crate::get_graph().unwrap();
                        let mut r = graph.get_vertex(&[id], &self.params)?;
                        if let Some(v) = r.next() {
                            input.modify_head(v, &self.labels);
                            input.remove_labels(&self.remove_labels);
                            Ok(input)
                        } else {
                            Err(str_to_dyn_error(&format!("vertex with id {} not found", id)))
                        }
                    }
                    // TODO: there is no need to add identity after edge, check with Compiler
                    VertexOrEdge::E(_e) => {
                        // the case that we assume all properties are already preserved for edge, so we do not query the edges
                        if !self.labels.is_empty() {
                            input.add_labels(&self.labels);
                        }
                        input.remove_labels(&self.remove_labels);
                        Ok(input)
                    }
                }
            } else {
                // the case of identity step
                // TODO: check with compiler when do this
                if !self.labels.is_empty() {
                    input.add_labels(&self.labels);
                }
                input.remove_labels(&self.remove_labels);
                Ok(input)
            }
        } else if let Some(_) = input.get_object() {
            // the case of as step, e.g., g.V().count().as("a")
            if !self.labels.is_empty() {
                input.add_labels(&self.labels);
            }
            input.remove_labels(&self.remove_labels);
            Ok(input)
        } else {
            Err(str_to_dyn_error("invalid head in identity"))
        }
    }
}

impl MapFuncGen for IdentityStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let labels = self.get_tags();
        let params = self.params.clone();
        let remove_labels = self.get_remove_tags();
        Ok(Box::new(IdentityFunc { params, labels, remove_labels }))
    }
}
