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

use crate::generated::gremlin as pb;
use crate::process::traversal::step::MapFuncGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{QueryParams, Vertex, VertexOrEdge};
use crate::{str_to_dyn_error, DynResult, FromPb};
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

struct IdentityFunc {
    params: QueryParams<Vertex>,
    tags: BitSet,
    remove_tags: BitSet,
}

// runtime identity step is used in the following scenarios:
// 1. g.V().out().identity(props), where identity gives the props needs to be saved (will shuffle to out vertex firstly)
// TODO: 2. g.V().outE().as("a"), where identity may gives the tag "a". We do this only because compiler may give plan like this.
// 3. g.V().union(identity(), both()), which is the real gremlin identity step
// 4. g.V().count().as("a"), where identity gives the tag "a", since count() is an op in engine
// 5. Give hint of tags to remove. Since we may not able to remove tags in some OpKind, e.g., Filter, Sort, Group, etc, we add identity (map step) to remove tags.
impl MapFunction<Traverser, Traverser> for IdentityFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if let Some(elem) = input.get_element_mut() {
            if self.params.props.is_some() {
                // the case of preserving some properties in vertex in previous
                match elem.get_mut() {
                    VertexOrEdge::V(ori_v) => {
                        // the case of preserving properties on demand for vertex
                        let id = ori_v.id;
                        let graph = crate::get_graph().unwrap();
                        let mut r = graph.get_vertex(&[id], &self.params)?;
                        if let Some(v) = r.next() {
                            *ori_v = v;
                            input.add_tags(&self.tags);
                            input.remove_tags(&self.remove_tags);
                            Ok(input)
                        } else {
                            Err(str_to_dyn_error(&format!("vertex with id {} not found", id)))
                        }
                    }
                    // TODO: there is no need to add identity after edge, check with Compiler
                    VertexOrEdge::E(_ori_e) => {
                        // the case that we assume all properties are already preserved for edge, so we do not query the edges
                        input.add_tags(&self.tags);
                        input.remove_tags(&self.remove_tags);
                        Ok(input)
                    }
                }
            } else {
                // the case of identity step
                input.add_tags(&self.tags);
                input.remove_tags(&self.remove_tags);
                Ok(input)
            }
        } else if let Some(_) = input.get_object() {
            // the case of as step, e.g., g.V().count().as("a")
            input.add_tags(&self.tags);
            input.remove_tags(&self.remove_tags);
            Ok(input)
        } else {
            Err(str_to_dyn_error("invalid head in identity"))
        }
    }
}

pub struct IdentityStep {
    pub step: pb::IdentityStep,
    pub tags: BitSet,
    pub remove_tags: BitSet,
}

impl MapFuncGen for IdentityStep {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let params = QueryParams::from_pb(self.step.query_params)?;
        Ok(Box::new(IdentityFunc { params, tags: self.tags, remove_tags: self.remove_tags }))
    }
}
