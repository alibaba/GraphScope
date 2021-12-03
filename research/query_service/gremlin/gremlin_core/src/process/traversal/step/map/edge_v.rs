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
use crate::process::traversal::step::map::MapFuncGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{DefaultDetails, EndPointOpt, Label, QueryParams, Vertex, VertexOrEdge};
use crate::{str_to_dyn_error, DynResult, FromPb};
use bit_set::BitSet;
use graph_store::common::INVALID_LABEL_ID;
use pegasus::api::function::{FnResult, MapFunction};

struct EdgeVertexFunc {
    tags: BitSet,
    remove_tags: BitSet,
    params: QueryParams<Vertex>,
    get_src: bool,
}

impl MapFunction<Traverser, Traverser> for EdgeVertexFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if let Some(elem) = input.get_element() {
            match elem.get() {
                VertexOrEdge::E(e) => {
                    let id = if self.get_src { e.src_id } else { e.dst_id };
                    let graph = crate::get_graph().ok_or(str_to_dyn_error("Graph is None"))?;
                    let mut r = graph.get_vertex(&[id], &self.params)?;
                    // get vertex from store, or generate a local vertex with id only.
                    // TODO(bingqing): check with compiler if it will optimize when store supports get property locally;
                    let v = if let Some(v) = r.next() {
                        v
                    } else {
                        let label =
                            if self.get_src { e.get_src_label() } else { e.get_dst_label() }
                                .map(|l| l.clone());
                        Vertex::new(
                            id,
                            label.clone(),
                            DefaultDetails::new(id, label.unwrap_or(Label::Id(INVALID_LABEL_ID))),
                        )
                    };
                    input.split(v, &self.tags);
                    input.remove_tags(&self.remove_tags);
                    Ok(input)
                }
                _ => Err(str_to_dyn_error("Should not call `EdgeVertexStep` on a vertex")),
            }
        } else {
            Err(str_to_dyn_error("invalid input for `EdgeVertexStep`"))
        }
    }
}

pub struct EdgeVertexStep {
    pub step: pb::EdgeVertexStep,
    pub tags: BitSet,
    pub remove_tags: BitSet,
}

impl MapFuncGen for EdgeVertexStep {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let step = self.step;
        let opt_pb = unsafe { std::mem::transmute(step.endpoint_opt) };
        let opt = EndPointOpt::from_pb(opt_pb)?;
        let params = QueryParams::from_pb(step.query_params)?;
        match opt {
            EndPointOpt::Out => Ok(Box::new(EdgeVertexFunc {
                tags: self.tags,
                remove_tags: self.remove_tags,
                params,
                get_src: true,
            }) as Box<dyn MapFunction<Traverser, Traverser>>),
            EndPointOpt::In => Ok(Box::new(EdgeVertexFunc {
                tags: self.tags,
                remove_tags: self.remove_tags,
                params,
                get_src: false,
            }) as Box<dyn MapFunction<Traverser, Traverser>>),
            EndPointOpt::Other => Err(str_to_dyn_error("`otherV()` has not been supported")),
        }
    }
}
