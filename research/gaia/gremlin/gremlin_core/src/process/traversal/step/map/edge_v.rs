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
use crate::process::traversal::step::{RemoveLabel, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{EndPointOpt, QueryParams, Tag, Vertex, VertexOrEdge};
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct EdgeVertexStep {
    symbol: StepSymbol,
    pub params: QueryParams<Vertex>,
    as_labels: Vec<Tag>,
    remove_labels: Vec<Tag>,
}

impl EdgeVertexStep {
    pub fn new(opt: EndPointOpt) -> Self {
        let symbol = match opt {
            EndPointOpt::Out => StepSymbol::OutV,
            EndPointOpt::In => StepSymbol::InV,
            EndPointOpt::Other => StepSymbol::OtherV,
        };
        EdgeVertexStep {
            symbol,
            params: QueryParams::new(),
            as_labels: vec![],
            remove_labels: vec![],
        }
    }
}

impl Step for EdgeVertexStep {
    fn get_symbol(&self) -> StepSymbol {
        self.symbol
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[Tag] {
        &self.as_labels
    }
}

impl RemoveLabel for EdgeVertexStep {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_labels.push(label);
    }

    fn remove_tags(&self) -> &[Tag] {
        self.remove_labels.as_slice()
    }
}

struct EdgeVertexFunc {
    labels: BitSet,
    remove_labels: BitSet,
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
                    if let Some(v) = r.next() {
                        input.split(v, &self.labels);
                        input.remove_labels(&self.remove_labels);
                        return Ok(input);
                    } else {
                        return Err(str_to_dyn_error(&format!(
                            "vertex with id {} not found",
                            e.src_id
                        )));
                    }
                }
                _ => (),
            }
        }

        Err(str_to_dyn_error("invalid input for EdgeVertexStep"))
    }
}

impl MapFuncGen for EdgeVertexStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let labels = self.get_tags();
        let remove_labels = self.get_remove_tags();
        let params = self.params.clone();
        match self.symbol {
            StepSymbol::OutV => {
                Ok(Box::new(EdgeVertexFunc { labels, remove_labels, params, get_src: true }))
            }
            StepSymbol::InV => {
                Ok(Box::new(EdgeVertexFunc { labels, remove_labels, params, get_src: false }))
            }
            StepSymbol::OtherV => Err(str_to_dyn_error("`otherV()` has not been supported")),
            _ => Err(str_to_dyn_error("Invalid symbol in `EdgeVertexStep`")),
        }
    }
}
