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
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::{QueryParams, Vertex, ID};
use crossbeam_queue::ArrayQueue;
use pegasus_common::downcast::*;

/// V(),
pub struct GraphVertexStep {
    pub symbol: StepSymbol,
    pub params: QueryParams<Vertex>,
    src: Option<ArrayQueue<Vec<ID>>>,
    as_labels: Vec<String>,
    requirement: Requirement,
}

impl_as_any!(GraphVertexStep);

impl GraphVertexStep {
    pub fn new(req: Requirement) -> Self {
        GraphVertexStep {
            symbol: StepSymbol::V,
            src: None,
            as_labels: vec![],
            requirement: req,
            params: QueryParams::new(),
        }
    }

    pub fn set_src(&mut self, ids: Vec<ID>, workers: u32) {
        let mut partition = Vec::with_capacity(workers as usize);
        for _ in 0..workers {
            partition.push(vec![]);
        }
        let workers = workers as u128;
        for id in ids {
            let idx = (id % workers) as usize;
            partition[idx].push(id);
        }
        let src = ArrayQueue::new(workers as usize);
        for p in partition {
            src.push(p).expect("unreachable");
        }
        self.src = Some(src);
    }
}

impl Step for GraphVertexStep {
    fn get_symbol(&self) -> StepSymbol {
        self.symbol
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        self.as_labels.as_slice()
    }
}

impl GraphVertexStep {
    pub fn gen_source(&self, worker_index: u32) -> Box<dyn Iterator<Item = Traverser> + Send> {
        let source = if let Some(ref seeds) = self.src {
            let src = seeds.pop().unwrap_or_else(|_| vec![]);
            if !src.is_empty() {
                let graph = crate::get_graph().unwrap();
                graph.get_vertex(&src, &self.params).expect("xxx")
            } else {
                // return an emtpy iterator;
                Box::new(std::iter::empty())
            }
        } else {
            if worker_index == 0 {
                let graph = crate::get_graph().unwrap();
                graph.scan_vertex(&self.params).expect("xxx")
            } else {
                // return an emtpy iterator;
                Box::new(std::iter::empty())
            }
        };

        if self.requirement.contains(Requirement::PATH)
            || self.requirement.contains(Requirement::LABELEDPATH)
        {
            let labels = self.get_tags();
            Box::new(source.map(move |v| Traverser::with_path(v, &labels)))
        } else {
            Box::new(source.map(|v| Traverser::new(v)))
        }
    }
}
