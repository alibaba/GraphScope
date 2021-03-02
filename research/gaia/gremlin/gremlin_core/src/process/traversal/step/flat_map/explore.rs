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

use crate::process::traversal::step::flat_map::FlatMapGen;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::{Traverser, TraverserSplitIter};
use crate::structure::{
    Direction, Edge, Element, GraphElement, QueryParams, Statement, Tag, Vertex, ID,
};
use crate::{DynIter, DynResult};
use pegasus::api::function::FlatMapFunction;
use std::collections::HashSet;
use std::sync::Arc;

/// out(), in(), both()
pub struct VertexStep {
    pub symbol: StepSymbol,
    pub params: QueryParams<Vertex>,
    direction: Direction,
    as_labels: Vec<String>,
}

impl VertexStep {
    pub fn new(direction: Direction) -> Self {
        let symbol = match direction {
            Direction::Out => StepSymbol::Out,
            Direction::In => StepSymbol::In,
            Direction::Both => StepSymbol::Both,
        };

        VertexStep { symbol, params: QueryParams::new(), direction, as_labels: vec![] }
    }
}

impl Step for VertexStep {
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

/// outE(), inE(), bothE();
pub struct EdgeStep {
    pub symbol: StepSymbol,
    pub params: QueryParams<Edge>,
    pub direction: Direction,
    as_labels: Vec<String>,
}

impl EdgeStep {
    pub fn new(direction: Direction) -> Self {
        let symbol = match direction {
            Direction::Out => StepSymbol::OutE,
            Direction::In => StepSymbol::InE,
            Direction::Both => StepSymbol::BothE,
        };

        EdgeStep { symbol, params: QueryParams::new(), direction, as_labels: vec![] }
    }
}

impl Step for EdgeStep {
    fn get_symbol(&self) -> StepSymbol {
        self.symbol
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[Tag] {
        self.as_labels.as_slice()
    }
}

pub struct FlatMapStatement<E: Into<GraphElement>> {
    labels: Arc<HashSet<String>>,
    stmt: Box<dyn Statement<ID, E>>,
}

impl<E: Into<GraphElement> + 'static> FlatMapFunction<Traverser, Traverser>
    for FlatMapStatement<E>
{
    type Target = DynIter<Traverser>;

    fn exec(&self, input: Traverser) -> DynResult<DynIter<Traverser>> {
        if let Some(e) = input.get_element() {
            let id = e.id();
            let iter = self.stmt.exec(id)?;
            Ok(Box::new(TraverserSplitIter::new(input, &self.labels, iter)))
        } else {
            panic!("invalid input for vertex/edge step;")
        }
    }
}

impl FlatMapGen for VertexStep {
    fn gen(&self) -> Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>> {
        let graph = crate::get_graph().expect("failure");
        let labels = Arc::new(self.get_tags());
        let stmt = graph.prepare_explore_vertex(self.direction, &self.params).expect("failure");
        Box::new(FlatMapStatement { labels, stmt })
    }
}

impl FlatMapGen for EdgeStep {
    fn gen(&self) -> Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>> {
        let graph = crate::get_graph().expect("failure");
        let labels = Arc::new(self.get_tags());
        let stmt = graph.prepare_explore_edge(self.direction, &self.params).expect("failure");
        Box::new(FlatMapStatement { labels, stmt })
    }
}
