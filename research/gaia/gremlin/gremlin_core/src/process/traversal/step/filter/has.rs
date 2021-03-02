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

use crate::process::traversal::step::filter::FilterFuncGen;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Tag, TraverserFilterChain};
use pegasus::api::function::{FilterFunction, FnResult};
use std::collections::HashSet;
use std::sync::Arc;

pub struct HasStep {
    has: Arc<TraverserFilterChain>,
    tags: Vec<Tag>,
}

impl HasStep {
    pub fn new(has: TraverserFilterChain) -> Self {
        HasStep { has: Arc::new(has), tags: vec![] }
    }
}

impl Step for HasStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Has
    }

    fn add_tag(&mut self, label: String) {
        self.tags.push(label);
    }

    fn tags(&self) -> &[Tag] {
        &self.tags
    }
}

struct HasTraverser {
    filter: Arc<TraverserFilterChain>,
    labels: HashSet<Tag>,
}

impl HasTraverser {
    pub fn new(filter: &Arc<TraverserFilterChain>, labels: HashSet<Tag>) -> Self {
        HasTraverser { filter: filter.clone(), labels }
    }
}

impl FilterFunction<Traverser> for HasTraverser {
    fn exec(&self, input: &Traverser) -> FnResult<bool> {
        if let Some(true) = self.filter.test(input) {
            if !self.labels.is_empty() {
                input.add_labels(&self.labels);
            }
            Ok(true)
        } else {
            // TODO: `None` means can't compare, should it be different with compare false;
            Ok(false)
        }
    }
}

impl FilterFuncGen for HasStep {
    fn gen(&self) -> Box<dyn FilterFunction<Traverser>> {
        let labels = self.get_tags();
        Box::new(HasTraverser::new(&self.has, labels))
    }
}
