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
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{FilterFunction, FnResult};
use std::sync::Arc;

pub struct HasStep {
    has: Arc<TraverserFilterChain>,
    as_tags: Vec<Tag>,
}

impl HasStep {
    pub fn new(has: TraverserFilterChain) -> Self {
        HasStep { has: Arc::new(has), as_tags: vec![] }
    }
}

impl Step for HasStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Has
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_tags.push(label);
    }

    fn tags_as_slice(&self) -> &[Tag] {
        &self.as_tags
    }
}

struct HasTraverser {
    filter: Arc<TraverserFilterChain>,
    tags: BitSet,
}

impl HasTraverser {
    pub fn new(filter: &Arc<TraverserFilterChain>, labels: BitSet) -> Self {
        HasTraverser { filter: filter.clone(), tags: labels }
    }
}

impl FilterFunction<Traverser> for HasTraverser {
    fn exec(&self, input: &Traverser) -> FnResult<bool> {
        if let Some(true) = self.filter.test(input) {
            if !self.tags.is_empty() {
                Err(str_to_dyn_error("Now we don't support as() in filter step"))
            } else {
                Ok(true)
            }
        } else {
            // TODO: `None` means can't compare, should it be different with compare false;
            Ok(false)
        }
    }
}

impl FilterFuncGen for HasStep {
    fn gen(&self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let labels = self.get_tags();
        Ok(Box::new(HasTraverser::new(&self.has, labels)))
    }
}
