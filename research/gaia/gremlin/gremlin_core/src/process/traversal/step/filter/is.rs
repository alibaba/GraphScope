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
use crate::process::traversal::step::{FilterFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::TraverserFilterChain;
use crate::DynResult;
use pegasus::api::function::{FilterFunction, FnResult};
use std::sync::Arc;

// TODO: support nested filter
pub struct IsStep {
    filter: Arc<TraverserFilterChain>,
}

impl IsStep {
    pub fn new(has: TraverserFilterChain) -> Self {
        IsStep { filter: Arc::new(has) }
    }
}

impl Step for IsStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Is
    }
}

struct IsTraverser {
    filter: Arc<TraverserFilterChain>,
}

impl IsTraverser {
    pub fn new(filter: &Arc<TraverserFilterChain>) -> Self {
        IsTraverser { filter: filter.clone() }
    }
}

impl FilterFunction<Traverser> for IsTraverser {
    fn exec(&self, input: &Traverser) -> FnResult<bool> {
        if let Some(true) = self.filter.test(input) {
            Ok(true)
        } else {
            // TODO: `None` means can't compare, should it be different with compare false;
            Ok(false)
        }
    }
}

impl FilterFuncGen for IsStep {
    fn gen(&self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        Ok(Box::new(IsTraverser::new(&self.filter)))
    }
}
