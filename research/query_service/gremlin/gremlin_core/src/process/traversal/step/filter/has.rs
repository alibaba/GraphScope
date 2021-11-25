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
use crate::process::traversal::step::filter::FilterFuncGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::pb_chain_to_filter;
use crate::structure::{
    without_tag, Filter, IsSimple, TraverserFilter, TraverserFilterChain, ValueFilter,
};
use crate::{str_to_dyn_error, DynResult, FromPb};
use pegasus::api::function::{FilterFunction, FnResult};
use std::sync::Arc;

struct HasTraverser {
    filter: Arc<TraverserFilterChain>,
}

impl HasTraverser {
    pub fn new(filter: Arc<TraverserFilterChain>) -> Self {
        HasTraverser { filter }
    }
}

impl FilterFunction<Traverser> for HasTraverser {
    fn test(&self, input: &Traverser) -> FnResult<bool> {
        if let Some(true) = self.filter.test(input) {
            Ok(true)
        } else {
            // TODO: `None` means can't compare, should it be different with compare false;
            Ok(false)
        }
    }
}

impl FilterFuncGen for pb::HasStep {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let mut filter = Filter::default();
        if let Some(predicates) = self.predicates {
            if let Some(test) = pb_chain_to_filter(&predicates)? {
                filter = without_tag(test)
            }
        }
        Ok(Box::new(HasTraverser::new(Arc::new(filter))))
    }
}

impl FilterFuncGen for pb::PathFilterStep {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let filter = if self.hint == 0 {
            TraverserFilter::HasCycle(IsSimple::Simple)
        } else {
            TraverserFilter::HasCycle(IsSimple::Cyclic)
        };
        Ok(Box::new(HasTraverser::new(Arc::new(Filter::with(filter)))))
    }
}

impl FilterFuncGen for pb::IsStep {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let value_filter_pb =
            self.single.ok_or(str_to_dyn_error("filter is not set in is step"))?;
        let value_filter = ValueFilter::from_pb(value_filter_pb)?;
        let traverser_filter = TraverserFilter::IsValue(value_filter);
        Ok(Box::new(HasTraverser::new(Arc::new(Filter::with(traverser_filter)))))
    }
}
