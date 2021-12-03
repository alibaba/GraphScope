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
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};
use pegasus::api::function::FilterFunction;

mod has;
mod where_predicate;

#[enum_dispatch]
pub trait FilterFuncGen {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Traverser>>>;
}

impl FilterFuncGen for pb::GremlinStep {
    fn gen_filter(self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::HasStep(has_step) => has_step.gen_filter(),
                pb::gremlin_step::Step::WhereStep(where_step) => where_step.gen_filter(),
                pb::gremlin_step::Step::PathFilterStep(path_filter_step) => {
                    path_filter_step.gen_filter()
                }
                pb::gremlin_step::Step::IsStep(is_step) => is_step.gen_filter(),
                _ => Err(str_to_dyn_error("pb GremlinStep is not a Filter Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
