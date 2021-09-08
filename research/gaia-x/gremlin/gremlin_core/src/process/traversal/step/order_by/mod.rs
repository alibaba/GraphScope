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
use crate::process::traversal::step::functions::CompareFunction;
pub use crate::process::traversal::step::order_by::order::Order;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};

mod order;

#[enum_dispatch]
pub trait CompareFunctionGen {
    fn gen_cmp(self) -> DynResult<Box<dyn CompareFunction<Traverser>>>;
}

impl CompareFunctionGen for pb::GremlinStep {
    fn gen_cmp(self) -> DynResult<Box<dyn CompareFunction<Traverser>>> {
        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::OrderByStep(order_step) => order_step.gen_cmp(),
                _ => Err(str_to_dyn_error("pb GremlinStep is not a Compare Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
