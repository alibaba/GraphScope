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
use crate::process::traversal::step::order_by::order::Order;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use pegasus::api::function::CompareFunction;
use pegasus_common::downcast::*;
use std::collections::HashSet;

mod order;

#[enum_dispatch]
pub trait CompareFunctionGen {
    fn gen(&self) -> Box<dyn CompareFunction<Traverser>>;
}

#[enum_dispatch(Step, CompareFunctionGen)]
pub enum OrderStep {
    OrderStep(order::OrderStep),
}

impl From<pb::GremlinStep> for OrderStep {
    fn from(step: pb::GremlinStep) -> Self {
        match step.step {
            Some(pb::gremlin_step::Step::OrderByStep(o)) => {
                let mut order_keys = vec![];
                for cmp in o.pairs {
                    // TODO(bingqing): throw exception instead of panic(), expect() or unwrap()
                    let tag_key_pb = cmp.key.expect("Order tag/key is not provided");
                    let tag_key = tag_key_pb.into();
                    let order_type: Order = unsafe { std::mem::transmute(cmp.order) };
                    order_keys.push((tag_key, order_type));
                }
                let order_step = order::OrderStep::new(order_keys);
                OrderStep::OrderStep(order_step)
            }
            _ => unimplemented!(),
        }
    }
}

impl_as_any!(OrderStep);
