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
use crate::process::traversal::step::by_key::TagKey;
pub use crate::process::traversal::step::order_by::order::Order;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::Tag;
use crate::DynResult;
use crate::FromPb;
use bit_set::BitSet;
use pegasus::api::function::CompareFunction;
use pegasus_common::downcast::*;

mod order;

#[enum_dispatch]
pub trait CompareFunctionGen {
    fn gen(&self) -> DynResult<Box<dyn CompareFunction<Traverser>>>;
}

#[enum_dispatch(Step, CompareFunctionGen)]
pub enum OrderStep {
    OrderStep(order::OrderStep),
}

impl FromPb<pb::GremlinStep> for OrderStep {
    fn from_pb(step: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match step.step {
            Some(pb::gremlin_step::Step::OrderByStep(o)) => {
                let mut order_keys = vec![];
                for cmp in o.pairs {
                    let tag_key = if let Some(tag_key_pb) = cmp.key {
                        TagKey::from_pb(tag_key_pb)?
                    } else {
                        TagKey::default()
                    };
                    let order_type_pb = unsafe { std::mem::transmute(cmp.order) };
                    let order_type = Order::from_pb(order_type_pb)?;
                    order_keys.push((tag_key, order_type));
                }
                let order_step = order::OrderStep::new(order_keys);
                Ok(OrderStep::OrderStep(order_step))
            }
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl_as_any!(OrderStep);
