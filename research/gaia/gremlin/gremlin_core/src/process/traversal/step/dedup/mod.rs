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
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::Tag;
use crate::{DynResult, FromPb};
use bit_set::BitSet;
use pegasus_common::collections::{CollectionFactory, Set};
use pegasus_common::downcast::*;

mod dedup;

#[enum_dispatch]
pub trait CollectionFactoryGen {
    fn gen(
        &self,
    ) -> DynResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>>;
}

#[enum_dispatch(CollectionFactoryGen, Step)]
pub enum DedupStep {
    HashDedup(dedup::HashDedupStep),
}

impl FromPb<pb::GremlinStep> for DedupStep {
    fn from_pb(step: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match step.step {
            Some(pb::gremlin_step::Step::DedupStep(_)) => {
                Ok(DedupStep::HashDedup(dedup::HashDedupStep {}))
            }
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl_as_any!(DedupStep);
