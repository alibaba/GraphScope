use crate::generated::gremlin as pb;
use crate::process::traversal::step::by_key::TagKey;
use crate::process::traversal::step::group_by::group_by::GroupByStep;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use pegasus::preclude::function::KeyFunction;
use pegasus_common::downcast::*;
use pegasus_server::factory::HashKey;
use std::collections::HashSet;

mod group_by;

#[enum_dispatch]
pub trait KeyFunctionGen {
    fn gen(&self) -> Box<dyn KeyFunction<Traverser, Target = HashKey<Traverser>>>;
}

#[enum_dispatch(Step, KeyFunctionGen)]
pub enum GroupStep {
    GroupBy(group_by::GroupByStep),
}

impl From<pb::GremlinStep> for GroupStep {
    fn from(step: pb::GremlinStep) -> Self {
        match step.step {
            Some(pb::gremlin_step::Step::GroupByStep(g)) => {
                if let Some(tag_key_pb) = g.key {
                    GroupStep::GroupBy(GroupByStep::new(tag_key_pb.into()))
                } else {
                    GroupStep::GroupBy(GroupByStep::new(TagKey::empty()))
                }
            }
            _ => unimplemented!(),
        }
    }
}

impl_as_any!(GroupStep);
