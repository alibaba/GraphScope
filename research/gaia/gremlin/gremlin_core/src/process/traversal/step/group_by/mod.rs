use crate::generated::gremlin as pb;
use crate::process::traversal::step::by_key::TagKey;
use crate::process::traversal::step::group_by::group_by::{AccumKind, GroupByStep, MapOpt};
use crate::process::traversal::step::order_by::Order;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::Tag;
use crate::DynResult;
use crate::FromPb;
use bit_set::BitSet;
use pegasus_common::downcast::*;
use pegasus_server::factory::GroupFunction;

mod group_by;

#[enum_dispatch]
pub trait GroupFunctionGen {
    fn gen(&self) -> DynResult<Box<dyn GroupFunction<Traverser>>>;
}

#[enum_dispatch(Step, GroupFunctionGen)]
pub enum GroupStep {
    GroupBy(group_by::GroupByStep),
}

impl FromPb<pb::GremlinStep> for GroupStep {
    fn from_pb(step: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match step.step {
            Some(pb::gremlin_step::Step::GroupByStep(g)) => {
                let tag_key = if let Some(tag_key_pb) = g.key {
                    TagKey::from_pb(tag_key_pb)?
                } else {
                    TagKey::default()
                };
                let accum_kind_pb = unsafe { std::mem::transmute(g.accum) };
                let accum_kind = AccumKind::from_pb(accum_kind_pb)?;
                let mut order_keys = vec![];
                for cmp in g.opt_order {
                    let order_tag_key = if let Some(tag_key_pb) = cmp.key {
                        TagKey::from_pb(tag_key_pb)?
                    } else {
                        TagKey::default()
                    };
                    let order_type_pb = unsafe { std::mem::transmute(cmp.order) };
                    let order_type = Order::from_pb(order_type_pb)?;
                    order_keys.push((order_tag_key, order_type));
                }
                let map_opt = if order_keys.is_empty() {
                    MapOpt { accum_kind, order_tag_key: None }
                } else {
                    MapOpt { accum_kind, order_tag_key: Some(order_keys) }
                };

                Ok(GroupStep::GroupBy(GroupByStep::new(tag_key, map_opt)))
            }
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl_as_any!(GroupStep);
