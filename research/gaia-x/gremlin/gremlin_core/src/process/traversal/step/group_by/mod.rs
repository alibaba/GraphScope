use crate::generated::gremlin as pb;
use crate::process::traversal::step::functions::KeyFunction;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};

mod group_by;

#[enum_dispatch]
pub trait KeyFunctionGen {
    fn gen_key(self) -> DynResult<Box<dyn KeyFunction<Traverser, Traverser, Traverser>>>;
}

impl KeyFunctionGen for pb::GremlinStep {
    fn gen_key(self) -> DynResult<Box<dyn KeyFunction<Traverser, Traverser, Traverser>>> {
        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::GroupByStep(g) => g.gen_key(),
                _ => Err(str_to_dyn_error("pb GremlinStep is not a Group Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
