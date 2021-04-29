use crate::generated::gremlin as pb;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};
use pegasus_server::factory::GroupFunction;

mod group_by;

#[enum_dispatch]
pub trait GroupFunctionGen {
    fn gen_group(self) -> DynResult<Box<dyn GroupFunction<Traverser>>>;
}

impl GroupFunctionGen for pb::GremlinStep {
    fn gen_group(self) -> DynResult<Box<dyn GroupFunction<Traverser>>> {
        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::GroupByStep(g) => g.gen_group(),
                _ => Err(str_to_dyn_error("pb GremlinStep is not a Compare Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
