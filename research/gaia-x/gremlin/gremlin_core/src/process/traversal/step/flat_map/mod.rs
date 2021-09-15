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
use crate::process::traversal::step::flat_map::explore::VertexStep;
use crate::process::traversal::step::flat_map::values::PropertiesStep;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::PropKey;
use crate::{str_to_dyn_error, DynResult, FromPb};
use pegasus::api::function::{DynIter, FlatMapFunction};

mod explore;
mod unfold;
mod values;

#[enum_dispatch]
pub trait FlatMapFuncGen {
    fn gen_flat_map(
        self,
    ) -> DynResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>;
}

impl FlatMapFuncGen for pb::GremlinStep {
    fn gen_flat_map(
        self,
    ) -> DynResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>
    {
        let tags = self.get_tags();

        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::VertexStep(vertex_step) => {
                    let vertex_step = VertexStep { step: vertex_step, tags };
                    vertex_step.gen_flat_map()
                }
                pb::gremlin_step::Step::PropertiesStep(properties_step) => {
                    let mut prop_keys= vec![] ;
                    if let Some(prop_step_keys) = properties_step.prop_keys {
                        for prop_key in prop_step_keys.prop_keys {
                            prop_keys.push(PropKey::from_pb(prop_key)?);
                        }
                    };
                    Ok(Box::new(PropertiesStep { prop_keys, tags }))
                }
                pb::gremlin_step::Step::UnfoldStep(unfold_step) => Ok(Box::new(unfold_step)),
                _ => Err(str_to_dyn_error("pb GremlinStep is not a FlatMap Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
