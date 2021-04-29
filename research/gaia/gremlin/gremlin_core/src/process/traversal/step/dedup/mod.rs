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
use pegasus_common::collections::{CollectionFactory, Set};

mod dedup;

#[enum_dispatch]
pub trait CollectionFactoryGen {
    fn gen_collection(
        self,
    ) -> DynResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>>;
}

impl CollectionFactoryGen for pb::GremlinStep {
    fn gen_collection(
        self,
    ) -> DynResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>> {
        if let Some(pb::gremlin_step::Step::DedupStep(dedup)) = self.step {
            Ok(Box::new(dedup))
        } else {
            Err(str_to_dyn_error("pb GremlinStep is not a Dedup Step"))
        }
    }
}
