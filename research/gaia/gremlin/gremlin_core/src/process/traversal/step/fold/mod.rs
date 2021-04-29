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
use crate::process::traversal::step::fold::fold::FoldFunc;
use crate::process::traversal::traverser::Traverser;
use crate::DynResult;
use pegasus_server::factory::FoldFunction;

mod fold;

#[enum_dispatch]
pub trait FoldFunctionGen {
    fn gen_fold(self) -> DynResult<Box<dyn FoldFunction<Traverser>>>;
}

impl FoldFunctionGen for pb::GremlinStep {
    fn gen_fold(self) -> DynResult<Box<dyn FoldFunction<Traverser>>> {
        // TODO: should define a unfold step pb with compiler, which provides the choices of different unfold types
        Ok(Box::new(FoldFunc {}))
    }
}
