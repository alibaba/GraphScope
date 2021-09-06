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

use crate::accum::{AccumFactory, Accumulator};
use crate::process::traversal::step::fold::fold::TraverserAccumFactory;
use crate::process::traversal::traverser::Traverser;
use crate::DynResult;
use pegasus_server::pb as server_pb;

mod fold;

#[enum_dispatch]
pub trait AccumFactoryGen {
    fn gen_accum(
        self,
    ) -> DynResult<
        Box<
            dyn AccumFactory<
                Traverser,
                Traverser,
                Target = Box<dyn Accumulator<Traverser, Traverser>>,
            >,
        >,
    >;
}

impl AccumFactoryGen for server_pb::AccumKind {
    fn gen_accum(
        self,
    ) -> DynResult<
        Box<
            dyn AccumFactory<
                Traverser,
                Traverser,
                Target = Box<dyn Accumulator<Traverser, Traverser>>,
            >,
        >,
    > {
        Ok(Box::new(TraverserAccumFactory { accum_kind: self.clone() }))
    }
}

// TODO(bingqing): this is just for compiling
impl Clone for Box<dyn Accumulator<Traverser, Traverser>> {
    fn clone(&self) -> Self {
        todo!()
    }
}
