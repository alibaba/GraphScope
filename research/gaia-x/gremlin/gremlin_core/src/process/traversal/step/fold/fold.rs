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

use crate::accum::{AccumFactory, Accumulator, CountAccum, ToListAccum};
use crate::process::traversal::traverser::Traverser;
use pegasus::Data;
use pegasus_server::pb::AccumKind;
use std::fmt::{Debug, Formatter};

//trait AccumulatorClone: Accumulator<Traverser, Traverser> + Clone {}

pub struct TraverserAccumFactory {
    pub accum_kind: AccumKind,
}

impl AccumFactory<Traverser, Traverser> for TraverserAccumFactory {
    type Target = Box<dyn Accumulator<Traverser, Traverser>>;

    fn create(&self) -> Self::Target {
        match self.accum_kind {
            AccumKind::ToList => {
                info!("create tolist accum");
                let traverser_accum = TraverserAccum::new(ToListAccum::new());
                Box::new(traverser_accum) as Box<dyn Accumulator<Traverser, Traverser>>
            }
            AccumKind::Cnt => {
                info!("create count accum");
                let traverser_accum = TraverserAccum::new(CountAccum::new());
                Box::new(traverser_accum) as Box<dyn Accumulator<Traverser, Traverser>>
            }
            _ => {
                todo!()
            }
        }
    }
}

pub struct TraverserAccum<O, F: AccumFactory<Traverser, O>> {
    inner: F::Target,
}

impl<O, F: AccumFactory<Traverser, O>> TraverserAccum<O, F> {
    pub fn new(factory: F) -> Self {
        let inner = factory.create();
        TraverserAccum { inner }
    }
}

impl<O, F: AccumFactory<Traverser, O>> Debug for TraverserAccum<O, F> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        //  write!(f, "{:?}", self.inner)
        todo!()
    }
}

impl<O: Data + Eq, F: AccumFactory<Traverser, O>> Accumulator<Traverser, Traverser>
    for TraverserAccum<O, F>
{
    fn accum(&mut self, next: Traverser) -> Result<(), std::io::Error> {
        info!("accum: {:?}", next);
        self.inner.accum(next)
    }

    fn finalize(&mut self) -> Traverser {
        Traverser::with(self.inner.finalize())
    }
}
