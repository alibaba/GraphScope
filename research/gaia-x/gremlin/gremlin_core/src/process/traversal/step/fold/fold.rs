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

use crate::accum::{Accumulator, Count, ToList};
use crate::process::traversal::traverser::Traverser;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::fmt::Debug;
use std::io::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraverserAccumulator {
    ToCount(Count<Traverser>),
    ToList(ToList<Traverser>),
}

impl Encode for TraverserAccumulator {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl Decode for TraverserAccumulator {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        unimplemented!()
    }
}

impl Accumulator<Traverser, Traverser> for TraverserAccumulator {
    fn accum(&mut self, next: Traverser) -> Result<(), Error> {
        match self {
            TraverserAccumulator::ToCount(count) => count.accum(next),
            TraverserAccumulator::ToList(list) => list.accum(next),
        }
    }

    fn finalize(&mut self) -> Traverser {
        match self {
            TraverserAccumulator::ToCount(count) => Traverser::with(count.finalize()),
            TraverserAccumulator::ToList(list) => Traverser::with(list.finalize()),
        }
    }
}
