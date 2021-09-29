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

use crate::process::traversal::step::accum::{Accumulator, Count, ToList};
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
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            TraverserAccumulator::ToCount(count) => {
                writer.write_u8(0)?;
                count.write_to(writer)?;
            }
            TraverserAccumulator::ToList(list) => {
                writer.write_u8(1)?;
                list.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for TraverserAccumulator {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let cnt = Count::read_from(reader)?;
                Ok(TraverserAccumulator::ToCount(cnt))
            }
            1 => {
                let list = ToList::read_from(reader)?;
                Ok(TraverserAccumulator::ToList(list))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
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
            TraverserAccumulator::ToCount(count) => Traverser::Object(count.finalize().into()),
            TraverserAccumulator::ToList(list) => Traverser::with(list.finalize()),
        }
    }
}
