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

use crate::DynResult;
use pegasus_server::pb as server_pb;
use pegasus_server::pb::AccumKind;

mod fold;
use crate::process::traversal::step::accum::{Count, ToList};
pub use fold::TraverserAccumulator;

#[enum_dispatch]
pub trait AccumFactoryGen {
    fn gen_accum(self) -> DynResult<TraverserAccumulator>;
}

impl AccumFactoryGen for server_pb::AccumKind {
    fn gen_accum(self) -> DynResult<TraverserAccumulator> {
        match self {
            AccumKind::Cnt => {
                Ok(TraverserAccumulator::ToCount(Count { value: 0, _ph: Default::default() }))
            }
            AccumKind::ToList => Ok(TraverserAccumulator::ToList(ToList { inner: vec![] })),
            _ => {
                todo!()
            }
        }
    }
}
