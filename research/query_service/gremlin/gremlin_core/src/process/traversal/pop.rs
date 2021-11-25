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
use crate::structure::codec::ParseError;
use crate::FromPb;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum Pop {
    First,
    Last,
    All,
    Mixed,
}

impl FromPb<pb::select_step::Pop> for Pop {
    fn from_pb(pop: pb::select_step::Pop) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match pop {
            pb::select_step::Pop::First => Ok(Pop::First),
            pb::select_step::Pop::Last => Ok(Pop::Last),
            pb::select_step::Pop::All => Ok(Pop::All),
            pb::select_step::Pop::Mixed => Ok(Pop::Mixed),
        }
    }
}
