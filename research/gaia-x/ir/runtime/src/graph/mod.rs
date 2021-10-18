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

pub mod element;
pub mod graph;
pub mod partitioner;
pub mod property;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Direction {
    Out = 0,
    In = 1,
    Both = 2,
}

use ir_common::generated::algebra as algebra_pb;
impl From<algebra_pb::expand_base::Direction> for Direction {
    fn from(direction: algebra_pb::expand_base::Direction) -> Self
    where
        Self: Sized,
    {
        match direction {
            algebra_pb::expand_base::Direction::Out => Direction::Out,
            algebra_pb::expand_base::Direction::In => Direction::In,
            algebra_pb::expand_base::Direction::Both => Direction::Both,
        }
    }
}
