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

use std::collections::HashMap;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use std::cmp::Ordering;

pub type PatternId = usize;
pub type PatternLabelId = ir_common::LabelId;
pub type DynIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub mod error;
pub mod extend_step;
pub mod pattern;

pub type PatternDirection = pb::edge_expand::Direction;

pub(crate) fn query_params(
    tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    predicate: Option<common_pb::Expression>,
) -> pb::QueryParams {
    pb::QueryParams {
        tables,
        columns,
        is_all_columns: false,
        limit: None,
        predicate,
        sample_ratio: 1.0,
        extra: HashMap::new(),
    }
}

pub trait PatternOrderTrait<D> {
    fn compare(&self, left: &D, right: &D) -> Ordering;
}

pub trait PatternWeightTrait<W: PartialOrd> {
    fn get_vertex_weight(&self, vid: PatternId) -> W;
    fn get_adjacencies_weight(&self, vid: PatternId) -> W;
}
