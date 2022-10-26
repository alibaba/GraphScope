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
use serde::{Deserialize, Serialize};

pub type PatternId = usize;
pub type PatternLabelId = ir_common::LabelId;
pub type DynIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub mod error;
pub mod extend_step;
pub mod pattern;
pub mod pattern_meta;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PatternDirection {
    Out = 0,
    In,
}

impl Into<u8> for PatternDirection {
    fn into(self) -> u8 {
        match self {
            PatternDirection::Out => 0,
            PatternDirection::In => 1,
        }
    }
}

impl PatternDirection {
    pub fn reverse(&self) -> PatternDirection {
        match self {
            PatternDirection::Out => PatternDirection::In,
            PatternDirection::In => PatternDirection::Out,
        }
    }
}

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
