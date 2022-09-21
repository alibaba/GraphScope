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
use std::convert::{TryFrom, TryInto};
use std::io;
use std::sync::Arc;

use dyn_type::Object;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use ir_common::{LabelId, NameOrId, OneOrMany};
use pegasus::codec::{ReadExt, WriteExt};

use crate::utils::expr::eval_pred::PEvaluator;

pub mod element;
pub type ID = u64;

pub fn read_id<R: ReadExt>(reader: &mut R) -> io::Result<ID> {
    reader.read_u64()
}

pub fn write_id<W: WriteExt>(writer: &mut W, id: ID) -> io::Result<()> {
    writer.write_u64(id)
}

/// The number of bits in an `ID`
pub const ID_BITS: usize = std::mem::size_of::<ID>() * 8;

/// Primary key in storage, including single column pk and multi column pks.
pub type PKV = OneOrMany<(NameOrId, Object)>;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Direction {
    Out = 0,
    In = 1,
    Both = 2,
}

impl From<algebra_pb::edge_expand::Direction> for Direction {
    fn from(direction: algebra_pb::edge_expand::Direction) -> Self
    where
        Self: Sized,
    {
        match direction {
            algebra_pb::edge_expand::Direction::Out => Direction::Out,
            algebra_pb::edge_expand::Direction::In => Direction::In,
            algebra_pb::edge_expand::Direction::Both => Direction::Both,
        }
    }
}

#[derive(Default, Debug)]
pub struct QueryParams {
    pub labels: Vec<LabelId>,
    pub limit: Option<usize>,
    pub columns: Option<Vec<NameOrId>>,
    pub partitions: Option<Vec<u64>>,
    pub filter: Option<Arc<PEvaluator>>,
    pub sample_ratio: Option<f64>,
    pub extra_params: Option<HashMap<String, String>>,
}

impl TryFrom<Option<algebra_pb::QueryParams>> for QueryParams {
    type Error = ParsePbError;

    fn try_from(query_params_pb: Option<algebra_pb::QueryParams>) -> Result<Self, Self::Error> {
        query_params_pb.map_or(Ok(QueryParams::default()), |query_params_pb| {
            let query_param = QueryParams::default()
                .with_labels(query_params_pb.tables)?
                .with_filter(query_params_pb.predicate)?
                .with_limit(query_params_pb.limit)?
                .with_sample_ratio(query_params_pb.sample_ratio)?
                .with_extra_params(query_params_pb.extra)?;
            if query_params_pb.is_all_columns {
                query_param.with_all_columns()
            } else {
                query_param.with_required_columns(query_params_pb.columns)
            }
        })
    }
}

impl QueryParams {
    fn with_labels(mut self, labels_pb: Vec<common_pb::NameOrId>) -> Result<Self, ParsePbError> {
        self.labels = labels_pb
            .into_iter()
            .map(|label| label.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self)
    }

    fn with_filter(mut self, filter_pb: Option<common_pb::Expression>) -> Result<Self, ParsePbError> {
        if let Some(filter_pb) = filter_pb {
            self.filter = Some(Arc::new(filter_pb.try_into()?));
        }
        Ok(self)
    }

    fn with_limit(mut self, limit_pb: Option<algebra_pb::Range>) -> Result<Self, ParsePbError> {
        if let Some(range) = limit_pb {
            // According to the semantics in gremlin, limit(-1) means no limit.
            if range.upper > 0 {
                self.limit = Some((range.upper - 1) as usize);
            } else if range.upper < 0 {
                Err(ParsePbError::from("Not a legal range"))?
            }
        }
        Ok(self)
    }

    fn with_sample_ratio(mut self, sample_ratio: f64) -> Result<Self, ParsePbError> {
        if sample_ratio <= 0.0 || sample_ratio > 1.0 {
            Err(ParsePbError::ParseError(format!("sample ratio must be between 0 and 1, {}", sample_ratio)))
        } else if sample_ratio == 1.0 {
            Ok(self)
        } else {
            self.sample_ratio = Some(sample_ratio);
            Ok(self)
        }
    }

    fn with_all_columns(mut self) -> Result<Self, ParsePbError> {
        self.columns = Some(vec![]);
        Ok(self)
    }

    // props specify the properties we query for, e.g.,
    // Some(vec![prop1, prop2]) indicates we need prop1 and prop2,
    // Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property,
    fn with_required_columns(
        mut self, required_columns_pb: Vec<common_pb::NameOrId>,
    ) -> Result<Self, ParsePbError> {
        if required_columns_pb.is_empty() {
            self.columns = None;
        } else {
            self.columns = Some(
                required_columns_pb
                    .into_iter()
                    .map(|prop_key| prop_key.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(self)
    }

    // Extra query params for different storages
    fn with_extra_params(mut self, extra_params_pb: HashMap<String, String>) -> Result<Self, ParsePbError> {
        if !extra_params_pb.is_empty() {
            self.extra_params = Some(extra_params_pb);
        }
        Ok(self)
    }

    pub fn is_queryable(&self) -> bool {
        !(self.labels.is_empty()
            && self.filter.is_none()
            && self.limit.is_none()
            && self.partitions.is_none()
            && self.columns.is_none())
    }

    pub fn get_extra_param(&self, key: &str) -> Option<&String> {
        if let Some(ref extra_params) = self.extra_params {
            extra_params.get(key)
        } else {
            None
        }
    }
}
