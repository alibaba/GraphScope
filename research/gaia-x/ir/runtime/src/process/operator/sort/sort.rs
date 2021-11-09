//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use crate::error::FnGenResult;
use crate::process::functions::CompareFunction;
use crate::process::operator::sort::CompareFunctionGen;
use crate::process::operator::TagKey;
use crate::process::record::Record;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::order_by::ordering_pair::Order;
use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};

struct RecordCompare {
    tag_key_order: Vec<(TagKey, Order)>,
}

impl CompareFunction<Record> for RecordCompare {
    fn compare(&self, left: &Record, right: &Record) -> Ordering {
        let mut result = Ordering::Equal;
        for (tag_key, order) in self.tag_key_order.iter() {
            let left_obj = tag_key.get_obj(left).ok();
            let right_obj = tag_key.get_obj(right).ok();
            let ordering = left_obj.partial_cmp(&right_obj);
            if let Some(ordering) = ordering {
                if Ordering::Equal != ordering {
                    result = {
                        match order {
                            Order::Desc => ordering.reverse(),
                            _ => ordering,
                        }
                    };
                    break;
                }
            }
        }
        result
    }
}

impl CompareFunctionGen for algebra_pb::OrderBy {
    fn gen_cmp(self) -> FnGenResult<Box<dyn CompareFunction<Record>>> {
        let record_compare = RecordCompare::try_from(self)?;
        Ok(Box::new(record_compare))
    }
}

impl TryFrom<algebra_pb::OrderBy> for RecordCompare {
    type Error = ParsePbError;

    fn try_from(order_pb: algebra_pb::OrderBy) -> Result<Self, Self::Error> {
        let mut tag_key_order = Vec::with_capacity(order_pb.pairs.len());
        for order_pair in order_pb.pairs {
            let key = order_pair
                .key
                .ok_or(ParsePbError::from("key is empty in order"))?
                .try_into()?;
            let order: Order = unsafe { ::std::mem::transmute(order_pair.order) };
            tag_key_order.push((key, order));
        }
        Ok(RecordCompare { tag_key_order })
    }
}
