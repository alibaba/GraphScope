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
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::{pb_value_to_object, ParseError};
use crate::structure::filter::compare::{Compare, EqCmp, OrdCmp};
use crate::structure::filter::BiPredicate;
use crate::structure::filter::Predicate;
use crate::structure::{get_tlv_type, with_tlv, ExpectValue, Reverse, TlvType};
use crate::FromPb;
use dyn_type::Object;

pub struct ValueFilter {
    pub cmp: Compare,
    pub expect: ExpectValue<Object>,
}

impl Reverse for ValueFilter {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}

impl Predicate<Traverser> for ValueFilter {
    fn test(&self, entry: &Traverser) -> Option<bool> {
        if let Some(left) = entry.get_object() {
            match self.expect {
                ExpectValue::Local(ref v) => self.cmp.test(&left, &v),
                ExpectValue::TLV => match get_tlv_type() {
                    TlvType::LeftValue => {
                        with_tlv(|obj| self.cmp.test(&obj, &left).unwrap_or(false))
                    }
                    TlvType::RightValue => {
                        with_tlv(|obj| self.cmp.test(&left, &obj).unwrap_or(false))
                    }
                },
            }
        } else {
            None
        }
    }
}

impl ValueFilter {
    pub fn eq(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Eq(EqCmp::Eq), expect: expect.into() }
    }

    pub fn neq(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Eq(EqCmp::NotEq), expect: expect.into() }
    }

    pub fn lt(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Ord(OrdCmp::Less), expect: expect.into() }
    }

    pub fn le(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Ord(OrdCmp::LessEq), expect: expect.into() }
    }

    pub fn gt(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Ord(OrdCmp::Greater), expect: expect.into() }
    }

    pub fn ge(expect: Option<Object>) -> Self {
        ValueFilter { cmp: Compare::Ord(OrdCmp::GreaterEq), expect: expect.into() }
    }
}

impl FromPb<pb::FilterValueExp> for ValueFilter {
    fn from_pb(filter: pb::FilterValueExp) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        let cmp_pb: pb::Compare = { unsafe { std::mem::transmute(filter.cmp) } };
        let right_value = pb_value_to_object(&filter.right.ok_or("right value is not set")?);
        let value_filter = match cmp_pb {
            pb::Compare::Eq => ValueFilter::eq(right_value),
            pb::Compare::Ne => ValueFilter::neq(right_value),
            pb::Compare::Lt => ValueFilter::lt(right_value),
            pb::Compare::Le => ValueFilter::le(right_value),
            pb::Compare::Gt => ValueFilter::gt(right_value),
            pb::Compare::Ge => ValueFilter::ge(right_value),
            pb::Compare::Within => return Err("Have not support Within in ValueFilter yet".into()),
            pb::Compare::Without => {
                return Err("Have not support Without in ValueFilter yet".into())
            }
        };
        Ok(value_filter)
    }
}
