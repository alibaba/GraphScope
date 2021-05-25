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

use crate::structure::filter::compare::{Compare, EqCmp, OrdCmp};
use crate::structure::filter::contains::Contains;
use crate::structure::filter::element::{ExpectValue, Reverse};
use crate::structure::filter::Predicate;
use crate::structure::{
    get_tlv_type, with_tlv, BiPredicate, Details, DynDetails, Element, TlvType,
};
use dyn_type::Object;
use std::collections::HashSet;

pub struct HasProperty {
    pub key: String,
    pub cmp: Compare,
    pub expect: ExpectValue<Object>,
}

impl<E: Element> Predicate<E> for HasProperty {
    fn test(&self, entry: &E) -> Option<bool> {
        let details: &DynDetails = entry.details();
        if let Some(left) = details.get_property(self.key.as_str()) {
            match self.expect {
                ExpectValue::Local(ref v) => self.cmp.test(&left, &v.as_borrow()),
                ExpectValue::TLV => match get_tlv_type() {
                    TlvType::LeftValue => {
                        with_tlv(|obj| self.cmp.test(&obj.as_borrow(), &left).unwrap_or(false))
                    }
                    TlvType::RightValue => {
                        with_tlv(|obj| self.cmp.test(&left, &obj.as_borrow()).unwrap_or(false))
                    }
                },
            }
        } else {
            None
        }
    }
}

impl HasProperty {
    pub fn eq(key: String, expect: Option<Object>) -> Self {
        HasProperty { key, cmp: Compare::Eq(EqCmp::Eq), expect: expect.into() }
    }

    pub fn lt(key: String, expect: Option<Object>) -> Self {
        HasProperty { key, cmp: Compare::Ord(OrdCmp::Less), expect: expect.into() }
    }

    pub fn le(key: String, expect: Option<Object>) -> Self {
        HasProperty { key, cmp: Compare::Ord(OrdCmp::LessEq), expect: expect.into() }
    }

    pub fn gt(key: String, expect: Option<Object>) -> Self {
        HasProperty { key, cmp: Compare::Ord(OrdCmp::Greater), expect: expect.into() }
    }

    pub fn ge(key: String, expect: Option<Object>) -> Self {
        HasProperty { key, cmp: Compare::Ord(OrdCmp::GreaterEq), expect: expect.into() }
    }
}

impl Reverse for HasProperty {
    fn reverse(&mut self) {
        self.cmp.reverse();
    }
}

pub struct ContainsProperty {
    pub key: String,
    pub cmp: Contains,
    pub expect: HashSet<Object>,
}

impl ContainsProperty {
    pub fn with_in(key: String, expect: HashSet<Object>) -> Self {
        ContainsProperty { key, cmp: Contains::Within, expect }
    }

    pub fn with_out(key: String, expect: HashSet<Object>) -> Self {
        ContainsProperty { key, cmp: Contains::Without, expect }
    }
}

impl<E: Element> Predicate<E> for ContainsProperty {
    fn test(&self, entry: &E) -> Option<bool> {
        let details: &DynDetails = entry.details();
        if let Some(left) = details.get_property(self.key.as_str()) {
            self.cmp.test(&left.try_to_owned().unwrap(), &self.expect)
        } else {
            None
        }
    }
}

impl Reverse for ContainsProperty {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}
