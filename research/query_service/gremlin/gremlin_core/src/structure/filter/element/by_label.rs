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

use crate::structure::element::Label;
use crate::structure::filter::compare::EqCmp;
use crate::structure::filter::contains::Contains;
use crate::structure::filter::element::{ExpectValue, Reverse};
use crate::structure::filter::{BiPredicate, Predicate};
use crate::structure::Element;
use std::collections::HashSet;

pub struct HasLabel {
    pub cmp: EqCmp,
    pub expect: ExpectValue<Label>,
}

impl<E: Element> Predicate<E> for HasLabel {
    fn test(&self, entry: &E) -> Option<bool> {
        self.expect.test(&self.cmp, entry.label())
    }
}

impl HasLabel {
    pub fn eq(expect: Option<Label>) -> Self {
        if let Some(label) = expect {
            HasLabel { cmp: EqCmp::Eq, expect: ExpectValue::Local(label) }
        } else {
            HasLabel { cmp: EqCmp::Eq, expect: ExpectValue::TLV }
        }
    }
}

impl Reverse for HasLabel {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}

pub struct ContainsLabel {
    pub cmp: Contains,
    pub expect: HashSet<Label>,
}

impl<E: Element> Predicate<E> for ContainsLabel {
    fn test(&self, entry: &E) -> Option<bool> {
        self.cmp.test(entry.label(), &self.expect)
    }
}

impl ContainsLabel {
    pub fn with_in(expect: HashSet<Label>) -> Self {
        ContainsLabel { cmp: Contains::Within, expect }
    }
}

impl Reverse for ContainsLabel {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}
