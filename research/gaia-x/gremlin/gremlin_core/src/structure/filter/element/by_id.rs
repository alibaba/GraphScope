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

use crate::structure::filter::compare::EqCmp;
use crate::structure::filter::contains::Contains;
use crate::structure::filter::element::{ExpectValue, Reverse};
use crate::structure::filter::{BiPredicate, Predicate};
use crate::{Element, ID};
use std::collections::HashSet;

pub struct HasId {
    pub cmp: EqCmp,
    pub expect: ExpectValue<ID>,
}

impl HasId {
    pub fn eq(id: Option<ID>) -> Self {
        if let Some(id) = id {
            HasId { cmp: EqCmp::Eq, expect: ExpectValue::Local(id) }
        } else {
            HasId { cmp: EqCmp::Eq, expect: ExpectValue::TLV }
        }
    }
}

impl Reverse for HasId {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}

impl<E: Element> Predicate<E> for HasId {
    fn test(&self, entry: &E) -> Option<bool> {
        let left = entry.id();
        self.expect.test(&self.cmp, &left)
    }
}

pub struct ContainsId {
    pub cmp: Contains,
    pub expect: HashSet<ID>,
}

impl ContainsId {
    pub fn with_in(expect: HashSet<ID>) -> Self {
        ContainsId { cmp: Contains::Within, expect }
    }

    pub fn with_out(expect: HashSet<ID>) -> Self {
        ContainsId { cmp: Contains::Without, expect }
    }
}

impl<E: Element> Predicate<E> for ContainsId {
    fn test(&self, entry: &E) -> Option<bool> {
        let left = entry.id();
        self.cmp.test(&left, &self.expect)
    }
}

impl Reverse for ContainsId {
    fn reverse(&mut self) {
        self.cmp.reverse()
    }
}
