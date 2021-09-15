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

use crate::structure::filter::element::Reverse;
use crate::structure::filter::BiPredicate;
use std::cmp::Ordering;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum EqCmp {
    Eq,
    NotEq,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum OrdCmp {
    Less,
    LessEq,
    Greater,
    GreaterEq,
}

impl Reverse for EqCmp {
    fn reverse(&mut self) {
        match self {
            EqCmp::Eq => *self = EqCmp::NotEq,
            EqCmp::NotEq => *self = EqCmp::Eq,
        }
    }
}

impl<T: PartialEq> BiPredicate<T, T> for EqCmp {
    fn test(&self, left: &T, right: &T) -> Option<bool> {
        let cmp = left.eq(right);
        Some(match self {
            EqCmp::Eq => cmp,
            EqCmp::NotEq => !cmp,
        })
    }
}

impl<T: PartialOrd> BiPredicate<T, T> for OrdCmp {
    fn test(&self, left: &T, right: &T) -> Option<bool> {
        left.partial_cmp(right).map(|res| match res {
            Ordering::Equal => *self == OrdCmp::LessEq || *self == OrdCmp::GreaterEq,
            Ordering::Greater => *self == OrdCmp::Greater || *self == OrdCmp::GreaterEq,
            Ordering::Less => *self == OrdCmp::Less || *self == OrdCmp::LessEq,
        })
    }
}

impl Reverse for OrdCmp {
    fn reverse(&mut self) {
        match self {
            OrdCmp::Less => *self = OrdCmp::GreaterEq,
            OrdCmp::LessEq => *self = OrdCmp::Greater,
            OrdCmp::Greater => *self = OrdCmp::LessEq,
            OrdCmp::GreaterEq => *self = OrdCmp::Less,
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum Compare {
    Eq(EqCmp),
    Ord(OrdCmp),
}

impl<T: PartialOrd> BiPredicate<T, T> for Compare {
    fn test(&self, left: &T, right: &T) -> Option<bool> {
        match self {
            Compare::Eq(p) => p.test(left, right),
            Compare::Ord(p) => p.test(left, right),
        }
    }
}

impl Reverse for Compare {
    fn reverse(&mut self) {
        match self {
            Compare::Eq(x) => x.reverse(),
            Compare::Ord(x) => x.reverse(),
        }
    }
}
