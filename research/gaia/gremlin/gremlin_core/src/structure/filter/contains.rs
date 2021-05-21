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
use std::collections::HashSet;
use std::hash::Hash;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Contains {
    Within,
    Without,
}

impl Reverse for Contains {
    fn reverse(&mut self) {
        match self {
            Contains::Within => *self = Contains::Without,
            Contains::Without => *self = Contains::Within,
        }
    }
}

impl<T: Eq + Hash> BiPredicate<T, HashSet<T>> for Contains {
    fn test(&self, left: &T, right: &HashSet<T>) -> Option<bool> {
        let contains = right.contains(left);
        Some(match self {
            Contains::Within => contains,
            Contains::Without => !contains,
        })
    }
}
