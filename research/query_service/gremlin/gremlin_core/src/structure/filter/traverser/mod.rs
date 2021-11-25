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

use crate::process::traversal::traverser::Traverser;
use crate::structure::filter::Predicate;

mod by_path;
mod by_tag;
mod by_value;

use crate::structure::filter::element::ElementFilter;
pub use by_path::*;
pub use by_tag::*;
pub use by_value::*;

pub struct HasHead {
    filter: ElementFilter,
}

impl HasHead {
    pub fn new(filter: ElementFilter) -> Self {
        HasHead { filter }
    }
}

impl From<HasHead> for TraverserFilter {
    fn from(raw: HasHead) -> Self {
        TraverserFilter::HasHead(raw)
    }
}

impl Predicate<Traverser> for HasHead {
    fn test(&self, entry: &Traverser) -> Option<bool> {
        if let Some(head) = entry.get_element() {
            self.filter.test(head)
        } else {
            Some(false)
        }
    }
}

pub enum TraverserFilter {
    HasHead(HasHead),
    HasTag(HasTag),
    HasCycle(IsSimple),
    IsValue(ValueFilter),
}

impl Predicate<Traverser> for TraverserFilter {
    fn test(&self, entry: &Traverser) -> Option<bool> {
        match self {
            TraverserFilter::HasHead(f) => f.test(entry),
            TraverserFilter::HasTag(f) => f.test(entry),
            TraverserFilter::HasCycle(f) => f.test(entry),
            TraverserFilter::IsValue(f) => f.test(entry),
        }
    }
}

impl From<HasTag> for TraverserFilter {
    fn from(raw: HasTag) -> Self {
        TraverserFilter::HasTag(raw)
    }
}

impl From<ValueFilter> for TraverserFilter {
    fn from(raw: ValueFilter) -> Self {
        TraverserFilter::IsValue(raw)
    }
}
