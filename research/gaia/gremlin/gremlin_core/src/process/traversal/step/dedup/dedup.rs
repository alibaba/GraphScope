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

use crate::process::traversal::step::dedup::CollectionFactoryGen;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use crate::DynResult;
use pegasus_common::collections::{CollectionFactory, Set};
use std::collections::HashSet;

pub struct HashDedupStep {}

impl Step for HashDedupStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Dedup
    }

    fn add_tag(&mut self, _label: Tag) {
        unimplemented!()
    }

    fn tags(&self) -> &[Tag] {
        unimplemented!()
    }
}

pub struct HashDedupFactory {}

impl CollectionFactory<Traverser> for HashDedupFactory {
    type Target = Box<dyn Set<Traverser>>;
    fn create(&self) -> Self::Target {
        Box::new(HashSet::new()) as Box<dyn Set<Traverser>>
    }
}

impl CollectionFactoryGen for HashDedupStep {
    fn gen(
        &self,
    ) -> DynResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>> {
        Ok(Box::new(HashDedupFactory {}))
    }
}
