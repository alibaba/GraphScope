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

use crate::common::object::Object;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, RemoveTag, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use crate::DynResult;
use bit_set::BitSet;
use pegasus::api::function::*;

pub struct GetPathStep;

impl Step for GetPathStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Path
    }
}

impl MapFuncGen for GetPathStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let func = map!(|item: Traverser| {
            let path = item.take_path();
            Ok(Traverser::Object(Object::UnknownOwned(Box::new(path))))
        });

        Ok(Box::new(func))
    }
}

pub struct PathLocalCount {
    as_tags: Vec<Tag>,
    remove_tags: Vec<Tag>,
}

impl PathLocalCount {
    pub fn empty() -> Self {
        PathLocalCount { as_tags: vec![], remove_tags: vec![] }
    }
}

impl RemoveTag for PathLocalCount {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_tags.push(label);
    }

    fn get_remove_tags_as_slice(&self) -> &[Tag] {
        self.remove_tags.as_slice()
    }
}

struct PathLocalCountFunc {
    tags: BitSet,
    remove_tags: BitSet,
}

impl Step for PathLocalCount {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Count
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_tags.push(label);
    }

    fn tags_as_slice(&self) -> &[Tag] {
        self.as_tags.as_slice()
    }
}

impl MapFunction<Traverser, Traverser> for PathLocalCountFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        let count = input.get_path_len() as i64;
        input.split_with_value(count, &self.tags);
        input.remove_tags(&self.remove_tags);
        Ok(input)
    }
}

impl MapFuncGen for PathLocalCount {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let tags = self.get_tags();
        let remove_tags = self.get_remove_tags();
        Ok(Box::new(PathLocalCountFunc { tags, remove_tags }))
    }
}
