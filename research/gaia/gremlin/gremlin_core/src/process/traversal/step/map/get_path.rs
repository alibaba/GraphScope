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
use crate::process::traversal::step::{MapFuncGen, RemoveLabel, Step};
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

    fn add_tag(&mut self, _: Tag) {
        unimplemented!();
    }

    fn tags(&self) -> &[Tag] {
        &[]
    }
}

impl MapFuncGen for GetPathStep {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let func = map!(|item: Traverser| {
            let path = item.take_path();
            Ok(Traverser::Unknown(Object::UnknownOwned(Box::new(path))))
        });

        Ok(Box::new(func))
    }
}

pub struct PathLocalCount {
    as_labels: Vec<Tag>,
    remove_labels: Vec<Tag>,
}

impl PathLocalCount {
    pub fn empty() -> Self {
        PathLocalCount { as_labels: vec![], remove_labels: vec![] }
    }
}

impl RemoveLabel for PathLocalCount {
    fn remove_tag(&mut self, label: Tag) {
        self.remove_labels.push(label);
    }

    fn remove_tags(&self) -> &[Tag] {
        self.remove_labels.as_slice()
    }
}

struct PathLocalCountFunc {
    labels: BitSet,
    remove_labels: BitSet,
}

impl Step for PathLocalCount {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Count
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[Tag] {
        self.as_labels.as_slice()
    }
}

impl MapFunction<Traverser, Traverser> for PathLocalCountFunc {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        let count = input.get_path_len() as i64;
        input.split_with_value(count, &self.labels);
        input.remove_labels(&self.remove_labels);
        Ok(input)
    }
}

impl MapFuncGen for PathLocalCount {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let labels = self.get_tags();
        let remove_labels = self.get_remove_tags();
        Ok(Box::new(PathLocalCountFunc { labels, remove_labels }))
    }
}
