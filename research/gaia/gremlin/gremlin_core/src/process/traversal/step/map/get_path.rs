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

use crate::object::Object;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{MapFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use pegasus::api::function::*;
use std::collections::HashSet;

pub struct GetPathStep;

impl Step for GetPathStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Path
    }

    fn add_tag(&mut self, _: String) {
        unimplemented!();
    }

    fn tags(&self) -> &[String] {
        &[]
    }
}

impl MapFuncGen for GetPathStep {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let func = map!(|item: Traverser| {
            let path = item.take_path();
            Ok(Traverser::Unknown(Object::UnknownOwned(Box::new(path))))
        });
        Box::new(func)
    }
}

pub struct PathLocalCount {
    as_labels: Vec<String>,
}

impl PathLocalCount {
    pub fn empty() -> Self {
        PathLocalCount { as_labels: vec![] }
    }
}

struct PathLocalCountFunc {
    labels: HashSet<String>,
}

impl Step for PathLocalCount {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Count
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        self.as_labels.as_slice()
    }
}

impl MapFunction<Traverser, Traverser> for PathLocalCountFunc {
    fn exec(&self, input: Traverser) -> FnResult<Traverser> {
        let count = input.get_path_len() as i64;
        Ok(input.split_with_value(count, &self.labels))
    }
}

impl MapFuncGen for PathLocalCount {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>> {
        let labels = self.get_tags();
        Box::new(PathLocalCountFunc { labels })
    }
}
