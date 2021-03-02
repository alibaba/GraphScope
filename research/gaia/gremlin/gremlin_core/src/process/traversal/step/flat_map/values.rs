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

use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{FlatMapGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::Details;
use crate::{DynIter, DynResult, Element};
use pegasus::api::function::FlatMapFunction;
use std::collections::HashSet;

pub struct ValuesStep {
    props: Vec<String>,
    as_labels: Vec<String>,
}

impl ValuesStep {
    pub fn new(props: Vec<String>) -> Self {
        ValuesStep { props, as_labels: vec![] }
    }
}

struct ValuesFunc {
    props: Vec<String>,
    labels: HashSet<String>,
}

impl Step for ValuesStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Values
    }

    fn add_tag(&mut self, label: String) {
        self.as_labels.push(label);
    }

    fn tags(&self) -> &[String] {
        self.as_labels.as_slice()
    }
}

impl FlatMapFunction<Traverser, Traverser> for ValuesFunc {
    type Target = DynIter<Traverser>;

    fn exec(&self, input: Traverser) -> DynResult<DynIter<Traverser>> {
        if let Some(elem) = input.get_element() {
            let mut result = vec![];
            for prop_name in self.props.iter() {
                let prop_value = elem.details().get_property(prop_name);
                if let Some(prop_value) = prop_value {
                    let traverser = input.split_with_value(
                        prop_value.try_to_owned().expect("Can't get owned property value"),
                        &self.labels,
                    );
                    result.push(Ok(traverser));
                }
            }
            Ok(Box::new(result.into_iter()))
        } else {
            panic!("invalid input for values;")
        }
    }
}

impl FlatMapGen for ValuesStep {
    fn gen(&self) -> Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>> {
        let labels = self.get_tags();
        Box::new(ValuesFunc { props: self.props.clone(), labels })
    }
}
