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
use crate::structure::{Details, Tag};
use crate::{str_to_dyn_error, DynIter, DynResult, Element};
use bit_set::BitSet;
use pegasus::api::function::FlatMapFunction;

pub struct ValuesStep {
    props: Vec<String>,
    as_tags: Vec<Tag>,
}

impl ValuesStep {
    pub fn new(props: Vec<String>) -> Self {
        ValuesStep { props, as_tags: vec![] }
    }
}

struct ValuesFunc {
    props: Vec<String>,
    tags: BitSet,
}

impl Step for ValuesStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Values
    }

    fn add_tag(&mut self, label: Tag) {
        self.as_tags.push(label);
    }

    fn tags_as_slice(&self) -> &[Tag] {
        self.as_tags.as_slice()
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
                    let mut traverser = input.clone();
                    traverser.split_with_value(
                        prop_value
                            .try_to_owned()
                            .ok_or(str_to_dyn_error("Can't get owned property value"))?,
                        &self.tags,
                    );
                    result.push(Ok(traverser));
                }
            }

            Ok(Box::new(result.into_iter()))
        } else {
            Err(str_to_dyn_error("invalid input for values;"))
        }
    }
}

impl FlatMapGen for ValuesStep {
    fn gen(
        &self,
    ) -> DynResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>
    {
        let tags = self.get_tags();
        Ok(Box::new(ValuesFunc { props: self.props.clone(), tags }))
    }
}
