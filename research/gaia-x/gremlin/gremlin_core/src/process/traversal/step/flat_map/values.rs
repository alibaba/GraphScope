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
use crate::structure::{Details, PropKey};
use crate::{str_to_dyn_error, DynIter, DynResult, Element};
use bit_set::BitSet;
use pegasus::api::function::FlatMapFunction;

pub struct PropertiesStep {
    pub prop_keys: Vec<PropKey>,
    pub tags: BitSet,
}

impl FlatMapFunction<Traverser, Traverser> for PropertiesStep {
    type Target = DynIter<Traverser>;

    fn exec(&self, input: Traverser) -> DynResult<DynIter<Traverser>> {
        if let Some(elem) = input.get_element() {
            let mut result = vec![];
            for prop_name in self.prop_keys.iter() {
                let prop_value = elem.details().get_property(prop_name);
                if let Some(prop_value) = prop_value {
                    let mut traverser = input.clone();
                    traverser.split_with_value(
                        prop_value
                            .try_to_owned()
                            .ok_or(str_to_dyn_error("Can't get owned property value"))?,
                        &self.tags,
                    );
                    result.push(traverser);
                }
            }

            Ok(Box::new(result.into_iter()))
        } else {
            Err(str_to_dyn_error("invalid input for values;"))
        }
    }
}
