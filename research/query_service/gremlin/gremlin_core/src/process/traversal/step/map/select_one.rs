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

use crate::process::traversal::path::PathItem;
use crate::process::traversal::traverser::Traverser;
use crate::str_to_dyn_error;
use crate::structure::Tag;
use bit_set::BitSet;
use pegasus::api::function::{FnResult, MapFunction};

pub struct SelectOneStep {
    pub select_tag: Tag,
    pub tags: BitSet,
    pub remove_tags: BitSet,
}

impl MapFunction<Traverser, Traverser> for SelectOneStep {
    fn exec(&self, mut input: Traverser) -> FnResult<Traverser> {
        if let Some(path_item) = input.select(&self.select_tag) {
            match path_item {
                PathItem::OnGraph(graph_element) => {
                    let graph_element = graph_element.clone();
                    input.split(graph_element, &self.tags);
                    input.remove_tags(&self.remove_tags);
                    Ok(input)
                }
                PathItem::Detached(obj) => {
                    let obj = obj.clone();
                    input.split_with_value(obj, &self.tags);
                    input.remove_tags(&self.remove_tags);
                    Ok(input)
                }
                PathItem::Empty => {
                    Err(str_to_dyn_error("Cannot get tag since the item is already deleted"))
                }
            }
        } else {
            Err(str_to_dyn_error("Cannot get tag"))
        }
    }
}
