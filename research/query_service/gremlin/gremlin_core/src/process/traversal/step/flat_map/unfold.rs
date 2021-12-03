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

use crate::generated::gremlin as pb;
use crate::process::traversal::step::util::result_downcast::try_downcast_list;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, DynResult};
use pegasus::api::function::{DynIter, FlatMapFunction};

/// This unfold step is used in group().by().by(sub_traversal)
/// When we process by(sub_traversal) on the result of group().by(), which is a pair of (traverser, Vec<Traverser>),
/// We need to get_values which is a list, and unfold it first

impl FlatMapFunction<Traverser, Traverser> for pb::UnfoldStep {
    type Target = DynIter<Traverser>;

    fn exec(&self, input: Traverser) -> DynResult<DynIter<Traverser>> {
        let list_traverser = try_downcast_list(
            input
                .get_object()
                .ok_or(str_to_dyn_error("The input traverser for fold should be object"))?,
        )
        .ok_or(str_to_dyn_error("Try downcast object for unfold failed"))?
        .into_iter()
        .map(|trav| trav);
        Ok(Box::new(list_traverser))
    }
}
