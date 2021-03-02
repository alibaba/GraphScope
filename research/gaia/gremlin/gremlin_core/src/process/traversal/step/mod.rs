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
use std::collections::HashSet;

#[enum_dispatch]
pub trait Step: 'static {
    fn get_symbol(&self) -> StepSymbol;

    fn add_tag(&mut self, label: Tag);

    fn tags(&self) -> &[Tag];

    fn get_tags(&self) -> HashSet<Tag> {
        let mut labels = HashSet::new();
        for l in self.tags() {
            labels.insert(l.clone());
        }
        labels
    }
}

mod by_key;
mod filter;
mod flat_map;
mod group_by;
mod map;
mod order_by;
mod sink;
mod source;
mod sub_traversal;
mod util;

use crate::structure::Tag;
pub use filter::{FilterFuncGen, FilterStep, HasStep, WherePredicateStep};
pub use flat_map::{EdgeStep, FlatMapGen, FlatMapStep, VertexStep};
pub use group_by::{GroupStep, KeyFunctionGen};
pub use map::ResultProperty;
pub use map::{MapFuncGen, MapStep};
pub use order_by::{CompareFunctionGen, OrderStep};
pub use sink::SinkFuncGen;
pub use source::GraphVertexStep;
pub use sub_traversal::{BySubJoin, HasAnyJoin, JoinFuncGen};

#[enum_dispatch(Step)]
pub enum GremlinStep {
    Map(MapStep),
    FlatMap(FlatMapStep),
    Filter(FilterStep),
}
