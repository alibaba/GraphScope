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

#[enum_dispatch]
pub trait Step: 'static {
    fn get_symbol(&self) -> StepSymbol;

    fn add_tag(&mut self, _label: Tag) {
        unreachable!()
    }

    fn tags_as_slice(&self) -> &[Tag] {
        &[]
    }

    fn get_tags(&self) -> BitSet {
        let mut labels = BitSet::with_capacity(INIT_TAG_NUM);
        for l in self.tags_as_slice() {
            labels.insert(l.clone() as usize);
        }
        labels
    }
}

pub trait RemoveTag: Step {
    /// To remove a given tag
    fn remove_tag(&mut self, label: Tag);

    /// Get the slice of tags to be removed
    fn get_remove_tags_as_slice(&self) -> &[Tag];

    /// Turn the tags to remove into a `BitSet`
    fn get_remove_tags(&self) -> BitSet {
        let mut tags = BitSet::with_capacity(INIT_TAG_NUM);
        for &tag in self.get_remove_tags_as_slice() {
            tags.insert(tag as usize);
        }
        tags
    }
}

mod by_key;
mod dedup;
mod filter;
mod flat_map;
mod fold;
mod group_by;
mod map;
mod order_by;
mod sink;
mod source;
mod sub_traversal;
mod util;

use crate::structure::{Tag, INIT_TAG_NUM};
use bit_set::BitSet;
pub use dedup::{CollectionFactoryGen, DedupStep};
pub use filter::{FilterFuncGen, FilterStep, HasStep, WherePredicateStep};
pub use flat_map::{EdgeStep, FlatMapGen, FlatMapStep, VertexStep};
pub use fold::{FoldFunctionGen, FoldStep};
pub use group_by::{GroupFunctionGen, GroupStep};
pub use map::ResultProperty;
pub use map::{MapFuncGen, MapStep};
pub use order_by::{CompareFunctionGen, OrderStep};
pub use sink::SinkFuncGen;
pub use source::graph_step_from;
pub use source::GraphVertexStep;
pub use sub_traversal::{BySubJoin, GroupBySubJoin, HasAnyJoin, JoinFuncGen};
pub use util::result_downcast;

#[enum_dispatch(Step)]
pub enum GremlinStep {
    Map(MapStep),
    FlatMap(FlatMapStep),
    Filter(FilterStep),
}
