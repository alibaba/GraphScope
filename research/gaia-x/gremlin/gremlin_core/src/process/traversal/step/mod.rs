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
use crate::process::traversal::step::util::StepSymbol;

#[enum_dispatch]
pub trait Step: 'static {
    fn get_symbol(&self) -> StepSymbol;

    fn get_tags(&self) -> BitSet {
        unreachable!()
    }

    fn get_remove_tags(&self) -> BitSet {
        unreachable!()
    }
}

impl Step for pb::GremlinStep {
    fn get_symbol(&self) -> StepSymbol {
        // TODO: return StepSymbol according to different gremlin step
        unimplemented!()
    }

    fn get_tags(&self) -> BitSet {
        let mut tags = BitSet::with_capacity(INIT_TAG_NUM);
        for step_tag in &self.tags {
            let tag = Tag::from_pb(step_tag.clone()).unwrap();
            tags.insert(tag as usize);
        }
        tags
    }

    fn get_remove_tags(&self) -> BitSet {
        let mut tags = BitSet::with_capacity(INIT_TAG_NUM);
        for step_tag in &self.remove_tags {
            let tag = Tag::from_pb(step_tag.clone()).unwrap();
            tags.insert(tag as usize);
        }
        tags
    }
}

mod by_key;
mod filter;
mod flat_map;
mod fold;
mod group_by;
mod map;
mod order_by;
mod sink;
mod source;
mod sub_traversal;
mod traverser_router;
mod util;

use crate::structure::{Tag, INIT_TAG_NUM};
use crate::FromPb;
use bit_set::BitSet;
pub use filter::FilterFuncGen;
pub use flat_map::FlatMapFuncGen;
pub use fold::{AccumFactoryGen, TraverserAccumulator};
pub use group_by::KeyFunctionGen;
pub use map::MapFuncGen;
pub use map::ResultProperty;
pub use order_by::CompareFunctionGen;
pub use sink::TraverserSinkEncoder;
pub use source::graph_step_from;
pub use source::GraphVertexStep;
pub use sub_traversal::TraverserLeftJoinGen;
pub use traverser_router::Router;
pub use util::*;
