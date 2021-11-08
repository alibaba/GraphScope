//
//! Copyright 2021 Alibaba Group Holding Limited.
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

mod graph_partition;
mod graph_query;

use crate::graph::graph::Statement;
use crate::graph::{ID, ID_BITS};
pub use graph_partition::SinglePartition;
pub use graph_query::create_demo_graph;
use ir_common::error::DynResult;
use pegasus::api::function::DynIter;

pub const ID_SHIFT_BITS: usize = ID_BITS >> 1;

/// Given the encoding of an edge, the `ID_MASK` is used to get the lower half part of an edge, which is
/// the src_id. As an edge is indiced by its src_id, one can use edge_id & ID_MASK to route to the
/// machine of the edge.
pub const ID_MASK: ID = ((1 as ID) << (ID_SHIFT_BITS as ID)) - (1 as ID);

pub fn from_fn<I, O, F>(func: F) -> Box<dyn Statement<I, O>>
where
    F: Fn(I) -> DynResult<DynIter<O>> + Send + Sync + 'static,
{
    Box::new(func) as Box<dyn Statement<I, O>>
}

#[macro_export]
macro_rules! limit_n {
    ($iter: expr, $n: expr) => {
        if let Some(limit) = $n {
            let r = $iter.take(limit);
            Box::new(r)
        } else {
            Box::new($iter)
        }
    };
}

#[macro_export]
macro_rules! filter_limit {
    ($iter: expr, $f: expr, $n: expr) => {
        if let Some(ref f) = $f {
            let f = f.clone();
            let r = $iter.filter(move |v| f.eval_bool(Some(v)).unwrap_or(false));
            limit_n!(r, $n)
        } else {
            let r = $iter;
            limit_n!(r, $n)
        }
    };
}
