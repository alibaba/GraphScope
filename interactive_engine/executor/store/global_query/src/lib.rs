//
//! Copyright 2022 Alibaba Group Holding Limited.
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

extern crate alloc;
extern crate groot_store;
extern crate itertools;
#[macro_use]
extern crate log;

pub mod apis;
pub mod store_impl;

pub use apis::global_query::{GlobalGraphQuery, PartitionLabeledVertexIds, PartitionVertexIds};
pub use apis::graph_partition::GraphPartitionManager;
pub use apis::graph_schema::Schema;
pub use groot_store::api as store_api;
pub use store_impl::groot::global_graph::GlobalGraph;
pub use store_impl::v6d::read_ffi::FFIGraphStore;
