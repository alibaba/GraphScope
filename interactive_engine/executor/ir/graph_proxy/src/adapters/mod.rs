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

mod csr_store;
mod exp_store;
#[cfg(feature = "with_global_query")]
mod gs_store;
#[cfg(feature = "with_global_query")]
mod vineyard_store;

pub use csr_store::create_csr_store;
pub use exp_store::{create_exp_store, SimplePartition};
#[cfg(feature = "with_global_query")]
pub use gs_store::{
    create_gs_store, GraphScopeStore, GrootClusterInfo, GrootMultiPartition, VineyardClusterInfo,
    VineyardMultiPartition,
};
#[cfg(feature = "with_global_query")]
pub use vineyard_store::VineyardGraphWriter;
