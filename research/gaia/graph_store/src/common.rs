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

pub type DefaultId = usize;
pub type LabelId = u8;
/// A vertex's label can be hierarchical, which so far can have at most two layers.
pub type Label = [LabelId; 2];
pub type PartitionId = u16;
pub type InternalId = u32;

pub static VERSION: &str = env!("CARGO_PKG_VERSION");
pub static NAME: &str = env!("CARGO_PKG_NAME");
pub static INVALID_LABEL_ID: LabelId = 0xff;
