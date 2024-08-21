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
pub type InternalId = usize;
pub type LabelId = u8;

pub static INVALID_LABEL_ID: LabelId = 0xff;
pub static VERSION: &str = env!("CARGO_PKG_VERSION");
pub static NAME: &str = env!("CARGO_PKG_NAME");

pub const FILE_SCHEMA: &'static str = "schema.json";
pub const DIR_GRAPH_SCHEMA: &'static str = "graph_schema";

pub const DIR_BINARY_DATA: &'static str = "graph_data_bin";
pub const DIR_SPLIT_RAW_DATA: &'static str = "graph_split_raw";
