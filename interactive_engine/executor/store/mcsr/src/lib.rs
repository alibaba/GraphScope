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

#[macro_use]
extern crate abomonation_derive;
#[macro_use]
extern crate dyn_type;
#[macro_use]
extern crate log;
extern crate core;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

pub mod col_table;
pub mod columns;
pub mod date;
pub mod date_time;
mod edge_trim;
mod error;
pub mod graph;
pub mod graph_db;
pub mod graph_db_impl;
pub mod graph_loader;
pub mod graph_partitioner;
pub mod io;
pub mod ldbc_parser;
pub mod mcsr;
pub mod schema;
pub mod scsr;
pub mod types;
mod utils;
pub mod vertex_map;
