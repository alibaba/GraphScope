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

pub mod common;
pub mod config;
pub mod error;
pub mod graph_db;
pub mod graph_db_impl;
pub mod io;
pub mod ldbc;
pub mod parser;
pub mod prelude;
pub mod schema;
pub mod table;
pub mod utils;

#[macro_use]
extern crate dyn_type;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate abomonation_derive;
extern crate pegasus_common;

#[cfg(feature = "jemalloc")]
extern crate jemallocator;
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
