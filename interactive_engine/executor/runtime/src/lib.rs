//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#![allow(bare_trait_objects)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(dead_code)]
extern crate bincode;
extern crate byteorder;
extern crate itertools;
#[allow(unused_imports)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate log4rs;
#[macro_use]
extern crate maxgraph_common;
extern crate maxgraph_store;
extern crate protobuf;
extern crate regex;
extern crate structopt;
extern crate lru_time_cache;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rand;
extern crate serde_json;
extern crate core;
extern crate abomonation_derive;
extern crate abomonation;
extern crate alloc;
extern crate libc;

use maxgraph_common::proto::query_flow::QueryInput;

#[allow(bare_trait_objects)]
pub mod dataflow;
pub mod store;
