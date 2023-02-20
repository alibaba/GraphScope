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

mod expand_intersect;
mod get_v;
mod path_end;
mod path_start;
mod project;

pub use expand_intersect::IntersectionEntry;
use pegasus::api::function::{FilterMapFunction, MapFunction};

use crate::error::FnGenResult;
use crate::process::record::Record;

pub trait MapFuncGen {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>>;
}

pub trait FilterMapFuncGen {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>>;
}
