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

use std::io;

pub use crate::plan::ffi::*;

pub mod error;
pub mod glogue;
pub mod plan;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

pub trait JsonIO {
    /// Write the logical plan to a json via the given `writer`.
    fn into_json<W: io::Write>(self, writer: W) -> io::Result<()>;

    /// Read the logical plan from a json via the given `reader`
    fn from_json<R: io::Read>(reader: R) -> io::Result<Self>
    where
        Self: Sized;
}
