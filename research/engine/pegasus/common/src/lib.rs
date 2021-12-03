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
extern crate log;

pub mod buffer;
pub mod bytes;
pub mod channel;
pub mod codec;
pub mod collections;
pub mod downcast;
pub mod io;
pub mod logs;
pub mod queue;
pub mod rc;
pub mod utils;

#[macro_export]
macro_rules! inspect {
    ($($arg:tt)+) => (
         if log_enabled!(log::Level::Info) {
            log!(log::Level::Info, $($arg)+);
         } else {
            println!($($arg)+)
         }
    )
}

#[macro_export]
macro_rules! inspect_err {
    ($($arg:tt)+) => (
         if log_enabled!(log::Level::Error) {
            log!(log::Level::Error, $($arg)+);
         } else {
            eprintln!($($arg)+)
         }
    )
}
