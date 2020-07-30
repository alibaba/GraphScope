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

/// Print build information to stdout.
///
/// Information items are get from compiled-time environment variables set in `build.rs` .
pub fn get_build_info() {
    println!("Build Info:");
    println!("\tGit Commit : {}", env!("BUILD_GIT_HASH"));
    println!("\tHost       : {}", env!("BUILD_HOST"));
    println!("\tUser       : {}", env!("BUILD_USER"));
    println!("\tDir        : {}", env!("BUILD_DIR"));
    println!("\tDate       : {}", env!("BUILD_DATE"));
    println!("\tProfile    : {}", env!("BUILD_PROFILE"));
    println!("\tOpt Level  : {}", env!("BUILD_OPT_LEVEL"));
    println!("\tRustc      : {}", env!("BUILD_RUSTC"));
}
