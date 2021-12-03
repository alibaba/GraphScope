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

extern crate chrono;
extern crate protoc_grpcio;

use std::process::Command;
use std::env;
use std::fs::{self, DirEntry};
use std::path::Path;
use chrono::prelude::*;

/// Visit proto directory recursively. The callback function is invoked if a file is encountered,
/// otherwise add the sub-directory to watching list to trigger regeneration of .rs proto files (by
/// print the cargo directive).
fn visit_dirs(dir: &Path, cb: &mut dyn FnMut(&DirEntry)) {
    if dir.is_dir() {
        println!("cargo:rerun-if-changed={}", dir.to_str().unwrap());
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb);
            } else {
                cb(&entry);
            }
        }
    }
}

fn generate_rust_protos() {
    let proto_root = "../proto";
    let mut files = vec![];
    visit_dirs(Path::new(proto_root), &mut |e| {
        let p = e.path().to_str().unwrap().to_owned();
        if p.ends_with(".proto") && !p.contains("biz_cognitive_graph") {
            files.push(p);
        }
    });
    protoc_grpcio::compile_grpc_protos(files.as_slice(), &[proto_root], "./src/proto")
        .expect("protoc/grpc compile failed");
}

fn generate_build_info_env() {
    let output = Command::new("git").args(&["rev-parse", "HEAD"]).output().unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=BUILD_GIT_HASH={}", git_hash);

    let user = env::var("USER").unwrap_or("UNKNOWN".to_string());
    println!("cargo:rustc-env=BUILD_USER={}", user);

    let output = Command::new("hostname").output().unwrap();
    let host = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=BUILD_HOST={}", host);

    let dir = env::var("PWD").unwrap_or("UNKNOWN".to_string());
    println!("cargo:rustc-env=BUILD_DIR={}", dir);

    let profile = env::var("PROFILE").unwrap_or("UNKNOWN".to_string());
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);

    let opt_level = env::var("OPT_LEVEL").unwrap_or("UNKNOWN".to_string());
    println!("cargo:rustc-env=BUILD_OPT_LEVEL={}", opt_level);

    let rustc = env::var("RUSTC").unwrap_or("UNKNOWN".to_string());
    let output = Command::new(rustc).args(&["--version"]).output().unwrap();
    let rustc_version = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=BUILD_RUSTC={}", rustc_version);

    let datetime: DateTime<Local> = Local::now();
    println!("cargo:rustc-env=BUILD_DATE={:?}", datetime.to_string());
}

#[allow(unused_variables)]
fn main() {
    generate_build_info_env();
    generate_rust_protos();
}
