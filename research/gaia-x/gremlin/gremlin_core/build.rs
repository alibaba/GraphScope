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

extern crate tonic_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    codegen_inplace()
}

#[allow(dead_code)]
const GEN_DIR: &'static str = "src/generated";

#[cfg(feature = "proto_inplace")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/common.proto");
    println!("cargo:rerun-if-changed=proto/gremlin.proto");
    println!("cargo:rerun-if-changed=proto/gremlin_result.proto");
    if std::path::Path::new(GEN_DIR).exists() {
        std::fs::remove_dir_all(GEN_DIR).unwrap();
    }
    std::fs::create_dir(GEN_DIR).unwrap();
    tonic_build::configure().build_server(false).out_dir(GEN_DIR).compile(
        &["../proto/common.proto", "../proto/gremlin.proto", "../proto/gremlin_result.proto"],
        &["../proto"],
    )?;
    Ok(())
}

#[cfg(not(feature = "proto_inplace"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(false).compile(
        &["../proto/common.proto", "../proto/gremlin.proto", "../proto/gremlin_result.proto"],
        &["../proto"],
    )?;
    Ok(())
}
