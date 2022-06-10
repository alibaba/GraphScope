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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    codegen_inplace()
}

#[cfg(feature = "proto_inplace")]
use std::path::PathBuf;
#[cfg(feature = "proto_inplace")]
const GEN_DIR: &'static str = "src/generated";

#[cfg(feature = "proto_inplace")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/common.proto");
    println!("cargo:rerun-if-changed=../proto/expr.proto");
    println!("cargo:rerun-if-changed=../proto/algebra.proto");
    println!("cargo:rerun-if-changed=../proto/schema.proto");
    println!("cargo:rerun-if-changed=../proto/results.proto");
    let out_dir = PathBuf::from(GEN_DIR);
    if out_dir.exists() {
        let _ = std::fs::remove_dir_all(GEN_DIR);
    }
    let _ = std::fs::create_dir(GEN_DIR);
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Serialize,Deserialize)]")
        .out_dir(&out_dir)
        .compile_protos(
            &[
                "../proto/common.proto",
                "../proto/expr.proto",
                "../proto/algebra.proto",
                "../proto/schema.proto",
                "../proto/results.proto",
            ],
            &["../proto"],
        )?;

    Ok(())
}

#[cfg(not(feature = "proto_inplace"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/common.proto");
    println!("cargo:rerun-if-changed=../proto/expr.proto");
    println!("cargo:rerun-if-changed=../proto/algebra.proto");
    println!("cargo:rerun-if-changed=../proto/schema.proto");
    println!("cargo:rerun-if-changed=../proto/results.proto");
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Serialize,Deserialize)]")
        .compile_protos(
            &[
                "../proto/common.proto",
                "../proto/expr.proto",
                "../proto/algebra.proto",
                "../proto/schema.proto",
                "../proto/results.proto",
            ],
            &["../proto"],
        )?;

    Ok(())
}
