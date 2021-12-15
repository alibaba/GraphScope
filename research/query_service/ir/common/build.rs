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
use prost_wkt_build::*;
#[cfg(feature = "proto_inplace")]
use std::path::PathBuf;
#[cfg(feature = "proto_inplace")]
const GEN_DIR: &'static str = "src/generated";

#[cfg(feature = "proto_inplace")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/common.proto");
    println!("cargo:rerun-if-changed=../proto/expr.proto");
    println!("cargo:rerun-if-changed=../proto/algebra.proto");
    println!("cargo:rerun-if-changed=../proto/results.proto");
    let out_dir = PathBuf::from(GEN_DIR);
    if out_dir.exists() {
        std::fs::remove_dir_all(GEN_DIR).unwrap();
    }
    std::fs::create_dir(GEN_DIR).unwrap();
    let descriptor_file = out_dir.join("descriptors.bin");
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Serialize,Deserialize)]")
        .out_dir(&out_dir)
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .file_descriptor_set_path(&descriptor_file)
        .compile_protos(
            &[
                "../proto/common.proto",
                "../proto/expr.proto",
                "../proto/algebra.proto",
                "../proto/results.proto",
            ],
            &["../proto"],
        )?;
    let descriptor_bytes = std::fs::read(descriptor_file).unwrap();
    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();
    prost_wkt_build::add_serde(out_dir, descriptor);

    Ok(())
}

#[cfg(not(feature = "proto_inplace"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .type_attribute(".", "#[derive(Serialize,Deserialize)]")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .compile_protos(
            &[
                "../proto/common.proto",
                "../proto/expr.proto",
                "../proto/algebra.proto",
                "../proto/results.proto",
            ],
            &["../proto"],
        )?;
    Ok(())
}
