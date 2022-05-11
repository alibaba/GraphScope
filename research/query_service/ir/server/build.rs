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
    println!("cargo:rerun-if-changed=proto/job_service.proto");
    codegen_inplace()
}

#[cfg(feature = "gcip")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    let dir = "src/generated";
    if std::path::Path::new(&dir).exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    std::fs::create_dir(&dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir("src/generated")
        .compile(&["proto/job_service.proto"], &["proto"])?;
    Ok(())
}

#[cfg(not(feature = "gcip"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/job_service.proto"], &["proto"])?;
    Ok(())
}
