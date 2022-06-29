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

extern crate cmake;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    codegen_inplace()
}

#[cfg(feature = "with_native")]
const NATIVE_DIR: &'static str = "native";

#[cfg(feature = "with_native")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    let dst = cmake::Config::new(NATIVE_DIR)
        .build_target("native_store")
        .build();

    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-search=/usr/local/lib64");
    println!("cargo:rustc-link-search=/opt/homebrew/lib");
    match std::env::var("VINEYARD_ROOT_DIR") {
        Ok(val) => {
            println!("cargo:rustc-link-search={}/lib", val);
            println!("cargo:rustc-link-search={}/lib64", val);
        }
        Err(_) => (),
    }
    println!("cargo:rustc-link-search={}/build", dst.display());
    println!("cargo:rustc-link-lib=native_store");
    println!("cargo:rustc-link-lib=vineyard_graph");
    println!("cargo:rustc-link-lib=vineyard_io");
    println!("cargo:rustc-link-lib=vineyard_client");
    println!("cargo:rustc-link-lib=vineyard_basic");
    println!("cargo:rustc-link-lib=mpi");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=arrow");
    println!("cargo:rustc-link-lib=glog");

    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=mpi_cxx");
        if std::path::Path::new("/usr/ali/alicpp/built/gcc-4.9.2").exists() {
            println!("cargo:cfg=tunnel");
        }
    } else if cfg!(target_os = "macos") {
    } else {
        unimplemented!()
    }

    Ok(())
}

#[cfg(not(feature = "with_native"))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
