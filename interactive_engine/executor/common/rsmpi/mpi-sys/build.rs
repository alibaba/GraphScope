// Compiles the `rsmpi` C shim library.
extern crate cc;
// Generates the Rust header for the C API.
extern crate bindgen;
// Finds out information about the MPI library
extern crate build_probe_mpi;

use std::env;
use std::path::Path;

fn main() {
    // Try to find an MPI library
    let lib = match build_probe_mpi::probe() {
        Ok(lib) => lib,
        Err(errs) => {
            println!("Could not find MPI library for various reasons:\n");
            for (i, err) in errs.iter().enumerate() {
                println!("Reason #{}:\n{}\n", i, err);
            }
            panic!();
        }
    };

    let mut builder = cc::Build::new();
    builder.file("src/rsmpi.c");

    if cfg!(windows) {
        for inc in &lib.include_paths {
            builder.include(inc);
        }

        // Adds a cfg to identify MS-MPI
        println!("cargo:rustc-cfg=msmpi");
    } else {
        // Use `mpicc` wrapper on Unix rather than the system C compiler.
        builder.compiler("mpicc");
    }

    let compiler = builder.try_get_compiler();

    // Build the `rsmpi` C shim library.
    builder.compile("rsmpi");

    // Let `rustc` know about the library search directories.
    for dir in &lib.lib_paths {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }
    for lib in &lib.libs {
        println!("cargo:rustc-link-lib={}", lib);
    }

    let mut builder = bindgen::builder();
    // Let `bindgen` know about header search directories.
    for dir in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", dir.display()));
    }

    // Get the same system includes as used to build the "rsmpi" lib. This block only really does
    // anything when targeting msvc.
    if let Ok(compiler) = compiler {
        let include_env = compiler.env().iter().find(|(key, _)| key == "INCLUDE");
        if let Some((_, include_paths)) = include_env {
            if let Some(include_paths) = include_paths.to_str() {
                // Add include paths via -I
                builder = builder.clang_args(include_paths.split(';').map(|i| format!("-I{}", i)));
            }
        }
    }

    // Generate Rust bindings for the MPI C API.
    let bindings = builder
        .header("src/rsmpi.h")
        .emit_builtins()
        .generate()
        .unwrap();

    // Write the bindings to disk.
    let out_dir = env::var("OUT_DIR").expect("cargo did not set OUT_DIR");
    let out_file = Path::new(&out_dir).join("functions_and_types.rs");
    bindings.write_to_file(out_file).unwrap();
}
