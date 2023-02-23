#![deny(missing_docs)]
#![warn(missing_copy_implementations)]
#![warn(trivial_casts)]
#![warn(trivial_numeric_casts)]
#![warn(unused_extern_crates)]
#![warn(unused_import_braces)]
#![warn(unused_qualifications)]

extern crate pkg_config;

use std::{self, env, error::Error, path::PathBuf, process::Command};

use super::super::Library;

use pkg_config::Config;

impl From<pkg_config::Library> for Library {
    fn from(lib: pkg_config::Library) -> Self {
        Library {
            libs: lib.libs,
            lib_paths: lib.link_paths,
            include_paths: lib.include_paths,
            version: lib.version,
            _priv: (),
        }
    }
}

fn probe_via_mpicc(mpicc: &str) -> std::io::Result<Library> {
    // Capture the output of `mpicc -show`. This usually gives the actual compiler command line
    // invoked by the `mpicc` compiler wrapper.
    Command::new(mpicc).arg("-show").output().map(|cmd| {
        let output = String::from_utf8(cmd.stdout).expect("mpicc output is not valid UTF-8");
        // Collect the libraries that an MPI C program should be linked to...
        let libs = collect_args_with_prefix(output.as_ref(), "-l");
        // ... and the library search directories...
        let libdirs = collect_args_with_prefix(output.as_ref(), "-L")
            .into_iter()
            .map(PathBuf::from)
            .collect();
        // ... and the header search directories.
        let headerdirs = collect_args_with_prefix(output.as_ref(), "-I")
            .into_iter()
            .map(PathBuf::from)
            .collect();

        Library {
            libs,
            lib_paths: libdirs,
            include_paths: headerdirs,
            version: String::from("unknown"),
            _priv: (),
        }
    })
}

/// splits a command line by space and collects all arguments that start with `prefix`
fn collect_args_with_prefix(cmd: &str, prefix: &str) -> Vec<String> {
    cmd.split_whitespace()
        .filter_map(|arg| {
            if arg.starts_with(prefix) {
                Some(arg[2..].to_owned())
            } else {
                None
            }
        })
        .collect()
}

/// Probe the environment for an installed MPI library
pub fn probe() -> Result<Library, Vec<Box<dyn Error>>> {
    let mut errs = vec![];

    match probe_via_mpicc(&env::var("MPICC").unwrap_or_else(|_| String::from("mpicc"))) {
        Ok(lib) => return Ok(lib),
        Err(err) => {
            let err: Box<dyn Error> = Box::new(err);
            errs.push(err)
        }
    }

    match Config::new().cargo_metadata(false).probe("mpich") {
        Ok(lib) => return Ok(Library::from(lib)),
        Err(err) => {
            let err: Box<dyn Error> = Box::new(err);
            errs.push(err)
        }
    }

    match Config::new().cargo_metadata(false).probe("openmpi") {
        Ok(lib) => return Ok(Library::from(lib)),
        Err(err) => {
            let err: Box<dyn Error> = Box::new(err);
            errs.push(err)
        }
    }

    Err(errs)
}
