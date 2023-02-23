#![deny(missing_docs)]
#![warn(missing_copy_implementations)]
#![warn(trivial_casts)]
#![warn(trivial_numeric_casts)]
#![warn(unused_extern_crates)]
#![warn(unused_import_braces)]
#![warn(unused_qualifications)]
#![allow(clippy::unknown_clippy_lints)]

//! Probe an environment for an installed MPI library
//!
//! Probing is done in several steps on Unix:
//!
//! 1. Try to find an MPI compiler wrapper either from the environment variable `MPICC` or under
//!    the name `mpicc` then run the compiler wrapper with the command line argument `-show` and
//!    interpret the resulting output as `gcc` compatible command line arguments.
//! 2. Query the `pkg-config` database for an installation of `mpich`.
//! 3. Query the `pkg-config` database for an installation of `openmpi`.
//!
//! On Windows, only MS-MPI is looked for. The MSMPI_INC and MSMPI_LIB32/64 environment variables
//! are expected.
//!
//! The result of the first successful step is returned. If no step is successful, a list of errors
//! encountered while executing the steps is returned.

#[cfg(unix)]
extern crate pkg_config;

mod os;

pub use os::probe;

use std::path::PathBuf;

/// Result of a successfull probe
#[allow(clippy::manual_non_exhaustive)]
#[derive(Clone, Debug)]
pub struct Library {
    /// Names of the native MPI libraries that need to be linked
    pub libs: Vec<String>,
    /// Search path for native MPI libraries
    pub lib_paths: Vec<PathBuf>,
    /// Search path for C header files
    pub include_paths: Vec<PathBuf>,
    /// The version of the MPI library
    pub version: String,
    _priv: (),
}
