#![deny(warnings)]
extern crate mpi;

fn main() {
    let (version, subversion) = mpi::environment::version();
    println!("This is MPI-{}.{}.", version, subversion);
    println!("{}", mpi::environment::library_version().unwrap());
    let _universe = mpi::initialize().unwrap();
    println!("{}", mpi::environment::processor_name().unwrap());

    #[cfg(not(msmpi))]
    assert!(
        version >= 3,
        "Rust MPI bindings require MPI standard 3.0 and up."
    );
}
