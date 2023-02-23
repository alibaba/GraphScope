use mpi::traits::Equivalence;

fn main() {
    // Initialize and then immediately finalize MPI.
    let _ = mpi::initialize().unwrap();

    // Ensures that rsmpi panics if the user attempts to initialize a Datatype after MPI has been
    // finalized.
    assert!(std::panic::catch_unwind(|| {
        #[derive(Equivalence)]
        struct CheckPostFinalizePanic;
        let _ = CheckPostFinalizePanic::equivalent_datatype();
    })
    .is_err());
}
