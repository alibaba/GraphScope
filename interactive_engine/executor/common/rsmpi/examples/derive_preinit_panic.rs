use mpi::traits::Equivalence;

fn main() {
    // Ensures that rsmpi panics if the user attempts to initialize the datatype before
    // initializing MPI.
    assert!(std::panic::catch_unwind(|| {
        #[derive(Equivalence)]
        struct CheckPreInitPanic;
        let _ = CheckPreInitPanic::equivalent_datatype();
    })
    .is_err());
}
