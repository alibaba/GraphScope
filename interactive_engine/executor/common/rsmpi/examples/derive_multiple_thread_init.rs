use mpi::traits::Equivalence;

fn main() {
    let (_universe, threading) = mpi::initialize_with_threading(mpi::Threading::Multiple).unwrap();

    if threading != mpi::Threading::Multiple {
        // Silently return - MPI implementation may not support `threading::Multiple`
        return;
    }

    // This checks that rsmpi does not panic when attempting to lazily create the `DatatypeRef` on
    // another thread.
    assert!(std::thread::spawn(move || {
        #[derive(Equivalence)]
        struct EnsureNoPanicEquivalenceInitialization(i32);
        EnsureNoPanicEquivalenceInitialization::equivalent_datatype();
    })
    .join()
    .is_ok());
}
