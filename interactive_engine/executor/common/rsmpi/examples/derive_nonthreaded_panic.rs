use mpi::traits::Equivalence;

fn main() {
    // Request highest threading safety short of Multiple
    let (_universe, threading) =
        mpi::initialize_with_threading(mpi::Threading::Serialized).unwrap();

    // Sanity check
    assert_ne!(threading, mpi::Threading::Multiple);

    // This checks that rsmpi panics when attempting to lazily create the `DatatypeRef` in
    // `#[derive(Equivalence)]` from a thread other than the main thread when
    // `mpi::Threading::Multiple` isn't available.
    assert!(std::thread::spawn(move || {
        #[derive(Equivalence)]
        struct EnsurePanicEquivalenceInitialization(i32);
        EnsurePanicEquivalenceInitialization::equivalent_datatype();
    })
    .join()
    .is_err());

    // This checks that getting access to the lazily created `DatatypeRef` from another thread is
    // fine, as long as the `DatatypeRef` was previously initialized on the main thread.
    #[derive(Equivalence)]
    struct EnsureNoPanicIfInitializedOnMainThread(i32);
    EnsureNoPanicIfInitializedOnMainThread::equivalent_datatype();

    assert!(std::thread::spawn(move || {
        EnsureNoPanicIfInitializedOnMainThread::equivalent_datatype();
    })
    .join()
    .is_ok());
}
