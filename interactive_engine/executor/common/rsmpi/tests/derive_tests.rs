#![cfg(feature = "derive")]

use mpi::traits::Equivalence;

/// We test that #[derive(Equivalence)] correctly casts CONSTANT to a i32 for the
/// C interop. For defining a rust array, CONSTANT must be usize.
#[test]
fn derive_equivalence() {
    const CONSTANT: usize = 7;
    #[derive(Equivalence)]
    struct ArrayWrapper {
        // Size is a variable
        field: [f32; CONSTANT],
    }
    #[derive(Equivalence)]
    struct ArrayWrapper2 {
        // Size is a {number}
        field: [usize; 7],
    }
}
