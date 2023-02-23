#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let ints = [3i32, 2, 1];
    let packed = world.pack(&ints[..]);

    let mut new_ints = [0, 0, 0];
    unsafe {
        world.unpack_into(&packed, &mut new_ints[..], 0);
    }

    assert_eq!([3, 2, 1], new_ints);
}
