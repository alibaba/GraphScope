#![deny(warnings)]
extern crate mpi;

use mpi::datatype::DynBufferMut;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let root_process = world.process_at_rank(0);

    let int_type = i32::equivalent_datatype().dup();

    let mut ints = if world.rank() == 0 {
        [1i32, 2, 3, 4]
    } else {
        [0, 0, 0, 0]
    };

    let mut buffer =
        unsafe { DynBufferMut::from_raw(ints.as_mut_ptr(), ints.count(), int_type.as_ref()) };

    root_process.broadcast_into(&mut buffer);

    assert_eq!([1, 2, 3, 4], ints);
}
