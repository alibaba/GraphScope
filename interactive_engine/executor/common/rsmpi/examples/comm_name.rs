#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

const CNAME: &str = "__rsmpi__test";

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    assert_eq!("MPI_COMM_WORLD", world.get_name());
    world.set_name(CNAME);
    assert_eq!(CNAME, world.get_name());
}
