#![deny(warnings)]
extern crate mpi;

use mpi::topology::CommunicatorRelation;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let moon = world.duplicate();

    world.barrier();
    moon.barrier();

    assert_eq!(CommunicatorRelation::Congruent, world.compare(&moon));
}
