#![deny(warnings)]

extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();

    let comm = universe.world();

    let new_rank = comm.cartesian_map(&[2, comm.size() / 4], &[false, false]);

    println!("{} -> {:?}", comm.rank(), new_rank);
}
