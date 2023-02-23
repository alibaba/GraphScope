#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let t_start = mpi::time();
    world.barrier();
    let t_end = mpi::time();

    println!("barrier took: {} s", t_end - t_start);
    println!(
        "the clock has a resoltion of {} seconds",
        mpi::time_resolution()
    );
}
