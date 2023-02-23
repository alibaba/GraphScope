#![deny(warnings)]
extern crate mpi;

use mpi::topology::Rank;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();
    let root_rank = 0;
    let root_process = world.process_at_rank(root_rank);

    let mut x = 0 as Rank;
    if rank == root_rank {
        let v = (0..size).collect::<Vec<_>>();
        root_process.scatter_into_root(&v, &mut x);
    } else {
        root_process.scatter_into(&mut x);
    }
    assert_eq!(x, rank);
}
