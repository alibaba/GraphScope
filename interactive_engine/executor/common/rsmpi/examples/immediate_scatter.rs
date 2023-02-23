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
        mpi::request::scope(|scope| {
            let req = root_process.immediate_scatter_into_root(scope, &v[..], &mut x);
            req.wait();
        });
    } else {
        mpi::request::scope(|scope| {
            let req = root_process.immediate_scatter_into(scope, &mut x);
            req.wait();
        });
    }
    assert_eq!(x, rank);
}
