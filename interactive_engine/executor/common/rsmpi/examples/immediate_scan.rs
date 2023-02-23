#![deny(warnings)]
extern crate mpi;

use mpi::collective::SystemOperation;
use mpi::topology::Rank;
use mpi::traits::*;

fn fac(n: Rank) -> Rank {
    (1..=n).product()
}

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();

    let mut x = 0;
    mpi::request::scope(|scope| {
        world
            .immediate_scan_into(scope, &rank, &mut x, SystemOperation::sum())
            .wait();
    });
    assert_eq!(x, (rank * (rank + 1)) / 2);

    let y = rank + 1;
    let mut z = 0;
    mpi::request::scope(|scope| {
        world
            .immediate_exclusive_scan_into(scope, &y, &mut z, SystemOperation::product())
            .wait();
    });
    if rank > 0 {
        assert_eq!(z, fac(y - 1));
    }
}
