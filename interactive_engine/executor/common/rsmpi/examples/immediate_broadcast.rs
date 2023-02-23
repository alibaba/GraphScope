#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let root_rank = 0;
    let root_process = world.process_at_rank(root_rank);

    let mut x;
    if world.rank() == root_rank {
        x = 2_u64.pow(10);
        println!("Root broadcasting value: {}.", x);
    } else {
        x = 0_u64;
    }
    mpi::request::scope(|scope| {
        root_process.immediate_broadcast_into(scope, &mut x).wait();
    });
    println!("Rank {} received value: {}.", world.rank(), x);
    assert_eq!(x, 1024);
    println!();

    let mut a;
    let n = 4;
    if world.rank() == root_rank {
        a = (1..).map(|i| 2_u64.pow(i)).take(n).collect::<Vec<_>>();
        println!("Root broadcasting value: {:?}.", &a[..]);
    } else {
        a = std::iter::repeat(0_u64).take(n).collect::<Vec<_>>();
    }
    mpi::request::scope(|scope| {
        root_process
            .immediate_broadcast_into(scope, &mut a[..])
            .wait();
    });
    println!("Rank {} received value: {:?}.", world.rank(), &a[..]);
    assert_eq!(&a[..], &[2, 4, 8, 16]);
}
