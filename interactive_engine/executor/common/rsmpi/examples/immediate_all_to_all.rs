#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    let u = vec![rank; size as usize];
    let mut v = vec![0; size as usize];

    mpi::request::scope(|scope| {
        world
            .immediate_all_to_all_into(scope, &u[..], &mut v[..])
            .wait();
    });

    println!("u: {:?}", u);
    println!("v: {:?}", v);

    assert!(v.into_iter().zip(0..size).all(|(i, j)| i == j));
}
