#![deny(warnings)]
extern crate mpi;

use mpi::topology::Rank;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();

    if rank > 0 {
        let msg = rank as u8;
        world.barrier();
        world.process_at_rank(0).ready_send(&msg);
    } else {
        let mut v = vec![0u8; (size - 1) as usize];
        mpi::request::scope(|scope| {
            let reqs = v
                .iter_mut()
                .zip(1..)
                .map(|(x, i)| {
                    world
                        .process_at_rank(i as Rank)
                        .immediate_receive_into(scope, x)
                })
                .collect::<Vec<_>>();
            world.barrier();
            for req in reqs {
                req.wait();
            }
        });
        println!("Got message: {:?}", v);
        assert!(v.iter().zip(1..).all(|(x, i)| i == *x as usize));
    }
}
