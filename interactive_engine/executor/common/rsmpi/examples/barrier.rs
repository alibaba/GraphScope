#![deny(warnings)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let receiver_rank = 0;

    if world.rank() == receiver_rank {
        let n = (world.size() - 1) as usize;
        let mut buf = vec![0u64; 2 * n];
        for x in buf[0..n].iter_mut() {
            world.any_process().receive_into(x);
        }
        world.barrier();
        for x in buf[n..2 * n].iter_mut() {
            world.any_process().receive_into(x);
        }
        println!("{:?}", buf);
        assert!(buf[0..n].iter().all(|&x| x == 1));
        assert!(buf[n..2 * n].iter().all(|&x| x == 2));
    } else {
        world.process_at_rank(0).send(&1u64);
        world.barrier();
        world.process_at_rank(0).send(&2u64);
    }
}
