#![deny(warnings)]
extern crate mpi;

use mpi::point_to_point as p2p;
use mpi::topology::Rank;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();

    let next_rank = if rank + 1 < size { rank + 1 } else { 0 };
    let next_process = world.process_at_rank(next_rank);
    let previous_rank = if rank > 0 { rank - 1 } else { size - 1 };
    let previous_process = world.process_at_rank(previous_rank);

    let (msg, status): (Rank, _) = p2p::send_receive(&rank, &previous_process, &next_process);
    println!(
        "Process {} got message {}.\nStatus is: {:?}",
        rank, msg, status
    );
    world.barrier();
    assert_eq!(msg, next_rank);

    if rank > 0 {
        let msg: [Rank; 3] = [rank, rank + 1, rank - 1];
        world.process_at_rank(0).send(&msg);
    } else {
        for _ in 1..size {
            let (msg, status) = world.any_process().receive_vec::<Rank>();
            println!(
                "Process {} got long message {:?}.\nStatus is: {:?}",
                rank, msg, status
            );

            let x = status.source_rank();
            let v = vec![x, x + 1, x - 1];
            assert_eq!(v, msg);
        }
    }
    world.barrier();

    let mut x = rank;
    p2p::send_receive_replace_into(&mut x, &next_process, &previous_process);
    assert_eq!(x, previous_rank);
}
