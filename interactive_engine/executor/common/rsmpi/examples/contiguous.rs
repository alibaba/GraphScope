#![deny(warnings)]
extern crate mpi;

use mpi::datatype::{MutView, UserDatatype, View};
use mpi::point_to_point as p2p;
use mpi::topology::Rank;
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    let next_rank = if rank + 1 < size { rank + 1 } else { 0 };
    let next_process = world.process_at_rank(next_rank);
    let previous_rank = if rank > 0 { rank - 1 } else { size - 1 };
    let previous_process = world.process_at_rank(previous_rank);

    let b1 = (1..).map(|x| rank * x).take(3).collect::<Vec<_>>();
    let mut b2 = std::iter::repeat(-1).take(3).collect::<Vec<_>>();
    println!("Rank {} sending message: {:?}.", rank, b1);
    world.barrier();

    let t = UserDatatype::contiguous(3, &Rank::equivalent_datatype());
    let status;
    {
        let v1 = unsafe { View::with_count_and_datatype(&b1[..], 1, &t) };
        let mut v2 = unsafe { MutView::with_count_and_datatype(&mut b2[..], 1, &t) };
        status = p2p::send_receive_into(&v1, &next_process, &mut v2, &previous_process);
    }

    println!(
        "Rank {} received message: {:?}, status: {:?}.",
        rank, b2, status
    );
    world.barrier();

    let b3 = (1..).map(|x| previous_rank * x).take(3).collect::<Vec<_>>();
    assert_eq!(b3, b2);
}
