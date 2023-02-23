#![deny(warnings)]
extern crate mpi;

use mpi::datatype::PartitionMut;
use mpi::traits::*;
use mpi::Count;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let rank = world.rank();
    let size = world.size();

    let msg: Vec<_> = (0..rank).collect();

    let counts: Vec<Count> = (0..size).collect();
    let displs: Vec<Count> = counts
        .iter()
        .scan(0, |acc, &x| {
            let tmp = *acc;
            *acc += x;
            Some(tmp)
        })
        .collect();

    let mut buf = vec![0; (size * (size - 1) / 2) as usize];
    {
        let mut partition = PartitionMut::new(&mut buf[..], counts, &displs[..]);
        world.all_gather_varcount_into(&msg[..], &mut partition);
    }

    assert!(buf
        .iter()
        .zip((0..size).flat_map(|r| (0..r)))
        .all(|(&i, j)| i == j));
    println!("Process {} got message {:?}", rank, buf);
}
