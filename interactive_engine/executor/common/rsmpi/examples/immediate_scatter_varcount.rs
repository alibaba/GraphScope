#![deny(warnings)]
extern crate mpi;

use mpi::datatype::Partition;
use mpi::traits::*;
use mpi::Count;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let rank = world.rank();
    let size = world.size();

    let root_rank = 0;
    let root_process = world.process_at_rank(root_rank);

    let mut buf = vec![0; rank as usize];

    if rank == root_rank {
        let msg: Vec<_> = (0..size).flat_map(|i| (0..i)).collect();
        let counts: Vec<Count> = (0..size).collect();
        let displs: Vec<Count> = counts
            .iter()
            .scan(0, |acc, &x| {
                let tmp = *acc;
                *acc += x;
                Some(tmp)
            })
            .collect();
        let partition = Partition::new(&msg[..], counts, &displs[..]);
        mpi::request::scope(|scope| {
            root_process
                .immediate_scatter_varcount_into_root(scope, &partition, &mut buf[..])
                .wait();
        });
    } else {
        mpi::request::scope(|scope| {
            root_process
                .immediate_scatter_varcount_into(scope, &mut buf[..])
                .wait();
        });
    }

    assert!(buf.iter().zip(0..rank).all(|(&i, j)| i == j));
    println!("Process {} got message: {:?}", rank, buf);
}
