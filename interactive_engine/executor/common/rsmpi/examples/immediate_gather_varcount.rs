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

    let root_rank = 0;
    let root_process = world.process_at_rank(root_rank);

    let msg: Vec<_> = (0..rank).collect();

    if rank == root_rank {
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
            mpi::request::scope(|scope| {
                root_process
                    .immediate_gather_varcount_into_root(scope, &msg[..], &mut partition)
                    .wait();
            })
        }

        assert!(buf
            .iter()
            .zip((0..size).flat_map(|r| (0..r)))
            .all(|(&i, j)| i == j));
        println!("{:?}", buf);
    } else {
        mpi::request::scope(|scope| {
            root_process
                .immediate_gather_varcount_into(scope, &msg[..])
                .wait();
        });
    }
}
