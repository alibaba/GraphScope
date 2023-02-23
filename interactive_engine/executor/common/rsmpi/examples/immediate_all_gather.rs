#![deny(warnings)]
extern crate mpi;

use mpi::datatype::{MutView, UserDatatype, View};
use mpi::traits::*;
use mpi::Count;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let root_rank = 0;

    let count = world.size() as usize;
    let i = 2_u64.pow(world.rank() as u32 + 1);
    let mut a = vec![0u64; count];

    mpi::request::scope(|scope| {
        world
            .immediate_all_gather_into(scope, &i, &mut a[..])
            .wait();
    });

    if world.rank() == root_rank {
        println!("Root gathered sequence: {:?}.", a);
    }
    assert!(a
        .iter()
        .enumerate()
        .all(|(a, &b)| b == 2u64.pow(a as u32 + 1)));

    let factor = world.rank() as u64 + 1;
    let a = (1_u64..)
        .take(count)
        .map(|x| x * factor)
        .collect::<Vec<_>>();
    let mut t = vec![0u64; count * count];

    mpi::request::scope(|scope| {
        world
            .immediate_all_gather_into(scope, &a[..], &mut t[..])
            .wait();
    });

    if world.rank() == root_rank {
        println!("Root gathered table:");
        for r in t.chunks(count) {
            println!("{:?}", r);
        }
    }
    assert!((0_u64..)
        .zip(t.iter())
        .all(|(a, &b)| b == (a / count as u64 + 1) * (a % count as u64 + 1)));

    let d = UserDatatype::contiguous(count as Count, &u64::equivalent_datatype());
    t = vec![0u64; count * count];

    {
        let sv = unsafe { View::with_count_and_datatype(&a[..], 1, &d) };
        let mut rv = unsafe { MutView::with_count_and_datatype(&mut t[..], count as Count, &d) };
        mpi::request::scope(|scope| {
            world.immediate_all_gather_into(scope, &sv, &mut rv).wait();
        });
    }

    if world.rank() == root_rank {
        println!("Root gathered table:");
        for r in t.chunks(count) {
            println!("{:?}", r);
        }
    }
    assert!((0_u64..)
        .zip(t.iter())
        .all(|(a, &b)| b == (a / count as u64 + 1) * (a % count as u64 + 1)));
}
