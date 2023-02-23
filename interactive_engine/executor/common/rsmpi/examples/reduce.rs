#![deny(warnings)]
#![allow(clippy::many_single_char_names)]
#![allow(clippy::needless_pass_by_value)]
extern crate mpi;

use std::os::raw::{c_int, c_void};

#[cfg(feature = "user-operations")]
use mpi::collective::UserOperation;
use mpi::collective::{self, SystemOperation, UnsafeUserOperation};
use mpi::ffi::MPI_Datatype;
use mpi::topology::Rank;
use mpi::traits::*;

#[cfg(feature = "user-operations")]
fn test_user_operations<C: Communicator>(comm: C) {
    let rank = comm.rank();
    let size = comm.size();
    let mut h = 0;
    comm.all_reduce_into(
        &(rank + 1),
        &mut h,
        &UserOperation::commutative(|x, y| {
            let x: &[Rank] = x.downcast().unwrap();
            let y: &mut [Rank] = y.downcast().unwrap();
            for (&x_i, y_i) in x.iter().zip(y) {
                *y_i += x_i;
            }
        }),
    );
    assert_eq!(h, size * (size + 1) / 2);
}

#[cfg(not(feature = "user-operations"))]
fn test_user_operations<C: Communicator>(_: C) {}

#[cfg(not(all(msmpi, target_arch = "x86")))]
unsafe extern "C" fn unsafe_add(
    invec: *mut c_void,
    inoutvec: *mut c_void,
    len: *mut c_int,
    _datatype: *mut MPI_Datatype,
) {
    use std::slice;

    let x: &[Rank] = slice::from_raw_parts(invec as *const Rank, *len as usize);
    let y: &mut [Rank] = slice::from_raw_parts_mut(inoutvec as *mut Rank, *len as usize);
    for (&x_i, y_i) in x.iter().zip(y) {
        *y_i += x_i;
    }
}

#[cfg(all(msmpi, target_arch = "x86"))]
unsafe extern "stdcall" fn unsafe_add(
    invec: *mut c_void,
    inoutvec: *mut c_void,
    len: *mut c_int,
    _datatype: *mut MPI_Datatype,
) {
    use std::slice;

    let x: &[Rank] = slice::from_raw_parts(invec as *const Rank, *len as usize);
    let y: &mut [Rank] = slice::from_raw_parts_mut(inoutvec as *mut Rank, *len as usize);
    for (&x_i, y_i) in x.iter().zip(y) {
        *y_i += x_i;
    }
}

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();
    let root_rank = 0;

    if rank == root_rank {
        let mut sum: Rank = 0;
        world
            .process_at_rank(root_rank)
            .reduce_into_root(&rank, &mut sum, SystemOperation::sum());
        assert_eq!(sum, size * (size - 1) / 2);
    } else {
        world
            .process_at_rank(root_rank)
            .reduce_into(&rank, SystemOperation::sum());
    }

    let mut max: Rank = -1;

    world.all_reduce_into(&rank, &mut max, SystemOperation::max());
    assert_eq!(max, size - 1);

    let a: u16 = 0b0000_1111_1111_0000;
    let b: u16 = 0b0011_1100_0011_1100;

    let mut c = b;
    collective::reduce_local_into(&a, &mut c, SystemOperation::bitwise_and());
    assert_eq!(c, 0b0000_1100_0011_0000);

    let mut d = b;
    collective::reduce_local_into(&a, &mut d, SystemOperation::bitwise_or());
    assert_eq!(d, 0b0011_1111_1111_1100);

    let mut e = b;
    collective::reduce_local_into(&a, &mut e, SystemOperation::bitwise_xor());
    assert_eq!(e, 0b0011_0011_1100_1100);

    let f = (0..size).collect::<Vec<_>>();
    let mut g: Rank = 0;

    world.reduce_scatter_block_into(&f[..], &mut g, SystemOperation::product());
    assert_eq!(g, rank.wrapping_pow(size as u32));

    test_user_operations(universe.world());

    let mut i = 0;
    let op = unsafe { UnsafeUserOperation::commutative(unsafe_add) };
    world.all_reduce_into(&(rank + 1), &mut i, &op);
    assert_eq!(i, size * (size + 1) / 2);
}
