#![deny(warnings)]
#![allow(clippy::needless_pass_by_value)]
extern crate mpi;

use std::os::raw::{c_int, c_void};

#[cfg(feature = "user-operations")]
use mpi::collective::UserOperation;
use mpi::collective::{SystemOperation, UnsafeUserOperation};
use mpi::ffi::MPI_Datatype;
use mpi::topology::Rank;
use mpi::traits::*;

#[cfg(feature = "user-operations")]
fn test_user_operations<C: Communicator>(comm: C) {
    let op = UserOperation::commutative(|x, y| {
        let x: &[Rank] = x.downcast().unwrap();
        let y: &mut [Rank] = y.downcast().unwrap();
        for (&x_i, y_i) in x.iter().zip(y) {
            *y_i += x_i;
        }
    });
    let rank = comm.rank();
    let size = comm.size();
    let mut c = 0;
    mpi::request::scope(|scope| {
        comm.immediate_all_reduce_into(scope, &rank, &mut c, &op)
            .wait();
    });
    assert_eq!(c, size * (size - 1) / 2);
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
        mpi::request::scope(|scope| {
            world
                .process_at_rank(root_rank)
                .immediate_reduce_into_root(scope, &rank, &mut sum, SystemOperation::sum())
                .wait();
        });
        assert_eq!(sum, size * (size - 1) / 2);
    } else {
        mpi::request::scope(|scope| {
            world
                .process_at_rank(root_rank)
                .immediate_reduce_into(scope, &rank, SystemOperation::sum())
                .wait();
        });
    }

    let mut max: Rank = -1;

    mpi::request::scope(|scope| {
        world
            .immediate_all_reduce_into(scope, &rank, &mut max, SystemOperation::max())
            .wait();
    });
    assert_eq!(max, size - 1);

    let a = (0..size).collect::<Vec<_>>();
    let mut b: Rank = 0;

    mpi::request::scope(|scope| {
        world
            .immediate_reduce_scatter_block_into(scope, &a[..], &mut b, SystemOperation::product())
            .wait();
    });
    assert_eq!(b, rank.wrapping_pow(size as u32));

    test_user_operations(universe.world());

    let mut d = 0;
    let op = unsafe { UnsafeUserOperation::commutative(unsafe_add) };
    mpi::request::scope(|scope| {
        world
            .immediate_all_reduce_into(scope, &rank, &mut d, &op)
            .wait();
    });
    assert_eq!(d, size * (size - 1) / 2);
}
