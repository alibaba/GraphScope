#![deny(warnings)]
extern crate mpi;

use mpi::Threading;

fn main() {
    let (_universe, threading) = mpi::initialize_with_threading(Threading::Multiple).unwrap();
    assert_eq!(threading, mpi::environment::threading_support());
    println!("Supported level of threading: {:?}", threading);
}
